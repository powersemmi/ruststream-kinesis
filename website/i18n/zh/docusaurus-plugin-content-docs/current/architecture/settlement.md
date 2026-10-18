---
title: 结算
sidebar_position: 6
description: "ack、drop、retry 和 retry_after 在 Kinesis 上变成什么；Kinesis 为每个分片保存一个检查点，而不是逐条确认记录。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const watermark = {
  "title": "一个分片上的水位线：检查点停在第一个尚未结算的投递之前",
  "text": "以文字阅读本图",
  "shard": "一个分片，记录按到达顺序排列",
  "settled": "已结算",
  "unsettled": "运行中",
  "checkpoint": ["检查点：记录 2", "第一个运行中的记录", "之前的最后一条"],
  "store": ["LeaseStore", "::checkpoint", "持有租约时"],
  "next": ["3 结算后，", "检查点移到 5：", "6 仍在", "运行"],
  "at": "指向",
  "writes": "写入"
};

export const retries = {
  "title": "Kinesis 上的一次重试：尝试上限决定把副本发到流、发到死信流，还是让检查点越过该记录",
  "text": "以文字阅读本图",
  "record": ["一条记录", "x-ruststream-retry-count 说明这是第几次尝试"],
  "outcome": ["处理器返回", "retry_after(delay)，或在有声明时返回 retry()"],
  "cap": ["max_attempts：这次尝试是否低于上限？"],
  "copy": ["低于上限：", "原记录", "结算，副本", "发往流"],
  "deadLetter": ["到达上限：", "记录发往", "死信流"],
  "past": ["到达上限，", "没有死信流：", "检查点", "越过该记录"],
  "tip": ["副本是位于最新处的新记录，", "落在其键选定的分片上"],
  "crash": ["延迟的副本保存在进程中：", "计时器触发前崩溃就会丢失它"],
  "handled": "已处理",
  "retries": "重试",
  "below": "低于上限",
  "atCap": "到达上限"
};

# 结算

`ack`、`drop`、`retry` 和 `retry_after` 在 Kinesis 上变成什么。Kinesis 为每个分片保存一个检查点，而不是逐条确认记录。

<Diagram name="watermark" words={watermark} />

检查点指定分片中的一条记录，表示它之前的每条记录都已处理。因此本 crate 按到达顺序跟踪每个分片的投递。结算把它的投递标为已处理，检查点移到从最旧的投递开始、连续已处理的那一段投递中的最后一条记录。并行工作分区发来的 ack 可以按任意顺序到达，检查点绝不会越过仍在运行的记录。结算只在推进检查点时才写入租约存储，并且只有本实例持有该分片时，存储才接受这次写入。

| 结果 | 对分片的影响 |
|---|---|
| `ack` | 已处理：没有更早的记录仍在运行时，检查点前移 |
| `drop` | 已处理，与 `ack` 相同：检查点越过有毒的记录 |
| `retry()` | 被拒绝（`AckError::Unsupported`）并记录日志：记录保持未处理并挡住检查点，下次获取该分片的租约时，从检查点重新读取该分片 |
| 在 `max_attempts` 或死信流下的 `retry()` | 一个计数的副本发往流，原记录算作已处理 |
| `retry_after(delay)` | 延迟结束后，一个计数的副本发往流，原记录算作已处理 |
| 上限用尽 | 记录发往死信流；没有死信流时，记录算作已处理，检查点越过它 |
| 关闭前没有结算 | 检查点保持原位，分片的下一个所有者会再次读取该记录 |

分片按顺序读取，Kinesis 无法交回单条记录，因此重新入队被拒绝。读取者继续读取，该记录会在重启或租约移交之后，连同它之后的每条记录一起回来。投递是至少一次的。

[seek](kinesis/reading.md) 让它所移动的分片的水位线重新开始。对 seek 之前读到的投递进行结算不会改变任何东西，因此迟到的 ack 绝不会把检查点移回 seek 之前的位置。已关闭的分片只在它的每次投递都结算之后才标为 `SHARD_END`，它的子分片在那之后开始读取。

## 重试 {/* #retries */}

<Diagram name="retries" words={retries} />

Kinesis 既不扣留记录，也不统计投递次数，所以这两件事都由运行时来做。`retry_after` 结果，以及声明了 [`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) 或 [`dead_letter`](api:ruststream::runtime::Mounting::dead_letter) 死信流的注册上的 `retry()`，都走这条路径。尝试次数随副本的 `x-ruststream-retry-count` 消息头传递，第一次投递算作一次。

副本发往订阅读取的流，也就是 [`KinesisStream`](api:ruststream_kinesis::KinesisStream) 描述符报告的地址，因此注册无需指定目的地。副本落在最新处，位于其分区键选定的分片上，排在此后写入的记录之后。因此重试的记录会失去它与相邻记录之间的顺序。在这一顺序重要的地方，不要声明上限，让 `retry()` 挡住分片。流的每个消费者都会收到副本，而不只是发起重试的那个。

用尽尝试次数的记录连同它的载荷和消息头发往死信流。设置了上限却没有死信流时，记录按 `drop` 的方式了结。
