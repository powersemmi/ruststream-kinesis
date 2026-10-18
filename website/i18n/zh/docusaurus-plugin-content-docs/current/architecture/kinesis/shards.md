---
title: 分片与重新分片
sidebar_position: 1
description: "一个分片承担流吞吐量中固定的一份；拆分或合并会关闭分片并打开子分片，子分片在其父分片之后读取。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "拆分把分片 0 变为分片 1 和 2，合并把分片 1 和 2 变为分片 3，每个子分片都在其父分片之后读取",
  "text": "以文字阅读本图",
  "s0": ["分片 0，已关闭", "有结束序列号"],
  "s1": ["分片 1，已关闭", "0 的下半段"],
  "s2": ["分片 2，已关闭", "0 的上半段"],
  "s3": ["分片 3，打开", "父分片 1 和 2"],
  "gate": ["每个父分片都读到末尾之后，才读取子分片，", "因此同一个键的记录在变更前后", "保持顺序"],
  "split": "拆分",
  "merge": "合并"
};

# 分片与重新分片

一个分片承担流吞吐量中固定的一份；拆分或合并会关闭分片并打开子分片，子分片在其父分片之后读取。

<Diagram name="resharding" words={words} />

每个分片每秒最多接受 1 MB 或 1,000 条记录的写入。它每秒最多提供 2 MB 的读取，并且每秒最多接受五次 `GetRecords` 调用。读取该分片的所有消费者共用这五次调用，因此这一限制与计费无关：它决定分片究竟能被读取多频繁。预置模式的流拥有为它分配的分片，按需模式的流按流量需要获得分片。

拆分把一个分片的哈希键范围分给两个子分片。合并把两个相邻的分片合为一个。无论哪种方式，父分片都会关闭：`ListShards` 为它们报告一个结束序列号，它们不再接受新记录，已有的记录保留到保留期结束。读到已关闭分片末尾的读取者拿不到下一个迭代器：这就是 `SHARD_END`。

同一个键的记录可能先位于父分片上，之后位于子分片上。为了保持它们的顺序，消费者先把每个父分片读到末尾，再读取子分片。本 crate 在获取每个子分片的租约之前检查这一点：每个父分片都必须在租约存储中标为 `SHARD_END`，或者已因保留期裁剪而从列表中消失。订阅从最新处开始时，没有检查点的已关闭父分片算作已读完，因为它的旧记录反正会被跳过。读取者只在其分片的每次投递都结算之后，才写入 `SHARD_END`。

订阅每 10 秒重新列出一次分片，因此父分片读完后，子分片会在这段时间内打开。子分片的读取者从子分片的检查点继续读取，[读取](reading.md)一页用图说明了这一点。没有检查点的子分片从它最旧的记录打开，因此在读取者启动之前写入子分片的记录也会被读到。`create_if_missing(shards)` 以预置模式创建缺失的流，分片数为指定的数量，并最多等待一分钟，直到流可用。

这一模型就是 Amazon Kinesis Data Streams 自己的模型（[配额和限制](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html)、[容量模式](https://docs.aws.amazon.com/streams/latest/dev/how-do-i-size-a-stream.html)、[重新分片](https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-resharding.html)、[重新分片之后的读取](https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-after-resharding.html)）。
