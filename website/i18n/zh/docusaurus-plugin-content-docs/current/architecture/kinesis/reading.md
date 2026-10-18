---
title: 读取
sidebar_position: 2
description: "消费者用迭代器读取分片：GetShardIterator 把迭代器放到分片的日志上，每次 GetRecords 返回记录和下一个迭代器。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "分片日志上的五种迭代器类型，以及从那里开始读取的 GetRecords 调用循环",
  "text": "以文字阅读本图",
  "shard": "一个分片，记录按到达顺序排列",
  "next": "下一条",
  "horizon": ["TRIM_", "HORIZON"],
  "timestamp": ["AT_", "TIMESTAMP"],
  "atSequence": ["AT_", "SEQUENCE_", "NUMBER"],
  "afterSequence": ["AFTER_", "SEQUENCE_", "NUMBER"],
  "latest": ["LATEST"],
  "getIterator": ["GetShardIterator", "分片、迭代器类型、位置"],
  "getRecords": ["GetRecords", "迭代器，上限 1000 或批大小"],
  "answer": ["记录和 NextShardIterator：", "下一次读取的迭代器；", "已关闭分片的末尾没有它"],
  "expiry": ["迭代器在签发 5 分钟后过期。", "空读取等待 poll_interval，默认 1 秒"],
  "type": "迭代器类型",
  "iterator": "迭代器"
};

# 读取

消费者用迭代器读取分片：`GetShardIterator` 把迭代器放到分片的日志上，每次 `GetRecords` 返回记录和下一个迭代器。

<Diagram name="iterator" words={words} />

迭代器类型决定第一次读取从哪里开始。`TRIM_HORIZON` 从分片仍保存的最旧记录开始。`LATEST` 从最新的记录之后开始，因此读取者只看到此后到达的记录。`AT_TIMESTAMP` 按到达时间戳，从某个时间点或其后到达的第一条记录开始。`AT_SEQUENCE_NUMBER` 从某一条记录开始，`AFTER_SEQUENCE_NUMBER` 从紧随其后的记录开始。

每个 [`KinesisPosition`](api:ruststream_kinesis::KinesisPosition) 都是其中一种类型：`horizon()`、`latest()`、`timestamp(ms)` 和 `sequence(shard, seq)`，最后一个指定某个分片中的某一条记录。已存储的检查点让分片从它所指记录之后打开。没有检查点的分片从 `LATEST` 打开，除非 `start_at(..)` 指定了另一个位置。`start_at(..)` 在启动时生效，作用于订阅开始时打开的分片，并优先于它们的检查点。之后启动的读取者，无论是为子分片还是为从另一个实例接管的分片启动，都从检查点继续读取；没有检查点的子分片从 `TRIM_HORIZON` 打开。

一次 `GetRecords` 调用最多返回 10,000 条记录或 10 MB。本 crate 请求 1,000 条，批订阅则请求它的批大小。每个应答都带有下一个迭代器，读取者用它进行下一次调用。应答中没有记录时，读取者先等待描述符的 [`poll_interval`](api:ruststream_kinesis::KinesisStream::poll_interval)（默认一秒），再读取下一次。这样读取者就不会超出其分片允许的每秒五次读取。

迭代器在签发五分钟后过期。迭代器过期的读取者从检查点获取新的迭代器；尚无任何结算时，则从它读到的第一条记录获取，绝不从 `LATEST` 获取，因此不会跳过任何记录。被限流的读取等待一个轮询间隔，再用同一个迭代器重试。其他任何失败的读取都在订阅的流上报告，读取者等待一秒后用同一个迭代器重试，因为它仍然有效。连续失败十一次后，读取者停止并释放它的租约；订阅在下一次列出分片时重新获取该分片，新的读取者从旧读取者开始的地方读起。

本 crate 使用流的共享吞吐量读取，即一个分片的所有消费者共用的每秒五次读取。增强型扇出是另一种读取模型：它通过 `SubscribeToShard` 为每个消费者提供专用的每秒 2 MB。生产者用 Kinesis Producer Library 聚合的记录报告为 [`KinesisError::AggregatedRecord`](api:ruststream_kinesis::KinesisError::AggregatedRecord)，不会交给处理器。

这一模型就是 Amazon Kinesis Data Streams 自己的模型（[`GetShardIterator`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html)、[`GetRecords`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html)、[增强型扇出](https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html)、[聚合](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md)）。
