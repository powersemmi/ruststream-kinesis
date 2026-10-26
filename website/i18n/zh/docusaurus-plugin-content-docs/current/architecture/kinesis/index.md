---
title: 流
sidebar_position: 0
description: "Kinesis 流是一组分片，每个分片都是一个有序的记录日志，每个消费者都各自读取每个分片，直到保留期结束。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "生产者把记录写入一个流的三个分片，两个消费者各自读取每个分片",
  "text": "以文字阅读本图",
  "producers": ["生产者", "分区键选定分片"],
  "stream": "流 orders",
  "s0": "分片 0",
  "s1": "分片 1",
  "s2": "分片 2",
  "first": ["消费者 billing", "每个分片一个迭代器，", "每个分片一个检查点"],
  "second": ["消费者 audit", "每个分片一个迭代器，", "每个分片一个检查点"],
  "retention": ["记录为每个读取者保留，直到保留期结束：", "默认 24 小时，最长 365 天"],
  "puts": "PutRecord",
  "reads": "读取每个分片"
};

# 流

Kinesis 流是一组分片，每个分片都是一个有序的记录日志，每个消费者都各自读取每个分片，直到保留期结束。

<Diagram name="stream" words={words} />

生产者用 `PutRecord` 写入一条记录：流的名称、一个分区键和一个数据块。Kinesis 用 MD5 把分区键哈希成一个 128 位的数字。每个分片拥有这些数字中的一段范围，因此分区键选定分片，同一个键的记录保持顺序。顺序只在分片内部成立，在其他任何地方都不成立。

一条记录只包含一个数据块和一个分区键，此外什么也没有：记录没有消息头。分区键长 1 到 256 个 Unicode 字符，数据块连同分区键最大 10 MiB。流的名称由 1 到 128 个字符组成，可用字母、数字、`_`、`.` 和 `-`。本 crate 用名称指代流。

记录到达时，Kinesis 为它分配一个序列号。在一个分片内部，序列号随时间增长。本 crate 记录分片投递记录的顺序，从不比较序列号本身。

记录一直留在流中，直到保留期结束，不管有没有人读过：默认 24 小时，最长 365 天。每个消费者用自己的迭代器读取每个分片，因此两个消费者都会看到每条记录。消费者读到了哪里，就是每个分片一个位置，即它的检查点。Kinesis 既不保存检查点，也不保存确认：它们由消费者保存在[租约表](leases.md)中。

记录里没有存放用户消息头的地方，所以本 crate 把消息头写进载荷前面的一个小信封，并且只在有消息头时才这样做。不带消息头发布的记录就是原样的载荷，任何消费者都能读取。唯一的例外是本身以信封开头的载荷：它会得到一个不含消息头的信封，因此读回时完整无缺。

这一模型就是 Amazon Kinesis Data Streams 自己的模型（[关键概念](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html)、[`PutRecord`](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html)、[保留期](https://docs.aws.amazon.com/streams/latest/dev/kinesis-extended-retention.html)）。分片如何拆分与合并，见[分片与重新分片](shards.md)；消费者如何读取分片，见[读取](reading.md)。
