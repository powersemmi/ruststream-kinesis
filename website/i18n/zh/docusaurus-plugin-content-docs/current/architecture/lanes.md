---
title: 工作分区
sidebar_position: 5
description: "一个订阅为每个分片使用一个读取者，通过一个通道读取它的分片；workers(n, by_key) 把记录分到按分区键划分的工作分区上。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一个订阅的分发：三个分片，每个分片一个读取者，一个通道，以及按分区键划分的两个工作分区",
  "text": "以文字阅读本图",
  "shard0": "分片 0",
  "shard1": "分片 1",
  "shard2": "分片 2",
  "reader0": ["读取者", "持有租约"],
  "reader1": ["读取者", "持有租约"],
  "reader2": ["读取者", "持有租约"],
  "channel": ["一个容量为 64 次投递的通道", "通道满时每个读取者都会停下"],
  "keyed": ["workers(2, by_key)", "工作分区 = 分区键的哈希"],
  "lane0": ["工作分区 1", "键 alice 和 carol，", "各自有序"],
  "lane1": ["工作分区 2", "键 bob，有序"],
  "single": ["不用 workers 时，一个循环按到达顺序", "逐条为记录运行处理器"],
  "instances": ["共用一张租约表的实例共享分片：", "每个分片同一时间只由一个实例读取"],
  "oneAtATime": "一次一条"
};

# 工作分区

一个订阅为每个分片使用一个读取者，通过一个通道读取它的分片；`workers(n, by_key)` 把记录分到按分区键划分的工作分区上。

<Diagram name="lanes" words={words} />

订阅的流一次产出一条记录，不管它来自哪个分片。不用 `workers` 时，一个循环依次为每条记录运行处理器，因此每个分片都保持顺序，也没有任何东西并行运行。`workers(n)` 同时运行最多 `n` 个处理器，不保证顺序。

`workers(n, by_key)` 运行 `n` 个工作分区，记录进入其键的哈希所对应的工作分区。键是记录的 [`PARTITION_KEY_HEADER`](api:ruststream_kinesis::PARTITION_KEY_HEADER)，每次投递都用记录自己的分区键填写它。因此同一个键的记录绝不会分散到两个工作分区上，Kinesis 在分片上为一个键保持的顺序在服务中得以保持。工作分区少于键时，每个工作分区服务多个键。

订阅的所有读取者都送往同一个容量为 64 次投递的通道。处理器跟不上时，通道被填满，每个读取者都等待，任何分片都不会读到服务处理不过来的地方。并行工作分区发来的 ack 乱序到达，检查点仍然绝不会越过尚未结算的记录，[结算](settlement.md)一页用图说明了这一点。

共用一张[租约表](kinesis/leases.md)的服务实例共享分片：每个分片同一时间只由一个实例读取，新实例在租约空出时获取分片。使用默认的 [`MemoryLeaseStore`](api:ruststream_kinesis::MemoryLeaseStore) 时，每个实例保存自己的租约，因此每个实例都读取每个分片。

[批处理器](path.md)最多接收挂载处指定数量的记录，或者接收在批的第一条记录之后 50 毫秒内到达的记录，因此一个批往往小于这个大小。
