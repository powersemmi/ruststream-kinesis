---
title: 读取流
sidebar_position: 2
description: "逐条或按批读取 Kinesis 流，用 poll_interval 和批大小分配分片的读取额度，并了解一次投递带有什么。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 读取流

本指南逐条和按批读取一个流，分配每个分片的读取额度，并展示一次投递带有什么。

示例是一个分析服务。它在付款到达时逐笔记账，并按批统计页面浏览。

## 流是如何读取的 {/* #how-a-stream-is-read */}

订阅为它持有的每个分片运行一个读取者。读取者用分片的迭代器调用 `GetRecords`，按顺序交出记录，然后立即再次请求。如果一次读取什么也没返回，读取者先暂停 [`poll_interval`](../components/subscriptions/poll-interval.md)（默认一秒），再发起请求。读取者和处理器之间最多有 64 次投递在等待；读取者发现队列已满时就等待，所以慢处理器会让读取变慢。

Kinesis 允许每个分片[每秒五次 `GetRecords` 调用和每秒 2 MB](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html)。流的所有消费者共用这份额度。读取过于频繁的服务会被限流：Kinesis 返回 `ProvisionedThroughputExceededException`。AWS SDK 先自行重试被限流的调用。重试用完后，读取者等待一个 `poll_interval`，再用同一个迭代器请求。处理器看到的只是一次停顿，既没有错误，也没有重新投递。

## 逐条读取 {/* #one-record-at-a-time */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#payments"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#payments"
```

</TabItem>
</Tabs>

`book` 读取 `payments` 流，逐笔结算每次付款。逐条读取的订阅每次读取最多请求 1000 条记录。`poll_interval(Duration::from_millis(250))` 缩短空读取之后的暂停，所以新付款最多等待四分之一秒。每秒四次空读取，给其他消费者留下分片额度中每秒一次读取。这一设置也是一个[挂载点步骤](../components/subscriptions/subscriber-ext.md)：在 `b.include(..)` 之后写 `.poll_interval(..)`。

[`Ctx<Position>`](../components/context/index.md) 读取记录在流中的位置：它的分片和序列号，形式为 [`KinesisPosition`](../components/positions/index.md)。seek 到这个位置，会从这条记录开始重新读取该分片（[重放与 seek](replay.md)）。每次投递还带有三个[消息头](../components/context/headers.md)：

| 消息头 | 值 |
|---|---|
| `kinesis-shard-id`（[`SHARD_HEADER`](api:ruststream_kinesis::SHARD_HEADER)） | 读出记录的分片 |
| `kinesis-sequence-number`（[`SEQUENCE_HEADER`](api:ruststream_kinesis::SEQUENCE_HEADER)） | 记录在该分片中的序列号 |
| `partition-key`（[`PARTITION_KEY_HEADER`](api:ruststream_kinesis::PARTITION_KEY_HEADER)） | 发布记录时使用的分区键 |

处理器用 `ctx.headers()` 读取它们。记录的其他消息头由它的生产者写入。

## 批 {/* #batches */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#views"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#views"
```

</TabItem>
</Tabs>

接收切片的处理器是[批处理器](../components/subscriptions/batches.md)。应用用 `.batch(nonzero!(500))` 挂载 `count`，500 就成为它的读取者每次 `GetRecords` 调用的上限。因此一次读取取回的记录绝不会超过一批，而 Kinesis 把一次读取限制在 10 000 条记录以内。未满的批在它的第一条记录到达 50 毫秒后发出，所以流量很小的流也能很快投递它的记录。一批可以混有本实例持有的所有分片的记录。

批大小是延迟与读取额度之间的取舍。批小，读取就小，而每次读取都要占用分片每秒五次调用中的一次。`count` 用一个结果结算整批。返回 `Vec<HandlerOutcome>` 的处理器逐条结算每条记录，每条记录各自推进自己分片的检查点（[检查点与投递保证](checkpoints.md)）。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#app"
```

</TabItem>
</Tabs>

处理器默认一次运行一次投递。属性中的 `workers(n)`，或手写形式中的 `workers` 步骤，最多并行运行 `n` 次投递；[分片、分区键与顺序](ordering.md)展示了如何在并行时保持每个键的顺序。

本 crate 通过流的共享吞吐量、用 `GetRecords` 读取。由 Kinesis Producer Library 聚合的记录不会交给处理器：订阅把它报告为 [`KinesisError::AggregatedRecord`](../components/broker/errors.md)，然后继续读取。

示例文件中的测试在进程内运行应用。批测试在暂停的时钟上启动，所以未满的批所等待的 50 毫秒会瞬间过去。
