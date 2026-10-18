---
title: 分片、分区键与顺序
sidebar_position: 3
description: "用分区键为每条记录选定分片，在处理器并行运行时保持每个键的顺序，并在拆分与合并之后仍保持它。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 分片、分区键与顺序

本指南用分区键为每条记录选定分片，在处理器并行运行时保持每个键的顺序，并在流重新分片时仍保持它。

示例是一个订单服务。它把每个订单记入客户的账簿，一次处理四个订单，并为每条账簿条目发出一张收据作为确认。

## 键如何选定分片 {/* #how-a-key-picks-a-shard */}

每条 Kinesis 记录都有一个分区键。Kinesis 用 MD5 对键做哈希，哈希值落在哪个分片的[哈希键范围](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html)内，记录就存放在哪个分片上。Kinesis 在一个分片内保持记录的顺序，分片之间没有任何顺序。因此同一个键的记录保持顺序，不同键的记录分散到各个分片上。

一个键只能得到一个分片的吞吐量：一个分片每秒最多接收 1 MB 或 1000 条记录。承载大部分流量的键，会让它所在的分片成为整个流的瓶颈。

## 每个客户内的顺序 {/* #order-per-customer */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#post"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#post"
```

</TabItem>
</Tabs>

商店以客户作为分区键发布每个订单，所以一个客户的订单按下单顺序位于同一个分片上。`customer-orders` 流有两个分片，订阅同时读取这两个分片。

`workers(4, by_key)` 运行四个工作分区；键相同的投递在同一个工作分区上依次运行。投递的键就是它的分区键，由 `partition-key` [消息头](../components/context/headers.md)（[`PARTITION_KEY_HEADER`](api:ruststream_kinesis::PARTITION_KEY_HEADER)）携带。因此 `post` 同时处理四个订单，却绝不会同时处理同一个客户的两个订单。在手写形式中，同一设置是 [`workers_by_key`](api:ruststream::runtime::SubscriberSettings::workers_by_key)。普通的 `workers(4)` 不考虑键，同时运行四次投递，同一个客户的两个订单就可能以任意顺序完成。

## 为记录指定键 {/* #naming-the-key-of-a-record */}

`post` 用发布上的步骤 [`partition_key(..)`](../components/publishing/partition-key.md) 写入每条账簿条目。账簿条目落在其客户所在的分片上，所以账簿的读者也按顺序看到每个客户。这个步骤是 Kinesis 的设置，所以槽位要以 `Publisher<Options = KinesisPublishOptions>` 为约束才能提供它。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#confirm"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#confirm"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/ordering.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/ordering.rs#app"
```

</TabItem>
</Tabs>

回复没有可以加步骤的调用，所以由挂载点在策略上指定它的键：[`.out_reply(Publish::default()).partition_key("receipts")`](../components/publishing/publish-settings.md)。于是每张收据都落在同一个分片上，读者按同一个顺序看到收据，代价是只能使用这一个分片的吞吐量。

一次发布按以下顺序确定它的[键](../components/publishing/keys.md)：

1. 这次发布的 `partition_key(..)` 步骤。
2. 这次发布带有的 `partition-key` 消息头：手工写入，或者随来自 RustStream 另一个 Broker 的记录一起到达。
3. 挂载点在策略上指定的键。
4. 为每条记录生成的新键，它把记录分散到各个分片上。

最后一级不保持任何顺序：先后发布的两条记录可能落在不同的分片上。示例的测试用消息头设置键，这是 RustStream 每个 Broker 都能读取的可移植写法。

## 拆分与合并 {/* #splits-and-merges */}

拆分关闭一个分片，并打开两个分享其哈希键范围的子分片。合并关闭两个相邻的分片，并打开一个子分片。已关闭的父分片保留它们的记录，直到保留期结束。订阅每十秒重新列出一次分片。只有当子分片的每个父分片都已读到末尾，订阅才开始读取该子分片，所以一个键的记录在这一变化前后保持顺序。没有检查点的子分片从它最旧的记录打开，所以读取者启动之前写入其中的记录不会遗漏。[分片与重新分片](../architecture/kinesis/shards.md)用图展示了父分片和子分片，本 crate 连接真实服务的测试在 `tests/shards_kinesis.rs` 中拆分并合并一个流。

[延迟重试](retries.md)把记录的副本发布到流的最新处，所以被重试的记录会失去它在同一个键的记录中的位置。

## 测试 {/* #the-tests */}

示例文件中的测试在进程内运行应用。[进程内 Kinesis](../components/testing/index.md) 给每个流一个分片，所以通过两个分片读取订单的测试要连接 LocalStack 运行：

```bash
cargo test
KINESIS_TEST_ENDPOINT=http://localhost:4566 cargo test -- --test-threads=1
```
