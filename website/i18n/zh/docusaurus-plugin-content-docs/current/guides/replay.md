---
title: 重放与 seek
sidebar_position: 6
description: "从最旧的记录或从某个时刻重新读取 Kinesis 流，并在处理器内部移动正在运行的订阅。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重放与 seek

本指南从最旧的记录或从某个时刻重新读取流，并在处理器内部移动正在运行的订阅。

消费者读过一条记录之后，Kinesis 仍然保留它，直到流的保留期结束。服务可以再读一遍：重建读模型，为新的消费者回填数据，或者跳过它无法使用的记录。订阅在挂载点上取得起始位置，处理器则通过投递的上下文移动自己的订阅。

## 读模型 {/* #the-read-models */}

服务在内存中维护两个读模型：每个账户的余额和每张卡的消费额。每条记账都带有账簿的修订号。对过去记账的更正作为新的修订发布，读模型遇到新修订时，会从流的最旧记录开始重建自己。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#projection"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#projection"
```

</TabItem>
</Tabs>

## 从最旧的记录开始 {/* #start-from-the-oldest-record */}

余额保存在内存中，所以重启之后要从整个流重新构建。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#balance"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#balance"
```

</TabItem>
</Tabs>

[`start_at(KinesisPosition::horizon())`](../components/positions/start-at.md) 在每个分片的裁剪起点（trim horizon），即流仍保留的最旧记录处，打开该分片。它在每次启动时生效，作用于订阅开始时打开的分片，并优先于任何已存储的[检查点](checkpoints.md)。不用 `start_at` 时，分片从它的检查点之后继续，没有检查点时在最新处打开。

[`KinesisPosition`](../components/positions/index.md) 有四种位置：

| 位置 | 订阅从哪里读取 |
|---|---|
| [`horizon()`](api:ruststream_kinesis::KinesisPosition::horizon) | 每个分片最旧的记录 |
| [`latest()`](api:ruststream_kinesis::KinesisPosition::latest) | 每个分片的最新处，因此只读取新记录 |
| [`timestamp(ms)`](api:ruststream_kinesis::KinesisPosition::timestamp) | 每个分片中在 `ms` 或之后到达的第一条记录 |
| [`sequence(shard, seq)`](api:ruststream_kinesis::KinesisPosition::sequence) | 某个分片中的某一条记录，以及它之后的记录 |

流默认保留记录 24 小时，使用[延长保留期](https://docs.aws.amazon.com/streams/latest/dev/kinesis-extended-retention.html)最长可达 365 天。超过保留期的记录不复存在，裁剪起点也随之前移。

## 在处理器中 seek {/* #seek-from-a-handler */}

同一个处理器在收到更新修订的记账时重建余额。[`Ctx<SeekHandle>`](../components/positions/seek.md) 把处理器所在订阅的 [`KinesisSeeker`](api:ruststream_kinesis::KinesisSeeker) 交给它。[`seek`](api:ruststream::Seeker::seek) 把订阅移到最旧的记录，处理器再读一遍每条记账。触发重建的那条记账也会回来，这一次投影会应用它。

seek 可以使用上面任何一种位置。针对整个流的位置，会移动本实例持有的每个分片。`Ctx<Position>` 保存投递自身的位置，即一个 `sequence` 位置：seek 到那里，会从这条记录开始重新读取该分片，不移动其他任何分片。这要求本实例持有该分片；seek 到另一个实例正在读取的分片或已读完的分片，会返回错误。

## 在批处理器中 seek {/* #seek-from-a-batch-handler */}

卡的记账数量很大，所以消费额以最多 500 条的[批](../components/subscriptions/batches.md)构建。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#batch"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#batch"
```

</TabItem>
</Tabs>

批处理器声明 [`KinesisBatchContext`](../components/context/batch-context.md)，而不是单次投递的上下文。它带有同样的 `SeekHandle` 键，但没有位置，因为一批跨越许多记录。需要某一条记录位置的处理器主体，读取它的 `kinesis-shard-id` 和 `kinesis-sequence-number` 消息头。

## 从某个时刻开始 {/* #start-from-a-moment */}

付款审查在每次启动时读取最近一天的付款。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#payouts"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#payouts"
```

</TabItem>
</Tabs>

这个时刻要到服务启动时才知道，所以由挂载点而不是属性指定位置。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/replay.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#service"
```

</TabItem>
</Tabs>

`KinesisPosition::timestamp` 接收自纪元起的毫秒数。Kinesis 把它与自己给每条记录打上的到达时间戳相比较。这个时间戳是近似值，所以在同一时刻前后到达的记录无法精确区分。

## seek 与检查点 {/* #seeks-and-checkpoints */}

seek 会清除它移动的每个分片为检查点所做的跟踪。在 seek 之前读取的投递，结算后不产生任何效果，所以触发重建的那条记账的 ack 不推进任何检查点。因此，之后的检查点绝不会越过一条被 seek 重放、但还没有处理器处理过的记录。seek 只移动本实例：其他实例继续读取它们持有的分片，接管某个分片的实例从它的检查点继续。

示例文件中的测试在进程内运行应用。[进程内 Kinesis](../components/testing/index.md) 保存每个流的完整日志，所以 seek 在那里同样会重新读取它。
