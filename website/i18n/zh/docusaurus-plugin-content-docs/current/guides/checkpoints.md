---
title: 检查点与投递保证
sidebar_position: 4
description: "处理器的每种结果对分片的检查点有什么影响，重启或移交之后会重新读取什么，以及如何了结有毒的记录。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 检查点与投递保证

本指南展示处理器的每种结果对分片的检查点有什么影响，重启之后会重新读取什么，以及如何了结一条永远无法处理的记录。

Kinesis 不为每条记录、每个读取者保存任何状态。读取者记住自己在每个分片上读到了哪里，也就是它的检查点，下次从那里开始。在 RustStream 中，处理器的结果推进检查点。

## 服务 {/* #the-service */}

示例通过承运商发运订单。没有地址的货件永远无法发运。本版服务不认识的承运商的货件，要等到认识它的版本发布。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/checkpoints.rs#ship"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/checkpoints.rs#ship"
```

</TabItem>
</Tabs>

## 每种结果的作用 {/* #what-each-outcome-does */}

| 结果 | 记录 | 分片的检查点 |
|---|---|---|
| [`ack`](api:ruststream::runtime::HandlerOutcome::ack) | 已处理 | 分片中所有更早的记录都处理完后，越过这条记录 |
| [`drop`](api:ruststream::runtime::HandlerOutcome::drop) | 了结，永不再读 | 与 `ack` 相同 |
| [`retry`](api:ruststream::runtime::HandlerOutcome::retry)，挂载点未声明上限 | 保持未处理 | 停在它前面 |

检查点涵盖它之前的每条记录，所以它只越过一段连续的已处理记录。检查点移动时，本 crate 把它写入[租约存储](../components/leases/index.md)。由并行工作分区或在批中乱序处理的记录，要等它之前的记录也处理完，才会推进检查点。

`ship` 用 `drop` 了结没有地址的货件：再试一次也是同样的答复。

`retry` 要求 Kinesis 再次投递这条记录，而分片做不到对单条记录这样做：分片是按顺序读取的日志。这次投递得到 `AckError::Unsupported`，运行时把它记为警告，记录保持未处理。`ship` 就这样扣住承运商未知的货件。读取者继续读取：它后面的货件照常投递和处理，检查点则停在被扣住的货件前面。只有它所在的分片在等待，其他分片的检查点继续前进。在挂载点声明了上限时，`retry` 改为发布一份副本（[重试与死信流](retries.md)）。

## 重新读取什么 {/* #what-is-read-again */}

投递是至少一次的。取得分片的读取者从它的检查点之后开始，所以从第一条未处理的记录起，一切都会再投递一次：被扣住的货件，以及它后面的每个货件，不论是否已处理。服务重启时会这样，另一个实例接管该分片时也会这样（[多个实例](leases.md)）。因此，可能两次看到同一条记录的处理器要写成幂等的：同一个货件登记两次，结果只登记一次。

检查点保存在租约存储中，由存储决定重启后保留什么。默认的 [`MemoryLeaseStore`](../components/leases/memory-lease-store.md) 存在于进程中，启动时为空，所以重启后的服务在最新处打开每个分片，只读取此后发布的记录。[`DynamoLeaseStore`](../components/leases/dynamo-lease-store.md) 把检查点保存在一张表中，重启后的服务从上一个实例停下的地方继续。没有检查点的分片从它上一次读取所读到的第一条记录打开，存储把这条记录和检查点保存在一起。还没有人读过的分片在 [`start_at`](../components/positions/start-at.md) 指定的位置打开，未作指定时在最新处打开（[重放与 seek](replay.md)）。

只要同一个读取者持有该分片，被扣住的记录就不会回来。不重启而要再读它，处理器可以 [seek](replay.md) 回到它。

## 批与已关闭的分片 {/* #batches-and-closed-shards */}

返回一个结果的批处理器用这个结果结算批中的每条记录。返回 `Vec<HandlerOutcome>` 的批处理器逐条结算每条记录，每条记录各自推进自己分片的检查点。

被拆分或合并关闭的分片会读到末尾。只有它的每次投递都结算之后，它的检查点才变为 `SHARD_END`，它的子分片也在这之后才开始读取。因此，已关闭分片中被扣住的记录也会挡住它的子分片。

## 测试结果 {/* #testing-the-outcomes */}

示例文件中的测试在进程内运行应用，并检查每个货件如何结算。[进程内 Kinesis](../components/testing/index.md) 不保存检查点，所以被扣住的记录在那里也不会回来。重启后重新读取什么，要连接 LocalStack 运行，见[结算](../components/settlement/index.md)示例中的测试：它在一个比首次运行活得更久的存储上重启服务，最先看到的就是被扣住的记录。
