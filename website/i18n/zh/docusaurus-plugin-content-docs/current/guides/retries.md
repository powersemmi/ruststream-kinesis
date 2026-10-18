---
title: 重试与死信流
sidebar_position: 5
description: "立即或稍后重试一条 Kinesis 记录，限定尝试次数，把一直失败的记录转入死信流，并了解副本对顺序的影响。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重试与死信流

本指南立即或稍后重试一条记录，限定尝试次数，并把一直失败的记录转入[死信流](../components/settlement/dead-letters.md)。

Kinesis 既不扣留记录，也不统计投递次数，也没有死信队列。
这三件事都由框架来做：挂载点声明上限和死信流，重试以记录副本的形式传递，消息头里带着重试次数。

示例是一个退款服务，它通过发卡处理商发出退款。

## 订阅者 {/* #the-subscriber */}

处理商有三种拒绝退款的方式，每种需要不同的结果。
`Busy` 表示它的速率限额已经用完：立即重试仍会被拒绝，所以 [`retry_after`](../components/settlement/retry-after.md) 等待 30 秒。
`Unavailable` 表示它没有应答：`retry` 立即再试一次。
`Declined` 表示卡账户已经关闭，再试一次也是同样的答复，所以这笔退款被丢弃。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

[`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after) 结算这条记录，并在延迟过去后把一份副本发布回 `refunds`。
副本带着重试次数消息头（[`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)），其值加一。
副本在服务的内存中等待，所以它是至多一次的：在延迟结束前停止的进程会丢失它，而原记录已经位于检查点之后。

在声明了上限时，[`retry`](api:ruststream::runtime::HandlerOutcome::retry) 也会变成一份副本，并立即发布。
没有上限时，它让分片的检查点停在这条记录前面，[检查点与投递保证](checkpoints.md)展示了这一点。

## 上限与死信流 {/* #the-cap-and-the-dead-letter-stream */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) 统计一笔退款的每次投递，第一次也算在内。
第五次投递不再重试：[`dead_letter`](api:ruststream::runtime::Mounting::dead_letter) 把它发布到 `refunds.review`，载荷和消息头保持到达时的样子。
设置了上限却没有死信流时，检查点会像 `drop` 那样越过用尽尝试的记录。
被丢弃的退款完成结算，不会去任何地方；死信流只存放用完了尝试次数的退款。
这些流属于服务的基础设施；示例为本地的 LocalStack 用 `create_if_missing` 创建它们。

## 读取死信流 {/* #reading-the-dead-letter-stream */}

死信流是一个普通的流，`review` 像读取其他流一样读取它。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

转入死信的记录，其重试次数消息头保存它经历的投递次数，这里是 5。
`review` 用 `ctx.headers()` 读取它，并把这笔退款记入日志，留给人来查看。

## 副本与顺序 {/* #copies-and-order */}

副本回到订阅读取的流，所以 Kinesis 上的注册不需要自己的重试目的地。
副本保留记录的分区键，所以它落在该键所在的分片上。
不过它落在这个分片的最新处，排在它等待期间发布的记录之后。
因此等待 30 秒的退款，会在同一张卡后来到达的退款之后才被处理。

在键的顺序比继续向前更重要的地方，不要声明上限，让 `retry` 挡住分片：检查点停在这条记录上，下次获取该分片的租约时，从这条记录重新读取分片。
[`out_retry`](../components/settlement/out-retry.md) 替换副本发出所经由的发布者，因此转换可以在每份副本发出时给它打上标记。

## 测试延迟 {/* #testing-the-delay */}

示例文件中的测试在暂停的时钟上于进程内运行应用，所以 `tb.advance(BACKOFF)` 让 30 秒瞬间过去。
`TestApp::with_state` 启动应用，配上一个繁忙或不应答的替身处理商。
[测试 Kinesis 服务](testing.md)讲解了测试工具。
