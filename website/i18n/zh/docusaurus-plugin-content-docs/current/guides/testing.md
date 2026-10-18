---
title: 测试 Kinesis 服务
sidebar_position: 8
description: "用 TestApp 在进程内测试 Kinesis 服务：生产环境的应用、带键的记录、回复、延迟，以及在 LocalStack 上运行的同一个测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 测试 Kinesis 服务

测试用 [`TestApp`](api:ruststream::testing::TestApp) 运行服务自己的应用，连接的是[进程内 Kinesis](../components/testing/index.md) 或者 LocalStack。

被测服务负责银行卡扣款。付款到达 `card-payments`，它的分区键就是要扣款的账户。处理器在 `card-receipts` 上以一张收据作为回复。没有金额的付款被丢弃。超出银行即时限额的付款会被暂缓：处理器用 [`retry_after`](../components/settlement/retry-after.md) 要求一分钟后再投递它。经过三次投递后，付款进入[死信流](../components/settlement/dead-letters.md) `card-payments.review`。应用由 `service(broker)` 构建，所以测试可以选择 Broker，而 `main` 使用生产环境的 Broker。测试需要在 `[dev-dependencies]` 中启用 `ruststream-kinesis` 的 `testing` feature。

## 带键的付款 {/* #a-keyed-payment */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) 把 [`KinesisBroker`](api:ruststream_kinesis::KinesisBroker) 连接到进程内 Kinesis。测试不调用任何 AWS 端点，也不需要凭据。测试按生产类型访问 Broker：`tb.broker::<KinesisBroker>()`。记录的分区键以 `partition-key` 消息头（[`PARTITION_KEY_HEADER`](api:ruststream_kinesis::PARTITION_KEY_HEADER)）传递，所以测试用 `with_headers` 设置它。Broker 把这个消息头变成记录的分区键，投递再以同一个消息头把它报告回来。`publish` 在处理器及其回复都完成结算后才返回。所以接下来的几行断言的是已经完成的状态：订阅者如何结算，以及什么到达了 `card-receipts`。测试主体把已启动的应用作为参数接收，所以下面连接真实服务的测试也运行它。

## 没有金额的付款 {/* #a-payment-without-an-amount */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` 检查处理器返回的结果。`assert_not_called` 检查没有发布任何收据。

## 不必等待的延迟 {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

测试在暂停的时钟上启动，[`advance`](api:ruststream::testing::TestApp::advance) 让时钟前进。一分钟的 `HOLD` 不花费任何时间。Kinesis 无法扣留记录，所以 `retry_after` 在延迟结束时把记录的副本发布到它的流。副本在消息头中带着重试次数和记录的分区键，`published` 把它和测试自己的记录一起显示在 `card-payments` 上。第三次投递是挂载点允许的最后一次，所以这条记录按到达时的样子进入 `card-payments.review`。每次被暂缓的投递都记录为 `Outcome::Nack`。

## 进程内 Kinesis 模拟了什么 {/* #what-the-in-process-kinesis-models */}

进程内 Kinesis 像 Kinesis 服务端为 Broker 所做的那样写入和读取记录。分区键的规则相同，用户消息头使用同样的信封，一次投递带有的消息头和上下文与从分片读出的投递相同：

- 一个流上的每个订阅都读取该流的每条记录，并在最新处打开。
- [`start_at`](../components/positions/start-at.md) 和处理器的 seek 句柄从它们指定的位置重新读取流的日志（[重放与 seek](replay.md)）。
- 没有上限的 `retry` 得到与 Kinesis 相同的答复，即不受支持，记录不会回来。
- 延迟重试以计数的副本回来，用尽尝试的记录进入死信流。
- Kinesis 服务端拒绝的记录在这里同样被拒绝：超过 128 个字符或不符合 `[a-zA-Z0-9_.-]` 的流名称，为空或超过 256 个字符的分区键，超过 10 MiB 的记录（这是流最多能配置接收的大小）。发布这种记录的测试会得到服务端返回的错误。
- 在 `shutdown` 之后使用的发布者或 seek 句柄会报告 [`KinesisError::NotConnected`](../components/broker/shutdown.md)。

在那里，每个流只有一个分片，所以每条记录与其他每条记录之间都有顺序，服务指定的每个流都存在。多个分片、租约与检查点、保留期以及重新分片，都需要 LocalStack。

## 在 LocalStack 上运行同一个测试 {/* #the-same-test-on-a-stack */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) 把同一个应用连接到正在运行的 Kinesis，这里是 LocalStack。一次发布最多等待十秒，让它引发的反应完成结算；[`start_live_within`](api:ruststream::testing::TestApp::start_live_within) 设置更长的截止时间。测试用 `support::endpoint`（示例自带的辅助函数）从 `KINESIS_TEST_ENDPOINT` 读取端点，未设置时跳过。服务在订阅打开时创建 `card-payments`；测试创建只用于接收的 `card-receipts`。订阅在最新处打开，所以之前运行留在流中的记录不会被读到。断言与进程内的完全相同。

```bash
cargo test
KINESIS_TEST_ENDPOINT=http://localhost:4566 cargo test -- --test-threads=1
```
