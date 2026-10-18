---
title: 快速开始
sidebar_position: 1
description: "安装 ruststream-kinesis，启动本地 Kinesis，运行第一个订阅者，并在没有服务端的情况下测试它。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 快速开始

本指南安装 `ruststream-kinesis`，启动一个本地 Kinesis，并让第一个服务连上它运行。

## 安装 {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-kinesis = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` 是框架本身：`macros` 提供属性宏，`json` 提供默认编解码器，`logging` 把服务的日志打印到终端。
`ruststream-kinesis` 是 Kinesis Broker，基于官方客户端 `aws-sdk-kinesis` 构建。
它的 `testing` feature 加入测试所用的[进程内 Kinesis](../components/testing/index.md)。

这个 Broker crate 有以下 feature，默认全部关闭：

| Feature | 提供的内容 |
|---|---|
| `dynamodb-lease` | [`DynamoLeaseStore`](../components/leases/dynamo-lease-store.md)，让服务的多个实例共享分片（[多个实例](leases.md)） |
| `asyncapi` | 生成的 [AsyncAPI](../components/asyncapi/index.md) 文档中每个通道的 Kinesis 细节 |
| `testing` | `TestApp` 背后的进程内 Kinesis |

## 本地 Kinesis {/* #a-local-kinesis */}

[LocalStack](https://github.com/localstack/localstack) 在一个容器里运行 Kinesis：

```yaml title="compose.yaml"
services:
  localstack:
    image: localstack/localstack:4.14.0
    ports:
      - "4566:4566"
    environment:
      SERVICES: kinesis
      KINESIS_LATENCY: 0
```

```bash
docker compose up -d
```

LocalStack 在 `http://localhost:4566` 上应答，启动时没有任何流。
LocalStack 默认给每次 Kinesis 调用加上延迟，`KINESIS_LATENCY: 0` 关闭这一延迟。

## 第一个服务 {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` 从 `orders` 流读取 JSON 订单。
带金额的订单被 ack，没有金额的订单被丢弃。
[`KinesisStream`](../components/subscriptions/kinesis-stream.md) 指定流，[`create_if_missing(1)`](../components/subscriptions/create-if-missing.md) 在订阅打开时创建只有一个分片的流。
生产服务省去这一步，由基础设施提供它的流。
[`KinesisBroker::new`](../components/broker/index.md) 只记录配置。
[`endpoint`](../components/broker/endpoint.md)、[`region`](api:ruststream_kinesis::KinesisBroker::region) 和 [`test_credentials`](api:ruststream_kinesis::KinesisBroker::test_credentials) 让它指向本地的 LocalStack。
SDK 客户端在服务启动时才构建。
`#[ruststream::app]` 把 `app` 变成 `main`；在手写形式中，[`run_main`](api:ruststream::runtime::cli::run_main) 做同样的事。

运行服务：

```bash
cargo run -- run
```

订阅在流的最新处打开，所以它读取服务启动之后发布的记录。
在另一个终端里，用 AWS CLI 发布一个订单：

```bash
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
aws kinesis put-record --stream-name orders --partition-key customer-7 \
  --cli-binary-format raw-in-base64-out --data '{"id":1,"amount_cents":1250}'
```

服务记录一条带订单 id 的 `order taken` 日志。
<kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。

## 第一个测试 {/* #the-first-test */}

示例文件里还有两个测试。
每个测试都用 `TestApp::start` 启动同一个 `app`，发布一个订单，然后检查 `take_order` 如何结算它。
在测试中，`KinesisBroker` 连接的是进程内 Kinesis，所以测试不需要 LocalStack：

```bash
cargo test
```

[测试 Kinesis 服务](testing.md)逐一讲解这些测试，以及进程内 Kinesis 模拟了什么。

## 从 aws-sdk-kinesis 迁移 {/* #coming-from-aws-sdk-kinesis */}

Rust SDK 一次只读取一个分片、发起一次调用，基于它的服务要围绕这些调用自己编写消费者。
下表列出每一部分在 RustStream 中的位置：

| aws-sdk-kinesis | RustStream |
|---|---|
| `aws_config::load_from_env` 和 `Client::new` | [`KinesisBroker::new`](api:ruststream_kinesis::KinesisBroker::new)，或者配上你自己的 `SdkConfig` 的 [`from_config`](../components/broker/credentials.md)（[在生产环境中运行](production.md)） |
| `ListShards` 加上每个分片一次 `GetShardIterator` | [`KinesisStream::new`](../components/subscriptions/kinesis-stream.md) 中的流名称；订阅自己找到[分片](../components/subscriptions/shards.md)，并跟随拆分与合并 |
| 每个分片一个 `GetRecords` 循环 | 一个 `#[subscriber]` 处理器（[读取流](consuming.md)） |
| 解码 `record.data()` | 处理器的参数类型，由编解码器解码 |
| 处理完成后写入的检查点 | 处理器的结果（[检查点与投递保证](checkpoints.md)） |
| 让多个实例共享分片的租约表 | [`DynamoLeaseStore`](../components/leases/dynamo-lease-store.md)（[多个实例](leases.md)） |
| 带 `TRIM_HORIZON` 或 `AT_TIMESTAMP` 的 `GetShardIterator` | 挂载点上的 [`start_at`](../components/positions/start-at.md)，或处理器中的 seek 句柄（[重放与 seek](replay.md)） |
| 带分区键的 `PutRecord` | 回复或 `Out` 参数，经由 [`KinesisPublish`](../components/publishing/index.md) 发送（[分片、分区键与顺序](ordering.md)） |
