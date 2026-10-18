---
title: 在生产环境中运行
sidebar_position: 9
description: "在 AWS 上运行 Kinesis 服务：凭据和区域来自环境或你自己的配置，流由基础设施拥有，以及服务所需的 IAM 权限。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在生产环境中运行

本指南在 AWS 上运行一个 Kinesis 服务：它的凭据和区域从哪里来，它期望找到哪些流，以及它需要的 IAM 权限。

## 来自环境的凭据和区域 {/* #credentials-and-region-from-the-environment */}

`KinesisBroker::new()` 不从代码中接收任何东西。服务启动时，AWS SDK 的[默认提供链](../components/broker/credentials.md)找到区域和凭据：环境变量、共享的 `~/.aws` 文件、SSO，以及容器或实例的角色。在 ECS、EKS 或 EC2 上以 IAM 角色运行的服务完全不需要配置。Broker 用 SDK 最新的行为版本构建它的配置。

SDK 在为第一个请求签名时读取凭据。这个请求就是启动时对第一个订阅的流所做的检查，所以凭据错误的服务在处理任何记录之前就会停止。

## 你自己的配置 {/* #a-configuration-of-your-own */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#config"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#config"
```

</TabItem>
</Tabs>

[`KinesisBroker::from_config`](api:ruststream_kinesis::KinesisBroker::from_config) 接收服务构建的 `SdkConfig`。`aws_config` 用一个 `async` 加载器从环境构建配置，而应用函数是同步的。所以示例用不做任何 I/O 的 `SdkConfig::builder()` 构建配置。配置文件提供者在第一个请求时读取 `orders-service` 配置文件；带有 `role_arn` 的配置文件会代入该角色。固定的行为版本让 SDK 更新时，它的默认值不会在服务不知情时改变。Broker 保留配置的全部设置，只有在 Broker 自身上指定的 [`endpoint` 和 `region`](../components/broker/endpoint.md) 会取代配置中的值。[`DynamoLeaseStore`](../components/leases/dynamo-lease-store.md) 接收同一份配置（[多个实例](leases.md)）。示例还需要两个依赖：

```toml title="Cargo.toml"
[dependencies]
aws-config = "1"
aws-sdk-kinesis = "1"
```

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

`orders` 流属于基础设施：由 Terraform、CloudFormation 或控制台创建，连同它的容量模式、保留期和加密设置。流用名称指定；账户和区域来自配置。描述符省去了 [`create_if_missing`](../components/subscriptions/create-if-missing.md)，所以缺失的流会让服务在启动时停止，并返回指出该流的错误，而不是创建一个空流。`create_if_missing(n)` 创建一个有 `n` 个分片的预置模式流，适合本地的 LocalStack；生产流采用预置模式还是按需模式，由运维人员决定。

请求经由 SDK 默认的 HTTPS 客户端发送，SDK 按自己的重试策略重试失败的请求。运行在专用线程上的处理器（`threads(n)`）通过它自己的 SDK 客户端发送，该客户端由同一份配置构建，并共享其凭据。

## IAM 权限 {/* #iam-permissions */}

上面的服务需要对它的流拥有以下权限：

```json title="policy.json"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:DescribeStreamSummary",
        "kinesis:ListShards",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords"
      ],
      "Resource": "arn:aws:kinesis:eu-west-1:123456789012:stream/orders"
    }
  ]
}
```

每项功能各自增加所需的权限：

| 功能 | 权限 |
|---|---|
| 发布、回复或重试副本 | 对目标流的 `kinesis:PutRecord` |
| `create_if_missing` | `kinesis:CreateStream` 和 `kinesis:DescribeStream` |
| `DynamoLeaseStore` | 对租约表的 `dynamodb:GetItem` 和 `dynamodb:UpdateItem` |

## AsyncAPI 文档显示什么 {/* #what-the-asyncapi-document-shows */}

启用 `asyncapi` feature 后，服务的 AsyncAPI 文档指定一个[服务器](../components/asyncapi/server.md)：Broker 端点所在的主机；没有端点时，是其区域的区域主机，例如 `kinesis.eu-west-1.amazonaws.com`。SDK 从环境中解析的区域要到连接时才知道，那时文档已经构建完成，所以这样的 Broker 报告 `kinesis.amazonaws.com`。文档从不包含凭据，所以可以公开发布。

示例文件中的测试在进程内启动同一个应用；进程内 Kinesis 不签名任何请求，也不读取任何配置文件。[测试 Kinesis 服务](testing.md)讲解了测试工具。
