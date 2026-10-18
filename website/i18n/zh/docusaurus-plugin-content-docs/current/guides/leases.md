---
title: 多个实例
sidebar_position: 7
description: "在一个流上运行 Kinesis 服务的多个实例：DynamoDB 中的租约表把每个分片交给一个实例，并保存它的检查点。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 多个实例

本指南在一个流上运行同一服务的多个实例，由 DynamoDB 中的租约表把每个分片交给一个实例，并保存它的检查点。

Kinesis 不决定哪个消费者读取哪个分片。其他语言的 Kinesis Client Library 为此维护一张[租约表](https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html)，每个分片一行，记录它的所有者和检查点。`ruststream-kinesis` 通过[租约存储](../components/leases/index.md)维护同样的表。

## 单个实例 {/* #one-instance */}

没有指定存储的 Broker 把它的租约和检查点保存在进程内存中的 [`MemoryLeaseStore`](../components/leases/memory-lease-store.md) 里。这对单个实例是正确的。基于它的两个实例互不知晓，所以每个实例都读取每个分片，每条记录都被处理两次。重启后的实例从空存储开始，在最新处打开每个分片，除非 `start_at` 指定了另一个位置。

## 租约表 {/* #the-lease-table */}

```toml title="Cargo.toml"
[dependencies]
ruststream-kinesis = { version = "0.7", features = ["dynamodb-lease"] }
aws-config = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`dynamodb-lease` feature 加入 [`DynamoLeaseStore`](../components/leases/dynamo-lease-store.md)。它的表只有一个字符串分区键 `lease_key`，没有其他键；按需计费模式适合它。这张表和流一样是基础设施：用 Terraform、CloudFormation 或 AWS CLI 创建一次。在本地的 LocalStack 上，把 `dynamodb` 加入 compose 文件的 `SERVICES`，然后在那里创建它：

```bash
aws dynamodb create-table --table-name warehouse-leases \
  --attribute-definitions AttributeName=lease_key,AttributeType=S \
  --key-schema AttributeName=lease_key,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

一张表服务于一个服务的所有流：一行对应一个流的一个分片，键为 `stream:shard`。

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/leases.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/leases.rs#service"
```

</TabItem>
</Tabs>

存储和 Broker 使用同一份 AWS 配置，所以两者访问同一个账户和区域。`aws_config::defaults(..).load()` 从环境读取配置，而且它是 `async` 的，所以 `main` 先加载配置，再构建应用，并用 `run` 运行它。[`KinesisBroker::from_config`](../components/broker/credentials.md) 接收这份配置，[`lease_store`](api:ruststream_kinesis::KinesisBroker::lease_store) 接收存储。用相同的环境运行服务的两个副本，它们就共享 `warehouse-orders` 的分片。

## 分片如何共享 {/* #how-the-shards-are-shared */}

每个实例在订阅打开时列出分片，之后每十秒再列一次。它获取每个没有所有者或租约已过期的分片的租约，并读取这些分片。租约持续十秒，持有者每三秒续约一次。对表的每次写入都是条件写入，所以两个实例绝不会同时持有一个分片，检查点也只在写入者持有租约时才会落地。

实例之间不平衡分片：最先列出分片的实例获取所有空闲的分片。其他实例在以下情况获取分片：持有者停止并释放它的租约，或者持有者崩溃、断网，导致租约过期。续约失败的实例立即停止读取该分片，不写入检查点。

## 移交后重新读取什么 {/* #what-a-handover-reads-again */}

接管分片的实例从上一个持有者写入的检查点之后继续。上一个持有者在最后一个检查点之后处理过的记录会再投递一次，它[扣住](checkpoints.md)的记录及其后的一切也是如此。重启后的实例以同样的方式继续，因为检查点保存在表中。

## 为每个实例命名 {/* #naming-each-instance */}

每个 Broker 都有自己的所有者 id，对该 Broker 实例唯一，表在 `lease_owner` 列中显示它。[`owner_id`](../components/leases/owner-id.md) 改为由你为实例命名，比如用它所在 pod 的名字。以同一个名字重启的实例立即拿回它的租约，不必等它们过期。切勿让两个同时运行的实例共用一个固定的名字：两者会持有同样的分片并读取它们。

## 升级表与其他存储 {/* #upgrading-a-table-and-other-stores */}

本 crate 早期版本写入的表只按分片 id 作为行的键。[`legacy_stream`](../components/leases/legacy-stream.md) 指定这些行所属的流，于是升级后的服务从它们继续。基于其他数据库的存储实现 [`LeaseStore`](api:ruststream_kinesis::LeaseStore)：五个调用，分别获取、续约、记录检查点、读取和释放租约。

## 测试 {/* #testing */}

第一个测试在进程内启动应用，配上一份不做任何 I/O 构建的配置。[进程内 Kinesis](../components/testing/index.md) 不查询任何租约存储，所以它测试的是处理器。第二个测试连接 LocalStack 运行：它在一张新表上启动两个实例，停止第一个，然后看到第二个从第一个的检查点之后接管该分片。

```bash
KINESIS_TEST_ENDPOINT=http://localhost:4566 cargo test -- --test-threads=1
```

本 crate 自己针对租约表、连接真实服务的测试位于 `tests/leases_kinesis.rs`。
