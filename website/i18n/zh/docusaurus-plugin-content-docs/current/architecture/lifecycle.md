---
title: 生命周期
sidebar_position: 3
description: "KinesisBroker::new 只记录配置，connect 加载 AWS 配置，每个订阅打开它的分片，shutdown 关闭连接。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Broker 的各个状态作为类型，以及每次转换在 AWS 上做什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "aws": "在 AWS 上",
  "unconnected": ["KinesisBroker::new()"],
  "connected": ["ConnectedKinesis", "Broker"],
  "closed": ["Closed = ()", "没有要报告的内容"],
  "config": ["只记录配置，", "无 I/O"],
  "load": ["AWS 配置、", "客户端、租约存储、", "所有者 id"],
  "subscriptions": ["每个订阅：", "DescribeStreamSummary", "或 CreateStream，", "ListShards，每个分片", "一个租约和", "一个迭代器"],
  "running": ["每 10 秒列出", "分片，每 3 秒", "续约"],
  "flag": ["连接被标为", "已关闭"],
  "afterShutdown": ["比连接活得更久的发布者或 seek 句柄", "返回 KinesisError::NotConnected，绝不默默成功"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# 生命周期

[`KinesisBroker::new`](api:ruststream_kinesis::KinesisBroker::new) 只记录配置，connect 加载 AWS 配置，启动时每个订阅打开它的分片，shutdown 关闭连接。

<Diagram name="lifecycle" words={words} />

`new`、`from_config`、`endpoint`、`region`、`test_credentials`、`lease_store` 和 `owner_id` 都不做 I/O，所以服务的构建函数保持同步。Broker 的克隆共用它的连接。在 connect 之前从 Broker 取得的发布者，在运行时连接 Broker 之后即可工作。

connect 通过 SDK 的默认链加载 AWS 配置，使用 Broker 指定的端点和区域。用 `from_config` 构建的 Broker 保留它的 `SdkConfig`，只有 Broker 指定的端点和区域会取代配置中的值。随后 connect 构建 Kinesis 客户端。未设置 `lease_store` 时，Broker 把租约保存在 [`MemoryLeaseStore`](api:ruststream_kinesis::MemoryLeaseStore) 中；未设置 `owner_id` 时，它采用一个自己独有的租约所有者 id。SDK 延迟获取凭证，在第一次请求时才获取。在服务中，这个请求就是第一个订阅的 `DescribeStreamSummary`，因此凭证错误会让启动在订阅这一步失败。

每个订阅的 [`KinesisStream`](api:ruststream_kinesis::KinesisStream) 先接受检查：名称为空，或者设置了 `create_if_missing` 却没有指定分片数，都会在调用 AWS 之前返回 [`KinesisError::Invalid`](api:ruststream_kinesis::KinesisError::Invalid)。Broker 已经通过另一个订阅读取的流也会被拒绝。随后订阅用 `DescribeStreamSummary` 查询这个流。流不存在时返回 [`KinesisError::Stream`](api:ruststream_kinesis::KinesisError::Stream)，除非 `create_if_missing` 创建了它，并最多等待一分钟。

订阅在开始读取时返回。协调任务的第一轮列出分片，获取它能获取的每个分片的租约，并为每个分片启动一个读取者。这些读取者都在 `subscribe` 返回之前取得第一个迭代器。因此分片打开时的最新处就是订阅打开的时刻，此后发布的记录都会被读到。第一轮中的错误在它运行时报告在订阅的流上。

服务运行期间，协调任务每 10 秒列出一次分片，并为它现在能获取的分片启动读取者。每个读取者每 3 秒续约一次。订阅被丢弃时，读取者停止并释放它的租约，以便另一个实例能立即获取该分片。

shutdown 把连接标为已关闭并返回 `()`：SDK 客户端没有需要关闭或刷出的东西。在 shutdown 之前创建的发布者或 seek 句柄，此后返回 [`KinesisError::NotConnected`](api:ruststream_kinesis::KinesisError::NotConnected)。
