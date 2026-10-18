---
title: 设计
sidebar_position: 2
description: "ruststream-kinesis 的位置：在 RustStream 运行时与 AWS SDK 之间，每个分片一个读取者，旁边是租约存储，每个运行时一个 SDK 客户端。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "自上而下依次是服务、RustStream 运行时、本 crate、AWS SDK 客户端，以及 Kinesis 和 DynamoDB",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "订阅者处理器"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream 核心",
  "runtime": "运行时：分发、工作分区、结算",
  "crate": "ruststream-kinesis",
  "subscriber": ["KinesisSubscriber", "每个分片一个读取者"],
  "message": ["KinesisMessage", "ack = 检查点"],
  "publisher": ["KinesisPublisher", "一次 PutRecord"],
  "descriptor": ["KinesisStream", "描述符"],
  "broker": ["KinesisBroker", "配置"],
  "policy": ["KinesisPublish", "策略"],
  "leases": ["LeaseStore", "租约、检查点"],
  "sdk": "AWS SDK for Rust",
  "kinesisClient": ["aws-sdk-kinesis", "每个运行时一个客户端"],
  "dynamoClient": ["aws-sdk-dynamodb", "启用 dynamodb-lease 时"],
  "kinesis": ["Kinesis", "Data Streams"],
  "dynamodb": ["DynamoDB", "租约表"],
  "inProcess": ["在 testing 特性下，一个进程内的日志", "取代客户端和 Kinesis"],
  "mounted": "挂载到",
  "polls": "轮询流",
  "https": "HTTPS"
};

# 设计

`ruststream-kinesis` 所处的位置：在 RustStream 运行时与 AWS SDK 之间，每个分片一个读取者，旁边是租约存储，每个运行时一个 SDK 客户端。

<Diagram name="design" words={words} />

本 crate 在 [AWS SDK for Rust](https://docs.rs/aws-sdk-kinesis/latest/aws_sdk_kinesis/) 之上实现核心的 Broker trait。它上面的运行时对每个 Broker 都相同。Kinesis 特有的一切都在本 crate 里：[`KinesisStream`](api:ruststream_kinesis::KinesisStream) 描述符、分片读取者、租约、结果对检查点的作用，以及记录如何携带消息头。本 crate 支持订阅、接收批、重新定位订阅、把分区键交给按键分发，并在生成的文档中描述它的服务器。它没有事务，也没有请求-应答。

每个订阅对应一个流。一个协调任务列出流的分片，为它能获取的每个分片获取租约，并为它持有的每个分片启动一个读取任务。所有读取者都把投递送进同一个容量为 64 次投递的通道，订阅的流从中取出它们。批订阅在客户端把同样的投递分组。协调任务和读取者运行在连接 Broker 的那个运行时上。

已连接的 Broker 和每个发布者共用一个连接：SDK 客户端、租约存储、租约所有者 id 和一个关闭标志。SDK 在需要连接的那个请求所在的运行时上打开 HTTP 连接。因此每个运行时都通过自己的客户端发送：Broker 的运行时使用 connect 时构建的客户端，拥有自己运行时的处理器线程在第一次请求时构建一个，使用相同的配置和凭证缓存。这样，繁忙的线程绝不会拖住 Broker 的连接。

[`KinesisPublish`](api:ruststream_kinesis::KinesisPublish) 策略与连接配对，构造出 [`KinesisPublisher`](api:ruststream_kinesis::KinesisPublisher)，后者用一次 `PutRecord` 写入每条记录。它接收应答编码所在的缓冲区，不带消息头的记录就是原样的这个缓冲区。重试副本发回订阅读取的流，[结算](settlement.md)一页用图说明了这一点。

## 进程内传输 {/* #in-process-transport */}

在 `testing` 特性下，测试改为在进程内连接 Broker。这时已连接的 Broker 及其订阅、发布者和投递，都针对一个存在于测试中的日志工作。不带该特性的构建不会留下它的任何痕迹：编译器检查连接、投递流、seek 句柄和结算的大小与它们的线上形式完全相同。

这个日志写入记录的方式与发布者为 Kinesis 写入的方式相同，使用相同的分区键和相同的信封，并用相同的解码读回记录。Kinesis 拒绝的内容，它也拒绝，返回的错误与线上调用报告的相同：错误的流名称、错误的分区键、过大的记录，以及关闭后使用的任何句柄。它模拟位置和 seek、投递的消息头、重试副本和死信流，并对重新入队返回 `AckError::Unsupported`。

有些东西只属于线上服务。进程内的流只有一个分片，因此每条记录都与其他每条记录有序。进程内没有租约、检查点、保留期和重新分片，未处理的记录也不会回来。服务指定的每个流都存在，订阅从最新处打开。

各部分如何启动和停止，见[生命周期](lifecycle.md)；一条记录的路径，见[路径](path.md)。
