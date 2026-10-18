---
slug: /
sidebar_position: 0
description: "使用 RustStream 的 Kinesis Broker ruststream-kinesis 构建 Amazon Kinesis Data Streams 服务的指南。"
---

# 指南

`ruststream-kinesis` 是 [RustStream](https://powersemmi.github.io/ruststream/latest/zh/) 的 Amazon Kinesis Data Streams Broker，
基于官方客户端 [`aws-sdk-kinesis`](https://docs.rs/aws-sdk-kinesis) 构建。服务读写 Kinesis 流：
crate 发现流的分片，通过租约把每个分片交给一个服务实例，并为它的读取进度保存检查点。
这些指南带你把一个 Kinesis 服务从安装一路推进到生产环境。
从[快速开始](getting-started.md)读起。
