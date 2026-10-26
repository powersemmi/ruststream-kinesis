---
title: 路径
sidebar_position: 4
description: "一条 Kinesis 记录在 RustStream 服务中的路径：读取、投递、处理器、应答和检查点，逐条进行或按批进行。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一条记录从读取到检查点的路径：左边逐条处理并带应答，右边按批处理",
  "text": "以文字阅读本图",
  "single": "逐条",
  "batches": "按批",
  "receive": ["GetRecords", "最多 1000 条记录"],
  "channel": ["通道", "64 次投递，所有分片"],
  "message": ["KinesisMessage", "消息头、水位线位置"],
  "handler": ["处理器", "解码并运行"],
  "reply": ["应答：PutRecord", "解析出的分区键"],
  "ack": ["ack", "连续时写入检查点"],
  "batchReceive": ["GetRecords", "上限 n，最多 10,000"],
  "batchChannel": ["通道", "64 次投递"],
  "batchMessages": ["一个批", "n 条记录或 50 毫秒"],
  "batchHandler": ["批处理器", "每个批调用一次"],
  "batchAck": ["每个元素", "各自结算"],
  "batch": "记录"
};

# 路径

一条 Kinesis 记录在 RustStream 服务中的路径：读取、投递、处理器、应答和检查点，逐条进行或按批进行。

<Diagram name="path" words={words} />

分片的读取者每次 `GetRecords` 最多请求 1,000 条记录。每条记录都变成一个 [`KinesisMessage`](api:ruststream_kinesis::KinesisMessage)：载荷从 SDK 的应答中复制出来，如果载荷带有[信封](kinesis/index.md)，就从中读出信封。分区键、序列号和分片 id 变成消息头。投递还在分片的水位线中占据自己的位置，水位线决定它的检查点。每条记录有少数几次内存分配，[基准测试](/architecture/benchmarks/)公布一条记录的开销。

订阅的每个读取者都把投递送进同一个容量为 64 的通道。通道满时读取者等待，因此读取的节奏由处理器决定。读取在送出之前打上标记：如果读取还在进行时订阅[重新定位](kinesis/reading.md)，这次读取的记录会在离开通道时被丢弃，永远不会交给处理器。

批订阅把它的批大小作为每次读取的上限，最多为一次读取能返回的 10,000 条。批在容纳了这么多记录时关闭，或在第一条记录到达 50 毫秒后关闭，因此一个批往往小于这个大小。一个批可能包含多个分片的记录。每个元素各自结算，有自己的检查点。

应答编码进分发循环复用的缓冲区。分区键依次取自发布调用、`partition-key` 消息头和策略，谁也没有指定键的记录会得到一个把记录分散到各个分片上的键。用户消息头进入信封，不带消息头的应答就是原样的编码缓冲区，除非它本身以信封开头。[`KinesisPublisher`](api:ruststream_kinesis::KinesisPublisher) 用一次 `PutRecord` 写入它，使用发布所在运行时的 SDK 客户端。

投递在应答之后结算。`ack` 只在推进分片的水位线时才写入检查点，[结算](settlement.md)一页用图说明了这一点。
