---
title: 租约
sidebar_position: 3
description: "服务的各个实例通过租约表共享一个流的分片：每个分片一行，记录它的所有者、到期时间和检查点。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "两个实例通过租约表共享一个流的分片，一个实例的租约失效时，另一个实例接管它的分片",
  "text": "以文字阅读本图",
  "a": ["实例 A", "持有分片 0"],
  "table": "租约表",
  "row0": ["orders:shardId-000000000000", "所有者 A、到期时间、计数器、检查点"],
  "row1": ["orders:shardId-000000000001", "所有者 B、到期时间、计数器、检查点"],
  "b": ["实例 B", "持有分片 1"],
  "takeover": ["A 停止后，它的租约在 10 秒后失效。B 用条件写入", "接管这一行，并从 A 写入的检查点之后", "继续读取"],
  "renews": "每 3 秒续约"
};

# 租约

服务的各个实例通过租约表共享一个流的分片：每个分片一行，记录它的所有者、到期时间和检查点。

<Diagram name="leases" words={words} />

Kinesis 不记录谁在读取某个分片，也不记录读到了哪里。Kinesis Client Library 把这些信息保存在一张 DynamoDB 表中，每个分片一行，它的工作进程会接管无人续约的行。本 crate 通过 [`LeaseStore`](api:ruststream_kinesis::LeaseStore) 做同样的事。存储可以为某个所有者获取分片、续约、记录检查点、读取一行以及释放它。每一行都以流和分片共同作为键，因为分片 id 在不同的流之间会重复。

租约持续 10 秒，它的读取者每 3 秒续约一次。续约失败的读取者已经把分片让给了另一个所有者，它立即停止，不写入检查点。新的所有者从最后一个检查点开始读取，因此少数记录可能被投递两次。投递是至少一次的。

[`DynamoLeaseStore`](api:ruststream_kinesis::DynamoLeaseStore) 由 `dynamodb-lease` 特性启用，它把这些行保存在一张 DynamoDB 表中。该表只有一个字符串键 `lease_key`，写作 `stream:shard`。一行包含 `lease_owner`、`lease_expiry`、`lease_counter` 和 `checkpoint`。每次写入都是条件写入。当这一行没有所有者、所有者就是自己，或者租约已经过期时，获取成功。续约、检查点和释放只在本所有者持有这一行时成功。读取是强一致的。每个 Broker 都有自己的租约所有者 id，默认唯一，因此两个实例绝不会同时持有一个分片。

[`MemoryLeaseStore`](api:ruststream_kinesis::MemoryLeaseStore) 是默认的存储。它存在于进程中：对单个实例是正确的，重启后又变为空。于是重启后的服务没有检查点，每个分片都从最新处打开，除非 `start_at(..)` 指定了另一个位置。

存储属于一个 Broker。一个 Broker 通过一个订阅读取一个流，对同一个流的第二个订阅会在启动时被拒绝。两个各有自己租约存储的 Broker，无论位于一个服务中还是两个服务中，都各自读取整个流。共用一张表的实例共享分片。

这一模型就是 Kinesis Client Library 的租约表（[KCL 消费者](https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html)），加上 DynamoDB 的条件写入（[条件表达式](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html)）。
