# ruststream-kinesis { #ruststream-kinesis }

**`ruststream-kinesis`** 让 [RustStream](https://powersemmi.github.io/ruststream/) 服务运行在
Amazon Kinesis Data Streams 上。一条流是一个分片的、带保留期的日志，和 Kafka 一样。

传输在官方的 [`aws-sdk-kinesis`](https://docs.rs/aws-sdk-kinesis) 客户端之上实现。在它之上，订阅
跨越分裂和合并发现流的分片，给每个分片取一份带防护的租约，并为每个分片的进度写检查点。确认就是
一次检查点，一个分片同时只由一个服务实例读取，投递是至少一次的。

## 安装 { #install }

三个 feature，默认都关闭：`dynamodb-lease` 让多个服务实例共享分片，`testing` 让 `KinesisBroker`
在框架的 `TestApp` 下于进程内运行，`asyncapi` 把这个 Broker 自己的词汇写进生成的文档。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }
```

## 第一个服务 { #the-first-service }

处理器是一个接收已解码记录的 `async fn`，`KinesisStream` 说出它读取的流。应用对象把处理器挂载到
Broker 上，属性宏写出 `main`：

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_service.rs:app"
```

`cargo run -- run` 启动它。Broker 只记录配置：区域和凭证在运行时连接它的时候才解析。

## 这个 crate 提供什么 { #what-the-crate-offers }

crate 的 rustdoc 就是它的指南，写在它所描述的代码旁边：

- [订阅](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#subscribing)
  - 流的描述符、两次读取之间的停顿，以及为本地环境创建流。
- [租约与检查点](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#leases-and-checkpoints)
  - 一次确认写下什么，多个服务实例共享的 `DynamoDB` 存储，以及升级时如何沿用旧版本写下的租约表。
- [位置与定位](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#positions-and-seeking)
  - 在流仍然保留的任何地方打开订阅，并从处理器里重新定位它。
- [批](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#batches)
  - 挂载批时说出的大小，成为每个分片读取器请求的读取上限。
- [发布](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#publishing)
  - 发布策略，以及为记录挑选分片的分区键。
- [测试](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#testing)
  - `TestApp` 测试套件下的生产应用，在进程内运行，或对着真实的流。

## 接下来读什么 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-language-rust: **[API 参考](https://docs.rs/ruststream-kinesis)** - 这个 crate 在 docs.rs 上的 rustdoc，也是它的指南。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 安装、教程和 Broker 列表。
- :material-transit-connection-variant: **[框架参考](https://docs.rs/ruststream)** - 订阅者、路由、编解码器、中间件、CLI。

</div>

本站点只讲 Kinesis Broker。在每个 Broker 上表现一致的部分，都在
[RustStream 文档](https://powersemmi.github.io/ruststream/)里。
