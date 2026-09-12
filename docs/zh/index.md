# ruststream-kinesis { #ruststream-kinesis }

**`ruststream-kinesis`** 让 [RustStream](https://powersemmi.github.io/ruststream/) 服务运行在
Amazon Kinesis Data Streams 上。一条流是一个分片的日志，和 Kafka 一样。

传输在官方的 [`aws-sdk-kinesis`](https://docs.rs/aws-sdk-kinesis) 客户端之上实现。在它之上，订阅
跨越分裂和合并发现流的分片，给每个分片取一份带防护的租约，并为每个分片的进度写检查点。

`testing` feature 提供一个进程内 Broker。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_service.rs:app"
```

## 接下来读什么 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[Kinesis 指南](kinesis.md)** - 订阅、租约与检查点、位置、发布和测试。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 框架本身：订阅者、路由、编解码器、中间件、CLI。
- :material-language-rust: **[API 参考](https://docs.rs/ruststream-kinesis)** - 这个 crate 在 docs.rs 上的 rustdoc。

</div>

## 本站点与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本站点只讲 Kinesis Broker。在每个 Broker 上表现一致的部分，都在
[RustStream 文档](https://powersemmi.github.io/ruststream/)里。
