# ruststream-kinesis {#ruststream-kinesis}

**`ruststream-kinesis`** запускает сервис [RustStream](https://powersemmi.github.io/ruststream/) на
Amazon Kinesis Data Streams. Поток - это шардированный лог, как в Kafka.

Транспорт реализован поверх официального клиента
[`aws-sdk-kinesis`](https://docs.rs/aws-sdk-kinesis). Над ним подписка находит шарды потока при их
разделениях и слияниях, берёт на каждый шард аренду с ограждением и пишет контрольные точки
продвижения по каждому шарду.

Фича `testing` даёт брокер внутри процесса.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_service.rs:app"
```

## Куда идти дальше {#where-to-go-next}

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[Руководство по Kinesis](kinesis.md)** - подписки, аренда и контрольные точки, позиции, публикация и тестирование.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: подписчики, маршрутизация, кодеки, middleware, CLI.
- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-kinesis)** - rustdoc крейта на docs.rs.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт описывает только брокер Kinesis. Всё, что работает одинаково на любом брокере, лежит в
[документации RustStream](https://powersemmi.github.io/ruststream/).
