# ruststream-kinesis {#ruststream-kinesis}

**`ruststream-kinesis`** запускает сервис [RustStream](https://powersemmi.github.io/ruststream/) на
Amazon Kinesis Data Streams. Поток - это сегментированный лог с удержанием записей, как в Kafka.

Транспорт реализован поверх официального клиента
[`aws-sdk-kinesis`](https://docs.rs/aws-sdk-kinesis). Над ним подписка находит сегменты потока при
их разделениях и слияниях, берёт на каждый сегмент аренду с ограждением и пишет контрольные точки
продвижения по каждому сегменту. Подтверждение - это контрольная точка, сегмент читает один
экземпляр сервиса за раз, доставка идёт at-least-once.

## Установка {#install}

Три фичи, все выключены по умолчанию: `dynamodb-lease` делит сегменты между экземплярами сервиса,
`testing` даёт внутрипроцессный брокер, а `asyncapi` вписывает собственный словарь этого брокера в
генерируемый документ.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }
```

## Первый сервис {#the-first-service}

Обработчик - это `async fn` над разобранной записью, а `KinesisStream` называет поток, который он
читает. Объект приложения монтирует обработчик на брокер, а атрибут пишет `main`:

```rust
--8<-- "crates/ruststream-kinesis/examples/kinesis_service.rs:app"
```

Запускает сервис `cargo run -- run`. Брокер хранит только настройки: регион и учётные данные
разрешаются, когда рантайм его подключает.

## Что даёт крейт {#what-the-crate-offers}

Rustdoc крейта и есть его руководство, написанное рядом с кодом:

- [Подписка](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#subscribing)
  - дескриптор потока, пауза между чтениями и создание потока для локального стенда.
- [Аренда и контрольные точки](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#leases-and-checkpoints)
  - что пишет подтверждение, какое хранилище `DynamoDB` делят несколько экземпляров сервиса и как
  перейти на новую версию с таблицей аренды, которую вела прежняя.
- [Позиции и перемотка](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#positions-and-seeking)
  - открыть подписку в любом месте, которое поток ещё хранит, и перемотать её из обработчика.
- [Пакеты](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#batches)
  - размер, названный на монтировании пакета, становится пределом чтения для каждого сегмента.
- [Публикация](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#publishing)
  - политика публикации и ключ сегмента, который выбирает сегмент для записи.
- [Тестирование](https://docs.rs/ruststream-kinesis/latest/ruststream_kinesis/index.html#testing)
  - внутрипроцессный транспорт за фичей `testing`.

## Куда идти дальше {#where-to-go-next}

<div class="grid cards" markdown>

- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-kinesis)** - rustdoc крейта на docs.rs, он же его руководство.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - установка, учебник и список брокеров.
- :material-transit-connection-variant: **[Справочник фреймворка](https://docs.rs/ruststream)** - подписчики, маршрутизация, кодеки, middleware, CLI.

</div>

Этот сайт описывает только брокер Kinesis. Всё, что работает одинаково на любом брокере, лежит в
[документации RustStream](https://powersemmi.github.io/ruststream/).
