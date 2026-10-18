---
title: Начало работы
sidebar_position: 1
description: "Установка ruststream-kinesis, локальный Kinesis, первый подписчик на нём и его тест без сервера."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Начало работы

Это руководство устанавливает `ruststream-kinesis`, запускает локальный Kinesis и первый сервис на нём.

## Установка {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-kinesis = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-kinesis = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` - это сам фреймворк. Фича `macros` даёт атрибуты, `json` - кодек по умолчанию, а `logging` выводит журнал сервиса в терминал.
`ruststream-kinesis` - брокер Kinesis, построенный на официальном клиенте `aws-sdk-kinesis`.
Его фича `testing` добавляет [Kinesis внутри процесса](../components/testing/index.md), на котором работают тесты.

У крейта брокера такие фичи, все выключены по умолчанию:

| Фича | Что она даёт |
|---|---|
| `dynamodb-lease` | [`DynamoLeaseStore`](../components/leases/dynamo-lease-store.md), чтобы несколько экземпляров сервиса делили сегменты ([Несколько экземпляров](leases.md)) |
| `asyncapi` | сведения Kinesis о каждом канале в генерируемом документе [AsyncAPI](../components/asyncapi/index.md) |
| `testing` | Kinesis внутри процесса, на котором работает `TestApp` |

## Локальный Kinesis {/* #a-local-kinesis */}

[LocalStack](https://github.com/localstack/localstack) запускает Kinesis в одном контейнере:

```yaml title="compose.yaml"
services:
  localstack:
    image: localstack/localstack:4.14.0
    ports:
      - "4566:4566"
    environment:
      SERVICES: kinesis
      KINESIS_LATENCY: 0
```

```bash
docker compose up -d
```

Стенд отвечает на `http://localhost:4566` и запускается без потоков.
`KINESIS_LATENCY: 0` отключает задержку, которую LocalStack по умолчанию добавляет к каждому вызову Kinesis.

## Первый сервис {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` читает заказы в JSON из потока `orders`.
Заказ с суммой он подтверждает, а заказ без суммы отбрасывает.
[`KinesisStream`](../components/subscriptions/kinesis-stream.md) называет поток, а [`create_if_missing(1)`](../components/subscriptions/create-if-missing.md) создаёт его с одним сегментом, когда открывается подписка.
Промышленный сервис обходится без этого шага и получает потоки от своей инфраструктуры.
[`KinesisBroker::new`](../components/broker/index.md) только запоминает конфигурацию.
[`endpoint`](../components/broker/endpoint.md), [`region`](api:ruststream_kinesis::KinesisBroker::region) и [`test_credentials`](api:ruststream_kinesis::KinesisBroker::test_credentials) направляют его на локальный стенд.
Клиент SDK создаётся при запуске сервиса.
`#[ruststream::app]` превращает `app` в `main`. В ручной форме то же делает [`run_main`](api:ruststream::runtime::cli::run_main).

Запустите сервис:

```bash
cargo run -- run
```

Подписка открывается с конца потока, поэтому читает то, что опубликовано после запуска сервиса.
Опубликуйте заказ из другого терминала через AWS CLI:

```bash
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
aws kinesis put-record --stream-name orders --partition-key customer-7 \
  --cli-binary-format raw-in-base64-out --data '{"id":1,"amount_cents":1250}'
```

Сервис пишет в журнал `order taken` с номером заказа.
<kbd>Ctrl</kbd>+<kbd>C</kbd> останавливает его.

## Первый тест {/* #the-first-test */}

В файле примера есть и два теста.
Каждый запускает то же `app` через `TestApp::start`, публикует заказ и проверяет, как `take_order` завершил его доставку.
Там `KinesisBroker` подключается к Kinesis внутри процесса, поэтому тестам не нужен стенд:

```bash
cargo test
```

Руководство [Тестирование сервиса на Kinesis](testing.md) разбирает эти тесты и то, что моделирует Kinesis внутри процесса.

## Переход с aws-sdk-kinesis {/* #coming-from-aws-sdk-kinesis */}

Rust SDK читает поток по одному сегменту и по одному вызову, и сервис на нём сам пишет потребителя вокруг этих вызовов.
Вот куда переходит каждая часть в RustStream:

| aws-sdk-kinesis | RustStream |
|---|---|
| `aws_config::load_from_env` и `Client::new` | [`KinesisBroker::new`](api:ruststream_kinesis::KinesisBroker::new) или [`from_config`](../components/broker/credentials.md) с собственным `SdkConfig` ([Работа в промышленной среде](production.md)) |
| `ListShards` и `GetShardIterator` для каждого сегмента | имя потока в [`KinesisStream::new`](../components/subscriptions/kinesis-stream.md); подписка сама находит [сегменты](../components/subscriptions/shards.md) и следит за разделениями и слияниями |
| цикл `GetRecords` для каждого сегмента | обработчик `#[subscriber]` ([Чтение потока](consuming.md)) |
| декодирование `record.data()` | тип параметра обработчика, который декодирует кодек |
| контрольная точка, записанная после работы | исход обработчика ([Контрольные точки и гарантии доставки](checkpoints.md)) |
| таблица аренд, чтобы несколько экземпляров делили сегменты | [`DynamoLeaseStore`](../components/leases/dynamo-lease-store.md) ([Несколько экземпляров](leases.md)) |
| `GetShardIterator` с `TRIM_HORIZON` или `AT_TIMESTAMP` | [`start_at`](../components/positions/start-at.md) в точке монтирования или дескриптор перемотки в обработчике ([Повторное чтение и перемотка](replay.md)) |
| `PutRecord` с ключом сегмента | ответ или параметр `Out`, отправленный через [`KinesisPublish`](../components/publishing/index.md) ([Сегменты, ключи сегмента и порядок](ordering.md)) |
