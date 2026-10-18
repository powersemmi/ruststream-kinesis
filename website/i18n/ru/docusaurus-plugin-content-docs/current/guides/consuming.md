---
title: Чтение потока
sidebar_position: 2
description: "Чтение потока Kinesis по одной записи или пакетами, лимит чтений сегмента через poll_interval и размер пакета и то, что содержит доставка."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Чтение потока

Это руководство читает поток по одной записи и пакетами, укладывается в лимит чтений каждого сегмента и показывает, что содержит доставка.

Пример - сервис аналитики.
Он проводит платежи по мере поступления и считает просмотры страниц пакетами.

## Как читается поток {/* #how-a-stream-is-read */}

Подписка запускает по читателю на каждый сегмент, который она держит.
Читатель вызывает `GetRecords` с итератором сегмента, передаёт записи по порядку и сразу запрашивает снова.
После чтения без записей он ждёт [`poll_interval`](../components/subscriptions/poll-interval.md), по умолчанию одну секунду, и только потом запрашивает снова.
Между читателями и обработчиком ждут не больше 64 доставок. Читатель, который застал очередь полной, ждёт, поэтому медленный обработчик замедляет чтение.

Kinesis разрешает каждому сегменту [пять вызовов `GetRecords` в секунду и 2 МБ в секунду](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html).
Этот лимит делят все потребители потока.
Если сервис читает слишком часто, Kinesis ограничивает частоту его вызовов и отвечает `ProvisionedThroughputExceededException`.
Сначала AWS SDK сам повторяет ограниченный вызов.
Когда повторы исчерпаны, читатель ждёт один `poll_interval` и запрашивает снова с тем же итератором.
Обработчик видит паузу, без ошибки и без повторной доставки.

## По одной записи {/* #one-record-at-a-time */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#payments"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#payments"
```

</TabItem>
</Tabs>

`book` читает поток `payments` и завершает каждый платёж отдельно.
Подписка на одиночные записи запрашивает до 1000 записей за одно чтение.
`poll_interval(Duration::from_millis(250))` сокращает паузу после пустого чтения, поэтому новый платёж ждёт не больше четверти секунды.
Четыре пустых чтения в секунду оставляют другим потребителям одно чтение в секунду из лимита сегмента.
Эту настройку задаёт и [шаг точки монтирования](../components/subscriptions/subscriber-ext.md) `.poll_interval(..)` после `b.include(..)`.

Через [`Ctx<Position>`](../components/context/index.md) обработчик получает место записи в потоке: её сегмент и порядковый номер в виде [`KinesisPosition`](../components/positions/index.md).
Перемотка на эту позицию читает сегмент заново с этой записи ([Повторное чтение и перемотка](replay.md)).
Каждая доставка содержит ещё три [заголовка](../components/context/headers.md):

| Заголовок | Значение |
|---|---|
| `kinesis-shard-id` ([`SHARD_HEADER`](api:ruststream_kinesis::SHARD_HEADER)) | сегмент, из которого прочитана запись |
| `kinesis-sequence-number` ([`SEQUENCE_HEADER`](api:ruststream_kinesis::SEQUENCE_HEADER)) | её порядковый номер в этом сегменте |
| `partition-key` ([`PARTITION_KEY_HEADER`](api:ruststream_kinesis::PARTITION_KEY_HEADER)) | ключ сегмента, с которым её опубликовали |

Обработчик читает их через `ctx.headers()`.
Остальные заголовки записи - те, что записал её производитель.

## Пакеты {/* #batches */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#views"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#views"
```

</TabItem>
</Tabs>

Обработчик, который принимает срез, - [пакетный обработчик](../components/subscriptions/batches.md).
Приложение монтирует `count` с `.batch(nonzero!(500))`, и 500 становится пределом каждого вызова `GetRecords` его читателей.
Поэтому одно чтение никогда не выбирает больше одного пакета, а Kinesis ограничивает чтение 10 000 записей.
Неполный пакет уходит через 50 мс после своей первой записи, поэтому даже малоактивный поток быстро доставляет записи.
Один пакет может смешивать записи всех сегментов, которые держит экземпляр.

Размер пакета - выбор между задержкой и лимитом чтений.
Маленький пакет - это маленькое чтение, а каждое чтение расходует один из пяти вызовов сегмента в секунду.
`count` завершает весь пакет одним исходом.
Обработчик, который возвращает `Vec<HandlerOutcome>`, завершает каждую запись отдельно, и тогда каждая запись двигает контрольную точку своего сегмента ([Контрольные точки и гарантии доставки](checkpoints.md)).

## Приложение {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/consuming.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/consuming.rs#app"
```

</TabItem>
</Tabs>

По умолчанию обработчик выполняет одну доставку за раз.
`workers(n)` в атрибуте или шаг `workers` ручной формы выполняет до `n` доставок одновременно. Руководство [Сегменты, ключи сегмента и порядок](ordering.md) показывает, как при этом сохранить порядок каждого ключа.

Крейт читает с общей пропускной способностью потока, через `GetRecords`.
Запись, которую агрегировала Kinesis Producer Library, не передаётся обработчику: подписка сообщает о ней ошибкой [`KinesisError::AggregatedRecord`](../components/broker/errors.md) и читает дальше.

Тесты в файле примера запускают приложение внутри процесса.
Тест пакетов начинается с остановленными часами, поэтому 50 мс, которые ждёт неполный пакет, проходят мгновенно.
