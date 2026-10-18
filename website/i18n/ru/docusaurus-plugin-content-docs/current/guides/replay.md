---
title: Повторное чтение и перемотка
sidebar_position: 6
description: "Повторное чтение потока Kinesis с самой старой записи или с заданного момента и перемотка работающей подписки из её обработчика."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Повторное чтение и перемотка

Это руководство читает поток заново с самой старой записи или с заданного момента и перематывает работающую подписку из её обработчика.

Kinesis хранит запись и после того, как потребитель её прочитал, пока не истечёт срок хранения потока.
Сервис может прочитать её снова: чтобы перестроить модель чтения, заполнить данными нового потребителя или пропустить записи, которые ему не годятся.
Подписка получает начальную позицию в точке монтирования, а обработчик перематывает свою подписку через контекст доставки.

## Модели чтения {/* #the-read-models */}

Сервис держит в памяти две модели чтения: баланс каждого счёта и расходы по каждой карте.
Каждая проводка содержит ревизию реестра.
Исправление прошлых проводок публикуется новой ревизией, и модель чтения, которая её встречает, перестраивается с самой старой записи потока.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/replay.rs#projection"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#projection"
```

</TabItem>
</Tabs>

## Чтение с самой старой записи {/* #start-from-the-oldest-record */}

Балансы хранятся в памяти, поэтому после перезапуска они строятся заново по всему потоку.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/replay.rs#balance"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#balance"
```

</TabItem>
</Tabs>

[`start_at(KinesisPosition::horizon())`](../components/positions/start-at.md) открывает каждый сегмент с `TRIM_HORIZON`, самой старой записи, которую поток ещё хранит.
Эта позиция действует при каждом запуске, на сегменты, с которыми открывается подписка, и имеет приоритет над любой сохранённой [контрольной точкой](checkpoints.md).
Без `start_at` сегмент продолжает после своей контрольной точки, а если её нет, открывается с конца.

У [`KinesisPosition`](../components/positions/index.md) четыре позиции:

| Позиция | Откуда читает подписка |
|---|---|
| [`horizon()`](api:ruststream_kinesis::KinesisPosition::horizon) | с самой старой записи каждого сегмента |
| [`latest()`](api:ruststream_kinesis::KinesisPosition::latest) | с конца каждого сегмента, поэтому читаются только новые записи |
| [`timestamp(ms)`](api:ruststream_kinesis::KinesisPosition::timestamp) | с первой записи каждого сегмента, поступившей в момент `ms` или позже |
| [`sequence(shard, seq)`](api:ruststream_kinesis::KinesisPosition::sequence) | с одной записи одного сегмента и записей после неё |

По умолчанию поток хранит записи 24 часа, а с [продлённым сроком хранения](https://docs.aws.amazon.com/streams/latest/dev/kinesis-extended-retention.html) - до 365 дней.
Записи старше срока хранения удаляются, и `TRIM_HORIZON` сдвигается вперёд вслед за ними.

## Перемотка из обработчика {/* #seek-from-a-handler */}

Тот же обработчик перестраивает балансы, когда приходит проводка более новой ревизии.
[`Ctx<SeekHandle>`](../components/positions/seek.md) даёт обработчику [`KinesisSeeker`](api:ruststream_kinesis::KinesisSeeker) его собственной подписки.
[`seek`](api:ruststream::Seeker::seek) перематывает подписку на самую старую запись, и обработчик читает все проводки заново.
Проводка, с которой началась перестройка, тоже возвращается, и на этот раз проекция её применяет.

Перемотка принимает любую из позиций выше.
Позиция для всего потока перематывает каждый сегмент, который держит этот экземпляр.
`Ctx<Position>` хранит собственную позицию доставки, позицию `sequence`: перемотка на неё читает этот сегмент заново с этой записи и не трогает остальные сегменты.
Для неё этот экземпляр должен держать сегмент. Перемотка на сегмент, который читает другой экземпляр или который уже прочитан до конца, возвращает ошибку.

## Перемотка из пакетного обработчика {/* #seek-from-a-batch-handler */}

Проводки по картам идут большим объёмом, поэтому расходы строятся [пакетами](../components/subscriptions/batches.md) до 500 записей.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/replay.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#batch"
```

</TabItem>
</Tabs>

Пакетный обработчик объявляет [`KinesisBatchContext`](../components/context/batch-context.md) вместо контекста одной доставки.
В нём тот же ключ `SeekHandle` и нет позиции, потому что пакет охватывает много записей.
Тело, которому нужно место одной записи, читает её заголовки `kinesis-shard-id` и `kinesis-sequence-number`.

## Чтение с заданного момента {/* #start-from-a-moment */}

Проверка выплат при каждом запуске читает выплаты за последние сутки.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/replay.rs#payouts"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#payouts"
```

</TabItem>
</Tabs>

Момент известен только при запуске сервиса, поэтому позицию задаёт точка монтирования, а не атрибут.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/replay.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#service"
```

</TabItem>
</Tabs>

`KinesisPosition::timestamp` принимает миллисекунды от начала эпохи.
Kinesis сравнивает это значение с меткой времени поступления, которую он ставит на каждую запись.
Эта метка приблизительная, поэтому записи, поступившие примерно в один момент, точно не различаются.

## Перемотка и контрольные точки {/* #seeks-and-checkpoints */}

Перемотка сбрасывает учёт контрольных точек каждого сегмента, который она переносит.
Доставка, прочитанная до перемотки, завершается впустую, поэтому ack проводки, с которой началась перестройка, не двигает контрольную точку.
Так следующая контрольная точка никогда не обгоняет запись, которую перемотка прочитала заново и которую ещё не обработал ни один обработчик.
Перемотка двигает только этот экземпляр. Остальные экземпляры продолжают читать свои сегменты, а экземпляр, который забирает сегмент, продолжает с его контрольной точки.

Тесты в файле примера запускают приложение внутри процесса.
[Kinesis внутри процесса](../components/testing/index.md) хранит весь журнал каждого потока, поэтому перемотка и там читает его заново.
