---
title: Тестирование сервиса на Kinesis
sidebar_position: 8
description: "Тестирование сервиса на Kinesis внутри процесса с TestApp: рабочее приложение, записи с ключом, ответы, задержки и тот же тест на стенде."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Тестирование сервиса на Kinesis

Тест запускает собственное приложение сервиса через [`TestApp`](api:ruststream::testing::TestApp) на [Kinesis внутри процесса](../components/testing/index.md) или на стенде.

Тестируемый сервис списывает платежи по картам.
Платёж приходит в поток `card-payments`, и его ключ сегмента - счёт для списания.
Обработчик отвечает квитанцией в поток `card-receipts`.
Платёж без суммы отбрасывается.
Платёж сверх мгновенного лимита банка откладывается: обработчик просит доставить его снова через минуту с [`retry_after`](../components/settlement/retry-after.md).
После трёх доставок платёж уходит в `card-payments.review`, [поток мёртвых писем](../components/settlement/dead-letters.md).
Приложение строит `service(broker)`, поэтому тест выбирает брокер сам, а `main` берёт рабочий.
Тестам нужна фича `testing` крейта `ruststream-kinesis` в `[dev-dependencies]`.

## Платёж с ключом {/* #a-keyed-payment */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) подключает [`KinesisBroker`](api:ruststream_kinesis::KinesisBroker) к Kinesis внутри процесса.
Тест не обращается ни к одной конечной точке AWS, и учётные данные ему не нужны.
Тест обращается к брокеру по его рабочему типу: `tb.broker::<KinesisBroker>()`.
Ключ сегмента записи передаётся заголовком `partition-key` ([`PARTITION_KEY_HEADER`](api:ruststream_kinesis::PARTITION_KEY_HEADER)), поэтому тест задаёт его через `with_headers`.
Брокер превращает этот заголовок в ключ сегмента записи, а доставка возвращает его в том же заголовке.
`publish` возвращает управление, когда обработчик и его ответ завершены.
Поэтому следующие строки проверяют уже готовое состояние: как подписчик завершил доставку и что дошло до `card-receipts`.
Тело теста принимает запущенное приложение аргументом, поэтому его же выполняет и тест на стенде ниже.

## Платёж без суммы {/* #a-payment-without-an-amount */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` проверяет исход, который вернул обработчик.
`assert_not_called` проверяет, что квитанция не опубликована.

## Задержка без ожидания {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

Тест начинается с остановленными часами, а [`advance`](api:ruststream::testing::TestApp::advance) сдвигает их вперёд.
Минута `HOLD` не стоит ничего.
Kinesis не умеет придерживать запись, поэтому по окончании задержки `retry_after` публикует копию записи в её поток.
Копия содержит в заголовках число попыток и ключ сегмента записи, и `published` показывает её в `card-payments` рядом с собственной записью теста.
Третья доставка - последняя, которую допускает точка монтирования, поэтому запись уходит в `card-payments.review` в том виде, в каком пришла.
Каждая отложенная доставка записывается как `Outcome::Nack`.

## Что моделирует Kinesis внутри процесса {/* #what-the-in-process-kinesis-models */}

Kinesis внутри процесса записывает и читает записи так же, как это делает для брокера сам сервис Kinesis.
Действуют те же правила ключа сегмента, пользовательские заголовки передаются в том же конверте, а доставка содержит те же заголовки и тот же контекст, что и доставка из настоящего сегмента:

- Брокер читает поток одной подпиской: она читает каждую его запись и открывается с конца. Вторая подписка на тот же поток отклоняется, как её отклоняет брокер сервиса.
- [`start_at`](../components/positions/start-at.md) и дескриптор перемотки обработчика читают журнал потока заново с позиции, которую они называют ([Повторное чтение и перемотка](replay.md)).
- На `retry` без предела Kinesis внутри процесса отвечает так же, как Kinesis: не поддерживается, и запись не возвращается.
- Отложенный повтор возвращается копией со счётчиком попыток, а запись, исчерпавшая попытки, уходит в поток мёртвых писем.
- Запись, которую отвергает сервис Kinesis, отвергается и здесь: имя потока длиннее 128 символов или вне `[a-zA-Z0-9_.-]`, пустой ключ сегмента или ключ длиннее 256 символов, запись больше 10 МиБ, наибольшего размера, который можно настроить для потока. Тест, который публикует такую запись, получает ошибку сервиса.
- Издатель или дескриптор перемотки, использованный после `shutdown`, возвращает ошибку [`KinesisError::NotConnected`](../components/broker/shutdown.md).

У каждого потока там один сегмент, поэтому каждая запись упорядочена относительно любой другой, и каждый поток, который называет сервис, существует.
Для нескольких сегментов, аренд и контрольных точек, срока хранения и изменения числа сегментов нужен стенд.

## Тот же тест на стенде {/* #the-same-test-on-a-stack */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) подключает то же приложение к работающему Kinesis, здесь к LocalStack.
Публикация ждёт завершения реакции до десяти секунд. [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) задаёт более долгий срок.
Тест читает адрес из `KINESIS_TEST_ENDPOINT` через `support::endpoint`, вспомогательную функцию примеров, и пропускается, если переменная не задана.
Сервис создаёт `card-payments`, когда открывается его подписка. Тест создаёт `card-receipts`, в который только пишут.
Подписка открывается с конца потока, поэтому записи, которые оставил в потоке прошлый прогон, не читаются.
Проверки те же, что и внутри процесса, без изменений.

```bash
cargo test
KINESIS_TEST_ENDPOINT=http://localhost:4566 cargo test -- --test-threads=1
```
