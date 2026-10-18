---
title: Работа в промышленной среде
sidebar_position: 9
description: "Сервис на Kinesis в AWS: учётные данные и регион из окружения или из своей конфигурации, потоки от инфраструктуры и права IAM."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Работа в промышленной среде

Это руководство запускает сервис на Kinesis в AWS: откуда берутся его учётные данные и регион, какие потоки он ожидает найти и какие права IAM ему нужны.

## Учётные данные и регион из окружения {/* #credentials-and-region-from-the-environment */}

`KinesisBroker::new()` ничего не берёт из кода.
[Цепочка по умолчанию](../components/broker/credentials.md) AWS SDK находит регион и учётные данные при запуске сервиса: в переменных окружения, в общих файлах `~/.aws`, через SSO и в роли контейнера или инстанса.
Сервису, который работает под ролью IAM в ECS, EKS или EC2, никакая настройка не нужна.
Брокер строит свою конфигурацию с самой новой версией поведения SDK.

SDK читает учётные данные, когда подписывает свой первый запрос.
Этот запрос - проверка потока первой подписки при запуске, поэтому сервис с неверными учётными данными останавливается раньше, чем обработает хотя бы одну запись.

## Собственная конфигурация {/* #a-configuration-of-your-own */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/production.rs#config"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#config"
```

</TabItem>
</Tabs>

[`KinesisBroker::from_config`](api:ruststream_kinesis::KinesisBroker::from_config) принимает `SdkConfig`, который строит сервис.
`aws_config` строит такую конфигурацию из окружения асинхронным загрузчиком, а функция приложения синхронная.
Поэтому пример строит её через `SdkConfig::builder()`, который не выполняет ввода-вывода.
Поставщик профилей читает профиль `orders-service` при первом запросе. Профиль с `role_arn` принимает эту роль.
Закреплённая версия поведения не даёт умолчаниям SDK измениться при обновлении SDK.
Брокер сохраняет все настройки конфигурации, кроме [`endpoint` и `region`](../components/broker/endpoint.md), заданных на самом брокере.
Они заменяют собственные значения конфигурации.
[`DynamoLeaseStore`](../components/leases/dynamo-lease-store.md) принимает ту же конфигурацию ([Несколько экземпляров](leases.md)).
Примеру нужны ещё две зависимости:

```toml title="Cargo.toml"
[dependencies]
aws-config = "1"
aws-sdk-kinesis = "1"
```

## Сервис {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

Поток `orders` принадлежит инфраструктуре: его создают Terraform, CloudFormation или консоль, вместе с режимом ёмкости, сроком хранения и шифрованием.
Поток называется по имени, а аккаунт и регион берутся из конфигурации.
Дескриптор обходится без [`create_if_missing`](../components/subscriptions/create-if-missing.md), поэтому при отсутствии потока сервис останавливается при запуске с ошибкой, которая называет этот поток, а не создаёт пустой.
`create_if_missing(n)` создаёт поток в режиме выделенной ёмкости с `n` сегментами, что подходит для локального стенда. Режим промышленного потока, выделенная ёмкость или режим по запросу, выбирает тот, кто эксплуатирует сервис.

Запросы идут через HTTPS-клиент SDK по умолчанию, и SDK повторяет неудачный запрос по своей политике повторов.
Обработчик на выделенном потоке выполнения (`threads(n)`) отправляет записи через собственный клиент SDK, построенный из той же конфигурации и с теми же учётными данными.

## Права IAM {/* #iam-permissions */}

Сервису выше нужны такие права на его поток:

```json title="policy.json"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:DescribeStreamSummary",
        "kinesis:ListShards",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords"
      ],
      "Resource": "arn:aws:kinesis:eu-west-1:123456789012:stream/orders"
    }
  ]
}
```

Каждая возможность добавляет свои:

| Возможность | Права |
|---|---|
| публикация, ответ или копия для повтора | `kinesis:PutRecord` на поток, в который она уходит |
| `create_if_missing` | `kinesis:CreateStream` и `kinesis:DescribeStream` |
| `DynamoLeaseStore` | `dynamodb:GetItem` и `dynamodb:UpdateItem` на таблицу аренд |

## Что показывает документ AsyncAPI {/* #what-the-asyncapi-document-shows */}

С фичей `asyncapi` документ AsyncAPI сервиса называет один [сервер](../components/asyncapi/server.md): хост конечной точки брокера, а без неё региональный хост его региона, например `kinesis.eu-west-1.amazonaws.com`.
Регион, который SDK берёт из окружения, становится известен только при подключении, уже после сборки документа, поэтому такой брокер называет `kinesis.amazonaws.com`.
Документ никогда не содержит учётных данных, поэтому его можно публиковать.

Тесты в файле примера запускают то же приложение внутри процесса. Kinesis внутри процесса ничего не подписывает и не читает профилей.
Работу с `TestApp` описывает руководство [Тестирование сервиса на Kinesis](testing.md).
