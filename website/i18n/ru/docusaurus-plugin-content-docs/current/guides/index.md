---
slug: /
sidebar_position: 0
description: "Руководства по сервисам на Amazon Kinesis Data Streams с ruststream-kinesis, брокером Kinesis для RustStream."
---

# Руководства

Крейт `ruststream-kinesis` реализует брокер Amazon Kinesis Data Streams для
[RustStream](https://powersemmi.github.io/ruststream/latest/ru/). Он построен на официальном
клиенте [`aws-sdk-kinesis`](https://docs.rs/aws-sdk-kinesis). Сервис читает потоки Kinesis и пишет
в них. Крейт сам находит сегменты потока, отдаёт каждый сегмент в аренду одному экземпляру сервиса и
сохраняет контрольные точки его чтения. Руководства проводят сервис на Kinesis от установки до
промышленной эксплуатации.
