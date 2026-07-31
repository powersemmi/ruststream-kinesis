<h1 align="center">ruststream-kinesis</h1>

<p align="center">
  <i>The Amazon Kinesis Data Streams broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: a sharded, retained log with leases, checkpoints, and replay.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-kinesis/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-kinesis/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/MSRV-1.85-blue.svg" alt="MSRV 1.85">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

---

`ruststream-kinesis` will implement the [RustStream](https://github.com/powersemmi/ruststream) broker contract over the official [`aws-sdk-kinesis`](https://crates.io/crates/aws-sdk-kinesis). Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Status

**Not implemented yet.** This repository is a scaffold: the workspace, CI, and release plumbing are in place, and the crate is an empty stub. The implementation will target the `ruststream` 0.6 line; the design and scope are tracked in [powersemmi/ruststream#194](https://github.com/powersemmi/ruststream/issues/194).

## Planned surface

- Shard leasing, renewal, and reassignment across consumer instances, so a service scales past one replica without duplicate processing.
- Acknowledgement as per-shard checkpointing with the tracked-watermark model already used for partitioned logs in the project.
- Shard splits and merges handled as the shard set changes over time.
- Shared polling and enhanced fan-out as explicit consumer modes; start positions: latest, horizon, timestamp, sequence.
- Local-stack endpoint support for development and tests.

The broker contract (lazy startup, the typed connect/shutdown lifecycle, and the optional capability traits) is defined by [`ruststream`](https://crates.io/crates/ruststream) and verified by `ruststream::conformance`, with the suite run against a real broker before release.

## Contributing

```bash
just check   # fmt, clippy, feature checks
just test    # tests
just ci      # the full local gate
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
