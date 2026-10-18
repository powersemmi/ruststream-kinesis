---
title: Methodology
description: "How ruststream-kinesis measures itself: the three loops, the run on a Kinesis endpoint, the emulator-bound arithmetic and the code count."
---

# Methodology

How `ruststream-kinesis` measures itself: the three loops, the run on a Kinesis endpoint, the
emulator-bound arithmetic and the code count. The procedure is the framework's own
([RustStream methodology](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)),
and this page states what it is on Kinesis.

## The three loops

One process runs each scenario three times over, as three loops that differ in one thing: what
carries the records.

- **Raw client.** `aws-sdk-kinesis`, driven directly: `GetRecords` on a shard iterator, decode each
  record, checkpoint it, and renew the lease on the crate's schedule.
- **Broker crate.** This crate and nothing above it: the broker connects, the
  [`KinesisStream`](api:ruststream_kinesis::KinesisStream) descriptor opens a subscription, and a
  loop pulls deliveries off its stream, decodes them and acks them.
- **RustStream.** The service a user writes: a `#[subscriber]` handler and the app around it.

Everything else is equal across the three: one `SdkConfig` builds every client, the stream has one
shard, every read asks for 1,000 records, an empty read waits 20 milliseconds, the checkpoint goes
to the same place, the decode goes into the same struct, the body bytes, the tokio runtime with four
worker threads, and the binary. The pause is far below the one-second default on purpose: an
emulator on the loopback has no per-shard read budget, and a second of sleep would measure the
sleep.

The body is 512 bytes of JSON. It decodes into a struct of two integer fields, so the decode
allocates nothing and the number is about this crate rather than about the JSON parser.

## The run

- **A fresh stream per run.** Each run creates its own one-shard stream, and the DynamoDB scenario
  its own lease table, so a run never sees what an earlier one left.
- **The consumer attaches first**, at the trim horizon. Sixteen tasks then fill the stream, one
  `PutRecord` per record, and stay at most 50,000 records ahead of the consumer.
- **The window opens at the first delivery and closes when the last record's field is read**, in
  all three loops. Creating the stream, taking the lease and opening the iterator are outside it.
- **The record count comes from a probe**, which measures the raw loop's rate, so a measured run
  lasts at least five seconds on any machine.
- **The loops are interleaved**: raw, crate, RustStream, three rounds. Each loop reports its best,
  median and worst round.

A row is `emulator-bound` when round trips to the endpoint account for at least half of the time a
delivery took. That is arithmetic. A probe times the smallest request the endpoint answers,
`DescribeStreamSummary`, on a client of its own, before any record is published. A delivery costs
its share of the `GetRecords` call that carried it, and on the DynamoDB scenario also the
conditional write that settles it. The document publishes that share per row as
`round_trips_per_message`. The row is flagged when the round trip times that share reaches half the
measured time per record. The round trip is published with the machine, so the arithmetic can be
checked.

## The code count

Each scenario of the [cost chart](/architecture/benchmarks/) is the service a user writes, on
[`KinesisBroker`](api:ruststream_kinesis::KinesisBroker) against the same endpoint, built without
the `testing` feature and started on a single-threaded runtime. It reads a one-shard stream created
for the run from the trim horizon, and checkpoints through the default lease store. Another thread
fills the stream before the measured region opens, and that thread is not counted.

Instructions come from callgrind and allocations from DHAT. A scenario runs over one delivery, then
1000, then 2000. The difference between the last two is the steady cost of a record, and the single
delivery is the cold start. A read is shared by the records it returns, so each row carries its
share of one: up to a thousand records per read on the first two rows, one batch of 64 on the third.

The count covers everything the service's thread runs: the runtime, this crate and the AWS SDK with
its HTTP stack. Threads the SDK runs on its own are not counted, and neither is the endpoint's side
of a round trip. Between runs of one build the instruction counts move by less than half a percent,
because the service does real I/O: a drain can end with a read in flight, and lease renewals and
shard listings run on timers. A run against a saved baseline fails on two percent more instructions
in a scenario. Allocations are held to a floor each scenario declares, above the highest count
observed, on every run.

## Running it yourself

```bash
just bench
```

The recipe starts the LocalStack stand from `docker-compose.test.yml`, runs both comparison
scenarios, stops the stand and rewrites `docs/benchmarks/results.json`. It takes a few minutes and
wants the machine to itself. It clears `RUSTFLAGS`, so the binary is not tied to the CPU it was
built on. Both recipes build the benchmarks the way a release is built, with fat link-time
optimization across crates, so the numbers describe the code a service ships.

```bash
just bench-code
```

The code count takes a few minutes and needs valgrind besides the stand. `just bench-code 5000`
measures every scenario over five thousand deliveries instead of a thousand.
`just bench-code --save-baseline=main` on `main` records a baseline, and
`just bench-code --baseline=main` on a branch compares with it.
