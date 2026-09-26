# Benchmarks

Reaching a Kinesis record through this crate costs time on every record: the shard reader, the
decode, the settling. This page says how much, measured against the same work written by hand on
`aws-sdk-kinesis`.

Three loops in one process run the same scenario, and what differs between them is what carries a
record. **Raw client** drives `aws-sdk-kinesis` itself: `PutRecord` to publish, `GetRecords` to
read. **This crate** drives this crate's own types - its broker, its subscription, the stream it
yields, its acknowledgement and its publisher - from a loop written in the benchmark, with no
handler and no runtime. **Whole service** is what a user writes: a `#[subscriber]` handler under
the application object.

Everything else is held equal: one `SdkConfig` builds every client, the stream has one shard, all
three read it with the same `GetRecords` limit and the same pause after an empty read, decode into
the same type, write the same checkpoint, and run on the same tokio runtime and the same build. The
procedure is the framework's own and is described on the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

!!! warning "What answers here is an emulator"

    Kinesis has no server you can run, so all three loops talk to the LocalStack image the live
    suites use. The comparison stays valid, because one emulator answers all of them - but its
    saturation profile is not the hosted service's: no TLS handshake, no credential refresh, no
    regional latency. A row here is a statement about what a record costs inside this crate, and
    nothing at all about what a service costs in a region.

## The numbers

The best of three interleaved rounds, with the median round in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "This crate", "framework": "Whole service", "adapterOverhead": "Crate over raw", "overhead": "Service over raw", "indistinguishable": "indistinguishable", "brokerBound": "emulator-bound", "machine": "Machine", "os": "OS", "broker": "Emulator", "roundTrip": "Round trip", "build": "Build", "versions": "Versions", "measured": "Measured", "codeMeasured": "Code costs measured", "instructions": "Instructions per message", "allocations": "Allocations per message", "cold": "Cold start (instructions / allocations)", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

The table is read in your browser from the document the last run wrote, so nothing on this page is
a copy that could have gone stale.

The two percentage columns answer two different questions. `Crate over raw` is what this crate's
own consumer and publisher cost over the client they wrap, which is the question this repository
answers for. `Service over raw` is what a whole service pays; what the runtime adds sits between
the two columns, and it is published here rather than with the core because it is a fact about how
this crate and the runtime meet - how the stream yields, how deliveries arrive, how back-pressure
reaches the consumer.

Both scenarios read one shard, ask for 1000 records per `GetRecords` call and pause 20 ms after an
empty read, and every loop publishes one record per call because that is what this crate's
publisher issues. Those numbers decide what a shard delivers, so they are the same everywhere and
worth knowing before the row is read.

What the two rows differ in is where an acknowledgement goes. This crate settles a record by
checkpointing its shard through the lease store, and that store is pluggable: the default one keeps
the checkpoint in process, the `DynamoDB` one puts a second service in the loop. The raw loop writes
the same checkpoint to the same place - a string in a cell, or the same conditional `UpdateItem`
against the same table - so what separates the rows is the store, not the crate.

A percentage reported as `indistinguishable` is one where the two columns it compares differ by
less than the spread between runs of either. A figure below the run-to-run noise would read as
precision that was never measured, so none is published.

A row marked `emulator-bound` is one where the round trips a delivery costs already account for
half its time or more. The round trip is measured on its own, against the stack, before a single
record is published, and it is published with the environment below; the trips a delivery costs are
its share of a `GetRecords` call plus, on the checkpointing row, the conditional write that settles
it. What a flagged row says is that the crate and the runtime did their work inside a wait the
reader was paying anyway, which makes both percentage columns a floor under what they cost rather
than a measurement of it.

Neither row carries the flag today. A poll carries up to a thousand records, so its share of one
delivery is small; on the checkpointing row the settling write is a trip of its own and accounts
for about a third of what a delivery costs there. What paces these runs is something the
arithmetic does not count: what the emulator accepts. A single shard takes a few hundred records a
second from it, whichever loop publishes them, and that is why all three columns land inside one
another's spread. The rows say that neither this crate nor the runtime rises above that floor.
They do not say how the three would compare on a stand faster than they are.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-kinesis/latest/benchmarks/results.json).

## The crate's own code

<div id="benchmark-code"></div>

The second table is this crate's own cost per message, counted rather than timed: instructions
under callgrind and allocations under DHAT. Each scenario is the service a user writes, on
`KinesisBroker` built the way a service builds it, started by the runtime against the same
LocalStack stack. It reads a one-shard stream with the crate's shard reader and `GetRecords`,
settles through the default lease store, and replies with `PutRecord`.

What is counted is everything the service's thread runs: the framework's dispatch, this crate's
code, and the AWS SDK building, signing and parsing its requests. The emulator is another process,
and it is not counted. The records are published from a second thread before the count starts, and
the service reads them from the trim horizon of a stream created for the run.

Instructions and allocations are per message in the steady state: the slope between a run of 1000
deliveries and a run of 2000. A read of the shard is shared by the records it returns, so each row
carries its share of one: a read returns up to a thousand records on the first two rows and one
batch on the third. The last column is what starting the service and taking the first delivery
cost once: connecting, opening the subscription, listing the shard, and taking its lease and its
iterator. The numbers are absolute, the framework's own cost included; the core publishes that
cost alone on its [benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).

Between runs of one build the instruction counts move by less than half a percent and the
allocations by a few blocks on the longest runs, because the service does real I/O: a drain can end
with a read still in flight, and lease renewals and shard syncs run on timers. Each floor sits
above the highest count seen.
`just bench-code` fails on an allocation above the floor a scenario declares, and with
`--baseline=main` on more than two percent more instructions, and a pull request that changes the
cost cites its numbers.

## The machine

<div id="benchmark-environment"></div>

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one shard, one consumer, a 512 byte body and an emulator on the loopback. It measures what
a record costs in this crate, not what Kinesis can carry, and a row here is not comparable with a
row published for another broker: the transports do different work per record.

The window a run measures opens at the first delivery and closes when the last record's field has
been read, on all three loops alike, so the checkpoint that settles that last record sits outside
the number everywhere.

The feeder that fills the stream runs flat out and is never used to pace the consumer, so a
consumer that catches up with it pays the pause after an empty read. That pause is a poll the
delivery is charged for, and the round-trip arithmetic above is what says how much of the row it
accounts for.

The numbers are a snapshot of one machine on one day. They are re-measured by hand, on a machine
given to the run alone: the difference this page is about is smaller than the noise of a shared one.

## Running it yourself

```bash
just bench
```

The recipe starts the stack from `docker-compose.test.yml`, runs both scenarios, stops the stack
and rewrites `docs/benchmarks/results.json` with what it measured. It takes a few minutes and wants
the machine to itself. Every run creates its own one-shard stream, and the checkpointing scenario
its own lease table, so a run never sees what the one before it left behind. The record count is
not fixed: a probe run sets it so that every measured run lasts at least five seconds on whatever
machine it is taken on.

```bash
just bench-code
```

The recipe starts the same stack, counts the code table under valgrind, stops the stack and
rewrites the `code` section of the same document. It takes minutes and needs valgrind and the
benchmark runner besides the stack: `cargo install --locked gungraun-runner --version =0.19.4`.
