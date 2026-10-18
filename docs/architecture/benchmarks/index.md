---
title: Benchmarks
description: "What ruststream-kinesis costs on every record: throughput against the raw aws-sdk-kinesis client, and instructions and allocations of its code."
---

import {Environment, MovedSections, Results} from '@site/src/components/Benchmarks';
import {kinesis} from '@site/src/components/Benchmarks/kinesis';

export const labels = {"loading": "Loading the results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Broker crate", "framework": "RustStream", "overhead": "Overhead", "against": "({percent} over the client)", "indistinguishable": "indistinguishable", "brokerBound": "emulator-bound", "measured": "measured", "details": "How the numbers were taken", "pending": "No results published yet: {brokers}.", "instructions": "Instructions", "allocations": "Allocations", "cold": "Cold start", "table": "Show the numbers as a table", "axis": "How far each loop falls below the raw client's best round", "rounds": "rounds, best to slowest", "best": "best round", "median": "median round", "noise": "spread of the rounds", "comparison": "Against a raw client", "code": "Cost of the code", "broker": "The broker", "plugin": "The {plugin} plugin", "pluginRaw": "Without the plugin", "pluginAdapter": "Plugin written by hand", "pluginFramework": "The crate's plugin", "pluginAxis": "How far each loop falls below the best round of the app without the plugin", "pluginAgainst": "({percent} over no plugin)", "pluginOverhead": "Over the plugin by hand", "pluginOverHand": "({percent} over by hand)"};

export const machine = {"cpu": "Processor", "architecture": "Architecture", "cpu_frequency": "Frequency", "cores": "Cores", "memory": "Memory", "memory_speed": "Memory speed", "os": "Operating system", "broker": "Emulator", "rustc": "Compiler", "profile": "Build profile", "features": "Features", "rustflags": "RUSTFLAGS", "round_trip": "Round trip", "valgrind": "Valgrind"};

# Benchmarks

What `ruststream-kinesis` costs on every record: throughput against the raw `aws-sdk-kinesis`
client, and instructions and allocations of its code.

## Against a raw client

The first chart runs each scenario three ways against the same Kinesis endpoint. `Raw client`
drives `aws-sdk-kinesis` directly. `Broker crate` drives this crate's subscription with a loop of
its own and no runtime. `RustStream` is the service a user writes, a `#[subscriber]` handler in an
app. The first gap is what this crate costs over the client it wraps, and the second is what the
runtime adds on top.

Each loop is a bar from its best round to its slowest, with a dot at the best round and a notch at
the median. The zero of the axis is the raw client's best round, and a dot right of zero is slower.
A hollow dot is a loop whose difference from the raw client is smaller than the spread of the
rounds. The table under the chart prints the best round with the median in parentheses.

Both scenarios read one shard and checkpoint every record. They differ in where the checkpoint
goes: the first keeps it in process, as the default [lease store](../kinesis/leases.md) does, and
the second writes it to a DynamoDB table with a conditional write. The raw loop writes the same
checkpoint to the same place, so what separates the two rows is the store, not the crate.

## Cost of the code

The second chart counts what one record costs on the service's thread: instructions under
callgrind and allocations under DHAT, in the steady state. Each scenario is a service on
[`KinesisBroker`](api:ruststream_kinesis::KinesisBroker) against the same endpoint, with the
default lease store. The count includes the framework's dispatch and this crate's code. It also
includes the AWS SDK's work on that thread: building and signing every request, the HTTP exchange
and parsing every answer. That work is most of every figure, and nearly all of the reply row, so
these numbers are not comparable with those of a broker whose client works on threads of its own.
`Cold start` is what connecting, opening the subscription, listing the shard, taking its lease and
its iterator, and taking the first record cost once.

## Results

<Results entry={kinesis} labels={labels} details="/architecture/benchmarks/methodology/" />

## The machine

<Environment entry={kinesis} labels={machine} />

The numbers come from `docs/benchmarks/results.json` of this version, written by the benchmark
recipes. [Methodology](methodology.md) says how they were taken, and [Numbers](numbers.md) what
they allow you to conclude.

<MovedSections />
