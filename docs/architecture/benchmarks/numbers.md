---
title: Numbers
description: "What the benchmark numbers of ruststream-kinesis measure, and what they do not allow you to conclude."
---

# Numbers

What the benchmark numbers of `ruststream-kinesis` measure, and what they do not allow you to
conclude.

## The comparison against a raw client

The three loops of a row are comparable with each other, because they ran in one process,
interleaved, against one endpoint. A row is not comparable with a row another broker crate
publishes: the transports do different work per record.

A read carries up to a thousand records, so its share of one record is small. On the DynamoDB row
each record also costs a conditional write of its own, the checkpoint behind its `ack`. In an
`emulator-bound` row the round trips took at least half of the time a delivery took. The work of
the crate and the runtime then happens inside a wait the raw client pays as well, and the
difference between the loops is a lower bound on that work, not a measurement of it.

What paces both rows is how fast the emulator takes and returns records on one shard, whichever
loop reads them, and the round-trip arithmetic does not count that. So all three loops of a row land
inside one another's spread. The rows say that neither this crate nor the runtime rises above that
floor. They do not say how the three would compare on a faster stand.

## The cost of the code

An instruction count holds the runtime, this crate and the AWS SDK's work on the service's thread:
building and signing each request, the HTTP exchange and parsing each answer. A delivery pays its
share of a `GetRecords`, and a reply adds a `PutRecord` of its own, so the reply row is almost all
SDK work. Most of every figure is the SDK's work rather than this crate's, and the figures are not
comparable with those of a broker whose client works on threads of its own. The same count costs
more time where it misses the cache, so a count is not a time.

Every figure is the steady state, the difference between a run of 1000 deliveries and one of 2000.
Connecting, opening the subscription, listing the shard, taking its lease and its iterator, and the
first allocations behind them are the cold start, paid once.

## What they do not mean

The stand is LocalStack, an emulator of Kinesis and DynamoDB, not the hosted service. It answers
over plain HTTP on the loopback, which leaves out the TLS handshake, the credential refresh and the
latency of a region. The comparison stays valid, because all three loops talk to the same
emulator. The throughput does not carry over: a stream in a region has per-shard limits and answers
on another timescale.

A row is one shard, one consumer and a 512-byte body. It measures what a record costs inside this
crate, not what Kinesis can carry.

The window closes when the last record's field is read, before its checkpoint, in all three loops,
so one checkpoint of a run sits outside the number everywhere. A consumer that catches up with the
feeder pays the 20-millisecond pause after an empty read, and that pause counts toward the record
it delays.

The numbers are a snapshot of one machine on one day, with the build recorded beside them. They are
measured again by hand, on a machine given to the run alone: the differences they show are smaller
than the noise of a shared one.
