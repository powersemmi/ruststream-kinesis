// The benchmark is a binary of its own, not library surface: a measured loop panics on a broker
// fault rather than threading a `Result` through a scenario nobody recovers from.
#![allow(
    missing_docs,
    unreachable_pub,
    unused_qualifications,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! What this crate costs over the `aws-sdk-kinesis` client it wraps, and what the runtime costs
//! over this crate.
//!
//! Each scenario runs three times over, as three loops that differ in one thing each - what
//! carries the records:
//!
//! - **raw** drives the client directly: `PutRecord` to publish, `GetRecords` to read.
//! - **adapter** drives this crate's own types: its broker, its subscription source, the
//!   `Subscriber` stream it yields, its `IncomingMessage` and its `ack`, its `Publisher`. A loop
//!   here pulls, decodes, reads a field and settles; no handler, no app, no dispatch.
//! - **framework** is the service a user writes: `#[subscriber]`, the app, the runtime.
//!
//! Two differences come out of that. Adapter against raw is what this crate's own consumer and
//! publisher cost over the client they wrap, which is what this repository answers for. Framework
//! against adapter is what the runtime costs on top of it over this broker in particular, which is
//! worth publishing here because it is about how the two meet.
//!
//! Everything else is the same across all three - one `SdkConfig` builds every client, the same
//! single-shard stream, the same `GetRecords` limit, the same poll interval, the same checkpoint
//! written in the same place, the same decode into the same type, the same payload bytes, the same
//! tokio runtime and the same binary. The procedure the numbers follow is the framework's own,
//! published at <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
//!
//! The stack is the `LocalStack` emulator the live suites use, not the hosted service. What a row
//! says is what a delivery costs in this crate; it says nothing about what Kinesis carries in a
//! region.
//!
//! # What a run is
//!
//! Every run creates its own one-shard stream - and, on the checkpointing scenario, its own lease
//! table - so it never sees what the run before it left behind. The consumer is attached first and
//! a feeder then fills the stream with `PutRecords`. Both halves open at the trim horizon, so a
//! record cannot be lost to the instant the shard iterator was taken. Creating the stream, taking
//! the lease and opening the iterator are startup cost and sit outside the window, which runs from
//! the first delivery to the end of the last handler call.
//!
//! The message count is not a constant: a probe run measures the raw half's rate and the count is
//! set from it, so a measured run lasts at least [`SECONDS`] on whatever machine it is taken on.
//!
//! The loops are interleaved - raw, adapter, framework, raw, adapter, framework - and each
//! reports its best, median and worst round. The best is the headline: noise only ever slows a
//! run down, so the fastest round is the closest to the undisturbed cost. The distance between
//! the best and the worst is the noise a difference has to clear. Running one to the end and then
//! the next would charge every drift of the machine to whichever ran last.
//!
//! # What the numbers do not say
//!
//! The window ends where the last record's field is read rather than after its checkpoint, on
//! every loop alike, so one checkpoint out of a run's many sits outside the number everywhere.
//!
//! A row is reported as broker-bound when the round trips one delivery costs already account for
//! half its time or more. The trip is measured by [`round_trip`] before any record is published,
//! outside both halves; the trips a delivery costs are its share of a `GetRecords` call plus, where
//! the checkpoint store is a second service, the conditional write that settles it. A flagged row
//! is a floor under the costs it reports rather than a measurement of them: the work happened
//! inside a wait the reader was paying anyway.

use std::convert::Infallible;
use std::env;
use std::fmt::Write as _;
use std::hint::black_box;
use std::iter::repeat_n;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_dynamodb::client::Waiters as _;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_kinesis::client::Waiters as _;
use aws_sdk_kinesis::operation::get_records::GetRecordsError;
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::ShardIteratorType;
use futures::StreamExt as _;
use ruststream::runtime::RunningApp;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, StartAt, Subscriber,
    SubscriptionSource,
};
use ruststream_kinesis::prelude::*;
use ruststream_kinesis::{DynamoLeaseStore, KinesisPublishOptions, KinesisPublisher, LeaseStore};
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};

// A benchmark measures what ships, and the framework's harness feature changes what a delivery
// does on its way to the caller. The benchmark lives in a package of its own for the same reason:
// `ruststream-kinesis`'s dev-dependencies enable that feature through the conformance harness, and
// a benchmark inside that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);

/// How long a measured run lasts, at least.
const SECONDS: f64 = 5.0;
/// How much the calibrated count is raised above the probe's estimate.
///
/// The probe is short and cold, so it reads the machine low; without the margin the fastest
/// scenario lands just under the floor.
const MARGIN: f64 = 1.25;
/// The ceiling on a calibrated count, so a machine an order faster does not turn a run into an
/// afternoon.
const MAX_MESSAGES: usize = 1_000_000;
/// Rounds run. Each loop reports its best, median and worst round.
const PAIRS: usize = 3;
/// Worker threads both halves are driven on.
const WORKERS: usize = 4;

/// The region both clients are built for. The emulator answers for any of them; naming one keeps
/// the two halves on the same endpoint resolution.
const REGION: &str = "us-east-1";
/// Records one `GetRecords` call asks for, on both halves. This is the crate's own default for a
/// subscription that delivers single records, spelled out here so the raw loop asks the emulator
/// for exactly what the crate asks it for.
const READ_LIMIT: i32 = 1000;
/// The pause a reader takes after an empty read, on both halves.
///
/// A shard is polled, so this is the one setting that decides what an idle moment costs. It is
/// far below the service's own recommendation on purpose: against an emulator on the loopback
/// there is no per-shard read budget to spend, and a second of sleep would measure the sleep.
const POLL_INTERVAL: Duration = Duration::from_millis(20);
/// How many times one record may be refused before the run stops.
const PUT_ATTEMPTS: usize = 100;
/// How many publishes are in the air at once while a run is filled.
///
/// One at a time would not outrun the consumer against an emulator, and a feeder that cannot
/// outrun the consumer becomes the thing every row reports. The number is the same on both halves,
/// so what it changes is the pace of the run rather than the comparison in it.
const FEED_TASKS: usize = 16;
/// How far the feeder may run ahead of the consumer, in records.
///
/// This is a memory guard and nothing else: a shard retains what a consumer has not read, and a
/// stack holding a whole run at once is a stack that swaps. The feeder is deliberately not used to
/// pace the consumer - a ceiling that binds makes the feeder wait for the consumer, which couples
/// the two and destroys the one signal that says which of them set the pace.
const MAX_IN_FLIGHT: usize = 50_000;
/// Round trips the latency probe takes, and the budget it may spend taking them.
///
/// The procedure asks for tens of thousands of trips on one client. Against an emulator a trip is
/// milliseconds rather than microseconds, so that many would be minutes of probing; the probe
/// stops at whichever comes first and reports what it actually measured.
const PROBE_CALLS: usize = 20_000;
const PROBE_BUDGET: Duration = Duration::from_secs(10);
/// The share of a delivery's time that has to be round trips before the row is called
/// broker-bound.
const BOUND_SHARE: f64 = 0.5;
/// How long a run may go without a delivery before it is called stuck.
const STALL: Duration = Duration::from_secs(60);
/// How long a lease is valid without renewal, and the renewal cadence derived from it. Both are
/// the crate's own, so the raw loop renews on the same schedule.
const LEASE_TTL: Duration = Duration::from_secs(10);
const RENEW_EVERY: Duration = Duration::from_secs(3);

/// The body size both halves publish and decode, to the byte: the scenario is published under
/// this number, so the bytes on the wire have to be it.
const BODY_BYTES: usize = 512;
/// How wide one padding value is before the next field starts.
const PAD_WIDTH: usize = 16;
/// The values every body carries. Fixed, so every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;
/// The partition key every record carries. A one-shard stream puts them all on that shard
/// whatever the key is, and a constant one keeps a record's bytes constant too.
const PARTITION_KEY: &str = "bench";

/// What both halves decode a delivery into.
///
/// Two integer fields the handler reads, and a padding the type ignores: a decode that allocates
/// nothing, so the number is about this crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// A JSON body carrying the two fields, padded with fields [`Order`] ignores until it is exactly
/// `size` bytes.
///
/// The padding is a run of equally wide fields and one last field cut to whatever is left, so a
/// scenario published as a 512 byte body is one. Building it is startup work, and the assertion
/// below holds the promise the published name makes.
fn json_body(size: usize) -> Vec<u8> {
    let mut body = format!("{{\"id\":{ID},\"quantity\":{QUANTITY}");
    let mut field = 0u32;
    loop {
        let key = format!(",\"f{field}\":\"\"");
        // One byte stays reserved for the closing brace.
        let Some(room) = size.checked_sub(body.len() + key.len() + 1) else {
            break;
        };
        // A full-width field only when what it leaves behind can still hold the next one, whose
        // key is at most one digit longer. Otherwise this is the last field and it takes the
        // rest, because a remainder too small to start a field would come out as a short body.
        let width = if room > PAD_WIDTH + key.len() {
            PAD_WIDTH
        } else {
            room
        };
        body.push_str(&key[..key.len() - 1]);
        body.extend(repeat_n('x', width));
        body.push('"');
        field += 1;
    }
    body.push('}');
    assert_eq!(
        body.len(),
        size,
        "a body has to be the size the scenario publishes"
    );
    body.into_bytes()
}

/// The names one run owns: nothing is shared with the run before it.
#[derive(Clone, Debug)]
struct Names {
    stream: String,
    table: String,
    owner: String,
}

impl Names {
    fn fresh() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        Self {
            stream: format!("rs-bench-{stamp}"),
            table: format!("rs-bench-leases-{stamp}"),
            owner: format!("raw-{stamp}"),
        }
    }
}

/// Counts deliveries and marks the ends of the measured window.
///
/// Both halves call the same methods, so both pay for the signal. A delivery pays one relaxed
/// increment and two comparisons; the waiter is a single future for the whole run, woken once.
#[derive(Clone, Debug)]
struct Run(Arc<RunInner>);

#[derive(Debug)]
struct RunInner {
    total: usize,
    seen: AtomicUsize,
    first: OnceLock<Instant>,
    last: OnceLock<Instant>,
    drained: Notify,
}

impl Run {
    fn new(total: usize) -> Self {
        Self(Arc::new(RunInner {
            total,
            seen: AtomicUsize::new(0),
            first: OnceLock::new(),
            last: OnceLock::new(),
            drained: Notify::new(),
        }))
    }

    /// Records one handled delivery, and answers whether the run is over.
    fn arrived(&self) -> bool {
        let seen = self.0.seen.fetch_add(1, Ordering::Relaxed) + 1;
        if seen == 1 {
            let _ = self.0.first.set(Instant::now());
        }
        if seen == self.0.total {
            let _ = self.0.last.set(Instant::now());
            self.0.drained.notify_one();
        }
        seen >= self.0.total
    }

    fn handled(&self) -> usize {
        self.0.seen.load(Ordering::Acquire).min(self.0.total)
    }

    /// Resolves once every expected delivery has been handled.
    async fn drained(&self) {
        while self.0.seen.load(Ordering::Acquire) < self.0.total {
            self.0.drained.notified().await;
        }
    }

    /// The measured window: the first delivery to the end of the last handler call.
    fn window(&self) -> Duration {
        let first = *self.0.first.get().expect("the run took a delivery");
        let last = *self.0.last.get().expect("the run took its last delivery");
        last - first
    }
}

/// Waits for the run to finish, and fails with what it was waiting for if it stops moving.
async fn drain(run: &Run, half: &str) {
    let mut seen = 0;
    loop {
        if timeout(STALL, run.drained()).await.is_ok() {
            return;
        }
        let handled = run.handled();
        assert!(
            handled > seen,
            "{half}: {handled} of {} deliveries handled and nothing moved for {STALL:?}",
            run.0.total
        );
        seen = handled;
    }
}

/// What the measured half of one run produced.
#[derive(Clone, Copy, Debug)]
struct Sample {
    window: Duration,
    fed: Fed,
    /// `GetRecords` calls the hand-written reader made inside the window, empty ones included.
    /// Each one is a round trip, and it is how the broker-bound verdict is decided. The adapter
    /// half leaves it at zero: its reader is inside the crate, and it reads with the same limit
    /// and the same pause, so the count that answers the question is this one.
    reads: usize,
}

impl Sample {
    fn rate(self, messages: usize) -> f64 {
        messages as f64 / self.window.as_secs_f64()
    }

    /// What the feeder managed, for the same run. A consumer cannot be measured above it.
    fn feed_rate(self, messages: usize) -> f64 {
        messages as f64 / self.fed.took.as_secs_f64()
    }

    fn report(self, half: &str, messages: usize) -> String {
        format!(
            "{half:>9}: {:>9.0} msg/s (fed {:>9.0} msg/s, {} reads, feeder waited {} times)",
            self.rate(messages),
            self.feed_rate(messages),
            self.reads,
            self.fed.throttled,
        )
    }
}

// ---------------------------------------------------------------------------------------------
// The stack
// ---------------------------------------------------------------------------------------------

/// The configuration both clients are built from.
///
/// One `SdkConfig` for the whole process, handed to the crate through
/// [`KinesisBroker::from_config`] and to the hand-written loop through `Client::new`. Two loaders
/// built the same way would still be two sets of retry, timeout and credential settings to keep
/// in step; one value cannot drift from itself.
async fn config(endpoint: &str) -> SdkConfig {
    aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint.to_owned())
        .region(Region::new(REGION))
        .test_credentials()
        .load()
        .await
}

/// Creates the run's stream with one shard and waits until it is readable.
async fn create_stream(client: &KinesisClient, stream: &str) {
    client
        .create_stream()
        .stream_name(stream)
        .shard_count(1)
        .send()
        .await
        .expect("the stack creates the stream");
    client
        .wait_until_stream_exists()
        .stream_name(stream)
        .wait(Duration::from_secs(60))
        .await
        .expect("the stream becomes active");
}

async fn delete_stream(client: &KinesisClient, stream: &str) {
    let _ = client.delete_stream().stream_name(stream).send().await;
}

/// The one shard of the run's stream.
async fn only_shard(client: &KinesisClient, stream: &str) -> String {
    let shards = client
        .list_shards()
        .stream_name(stream)
        .send()
        .await
        .expect("the stack lists the shards");
    shards
        .shards()
        .first()
        .expect("a stream created with one shard has one")
        .shard_id()
        .to_owned()
}

/// Creates the run's lease table: the schema [`DynamoLeaseStore`] documents, a string partition
/// key named `lease_key` and nothing else.
async fn create_table(client: &DynamoClient, table: &str) {
    client
        .create_table()
        .table_name(table)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("lease_key")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("the attribute definition is complete"),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("lease_key")
                .key_type(KeyType::Hash)
                .build()
                .expect("the key schema is complete"),
        )
        .send()
        .await
        .expect("the stack creates the table");
    client
        .wait_until_table_exists()
        .table_name(table)
        .wait(Duration::from_secs(60))
        .await
        .expect("the table becomes active");
}

async fn delete_table(client: &DynamoClient, table: &str) {
    let _ = client.delete_table().table_name(table).send().await;
}

/// What the feeder of one run reports back.
#[derive(Clone, Copy, Debug)]
struct Fed {
    /// How long filling the stream took. A window no shorter than this is a window the feeder
    /// paced, whichever half was reading it.
    took: Duration,
    /// How often a feeding task had to wait for the consumer. Zero means the consumer was never
    /// the limit.
    throttled: usize,
}

/// This stack's round-trip time, measured outside both halves.
///
/// A cheap request whose answer the client waits for, back to back on one client:
/// `DescribeStreamSummary` returns one stream's header and nothing else, so what it times is the
/// trip rather than the work at the far end. The figure is published with the results, and it is
/// what decides whether a row measures dispatch or measures the trip.
async fn round_trip(config: &SdkConfig, stream: &str) -> (Duration, usize) {
    let client = KinesisClient::new(config);
    // The first call opens the connection; the trip is what the ones after it cost.
    client
        .describe_stream_summary()
        .stream_name(stream)
        .send()
        .await
        .expect("the stack answers the probe");

    let started = Instant::now();
    let mut calls = 0u32;
    while (calls as usize) < PROBE_CALLS && started.elapsed() < PROBE_BUDGET {
        client
            .describe_stream_summary()
            .stream_name(stream)
            .send()
            .await
            .expect("the stack answers the probe");
        calls += 1;
    }
    (started.elapsed() / calls, calls as usize)
}

/// The publish side of one half: the client this crate wraps, or this crate's own publisher.
///
/// A record is one `PutRecord` either way, because that is what the crate's publisher issues; a
/// raw half batching five hundred records into one call would be measured against a different
/// protocol, not against a different publisher.
#[derive(Clone)]
enum Ingress {
    /// `PutRecord` on the client, with the partition key on the request.
    Raw(KinesisClient),
    /// [`KinesisPublisher::publish`], with the partition key in its options. A body with no
    /// headers is written to the record as it is, so both halves put the same bytes on the wire.
    Adapter(KinesisPublisher),
}

impl Ingress {
    async fn put(&self, stream: &str, body: &[u8]) {
        let mut attempts = 0;
        loop {
            let outcome: Result<(), String> = match self {
                Self::Raw(client) => client
                    .put_record()
                    .stream_name(stream)
                    .partition_key(PARTITION_KEY)
                    .data(Blob::new(body.to_vec()))
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|err| err.to_string()),
                Self::Adapter(publisher) => publisher
                    .publish(
                        OutgoingMessage::new(stream, body),
                        Some(&KinesisPublishOptions {
                            partition_key: Some(PARTITION_KEY.to_owned()),
                        }),
                    )
                    .await
                    .map_err(|err| err.to_string()),
            };
            let Err(err) = outcome else {
                return;
            };
            // A shard asking for a slower cadence is the common refusal, and it is answered the
            // same way on both halves: wait and try the same record again. A refusal that keeps
            // coming back is not a cadence problem and stops the run.
            attempts += 1;
            assert!(attempts < PUT_ATTEMPTS, "the stack refuses a record: {err}");
            sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Fills the stream with the run's bodies, as fast as the stack takes them.
///
/// The calls go out on [`FEED_TASKS`] tasks at once, because one at a time is not enough: a round
/// trip against the emulator takes long enough that a single caller would set the pace of every
/// run and both halves would report the emulator's ingest rate. What the tasks share is a claim
/// counter, so between them they publish exactly `messages` records.
async fn feed(ingress: Ingress, stream: String, messages: usize, run: Run) -> Fed {
    let started = Instant::now();
    let body = Arc::new(json_body(BODY_BYTES));
    let claimed = Arc::new(AtomicUsize::new(0));
    let throttled = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::with_capacity(FEED_TASKS);
    for _ in 0..FEED_TASKS {
        let ingress = ingress.clone();
        let stream = stream.clone();
        let run = run.clone();
        let body = Arc::clone(&body);
        let claimed = Arc::clone(&claimed);
        let throttled = Arc::clone(&throttled);
        tasks.push(tokio::spawn(async move {
            loop {
                let mine = claimed.fetch_add(1, Ordering::Relaxed);
                if mine >= messages {
                    break;
                }
                while mine.saturating_sub(run.handled()) > MAX_IN_FLIGHT {
                    throttled.fetch_add(1, Ordering::Relaxed);
                    sleep(Duration::from_micros(500)).await;
                }
                ingress.put(&stream, &body).await;
            }
        }));
    }
    for task in tasks {
        task.await.expect("a feeding task ends");
    }
    Fed {
        took: started.elapsed(),
        throttled: throttled.load(Ordering::Relaxed),
    }
}

// ---------------------------------------------------------------------------------------------
// The checkpoint, written the same way on both halves
// ---------------------------------------------------------------------------------------------

/// Where the hand-written loop records the shard's progress.
///
/// The crate settles a delivery by checkpointing the shard through its lease store, so the raw
/// half writes the same checkpoint to the same place: an in-process cell for the default store,
/// and the lease table's conditional write for the `DynamoDB` one.
enum Checkpoint {
    /// What [`MemoryLeaseStore`](ruststream_kinesis::MemoryLeaseStore) does: the sequence number
    /// copied into a cell behind a mutex.
    InProcess(Mutex<Option<String>>),
    /// What [`DynamoLeaseStore`] does: one conditional `UpdateItem` per checkpoint, refused
    /// unless this owner still holds the lease.
    Dynamo {
        client: DynamoClient,
        table: String,
        /// The row the crate keeps for the shard, `stream:shard`.
        row: String,
        owner: String,
    },
}

impl Checkpoint {
    /// Takes the lease the conditional writes below are checked against.
    async fn acquire(&self) {
        let Self::Dynamo {
            client,
            table,
            row,
            owner,
        } = self
        else {
            return;
        };
        let now = millis();
        let expiry = now + LEASE_TTL.as_millis() as u64;
        client
            .update_item()
            .table_name(table)
            .key("lease_key", AttributeValue::S(row.clone()))
            .update_expression(
                "SET lease_owner = :me, lease_expiry = :expiry ADD lease_counter :one",
            )
            .condition_expression(
                "attribute_not_exists(lease_owner) OR lease_owner = :me OR lease_expiry < :now",
            )
            .expression_attribute_values(":me", AttributeValue::S(owner.clone()))
            .expression_attribute_values(":expiry", AttributeValue::N(expiry.to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
            .expression_attribute_values(":one", AttributeValue::N("1".to_owned()))
            .send()
            .await
            .expect("the lease is taken");
    }

    /// Keeps the lease alive on the crate's own cadence.
    async fn renew(&self) {
        let Self::Dynamo {
            client,
            table,
            row,
            owner,
        } = self
        else {
            return;
        };
        let expiry = millis() + LEASE_TTL.as_millis() as u64;
        client
            .update_item()
            .table_name(table)
            .key("lease_key", AttributeValue::S(row.clone()))
            .update_expression("SET lease_expiry = :expiry ADD lease_counter :one")
            .condition_expression("lease_owner = :me")
            .expression_attribute_values(":me", AttributeValue::S(owner.clone()))
            .expression_attribute_values(":expiry", AttributeValue::N(expiry.to_string()))
            .expression_attribute_values(":one", AttributeValue::N("1".to_owned()))
            .send()
            .await
            .expect("the lease is renewed");
    }

    /// Records one settled record, the way the crate's store records it.
    async fn record(&self, sequence: &str) {
        match self {
            Self::InProcess(cell) => {
                *cell.lock().expect("the cell is never held across a panic") =
                    Some(sequence.to_owned());
            }
            Self::Dynamo {
                client,
                table,
                row,
                owner,
            } => {
                client
                    .update_item()
                    .table_name(table)
                    .key("lease_key", AttributeValue::S(row.clone()))
                    .update_expression("SET checkpoint = :seq ADD lease_counter :one")
                    .condition_expression("lease_owner = :me")
                    .expression_attribute_values(":me", AttributeValue::S(owner.clone()))
                    .expression_attribute_values(":seq", AttributeValue::S(sequence.to_owned()))
                    .expression_attribute_values(":one", AttributeValue::N("1".to_owned()))
                    .send()
                    .await
                    .expect("the checkpoint is written");
            }
        }
    }
}

fn millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

// ---------------------------------------------------------------------------------------------
// The service half
// ---------------------------------------------------------------------------------------------

/// The stream the service being built subscribes to.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, which is
/// inside the builder of the run that is starting. A run installs its own name here first, so the
/// subscription the runtime opens is the one this run feeds.
static SUBJECT: Mutex<Option<String>> = Mutex::new(None);

fn install(stream: &str) {
    *SUBJECT
        .lock()
        .expect("the name cell is never held across a panic") = Some(stream.to_owned());
}

fn installed() -> String {
    SUBJECT
        .lock()
        .expect("the name cell is never held across a panic")
        .clone()
        .expect("a run installs its stream before it builds the service")
}

/// The handler a service writes: a decoded record, a field read, an acknowledgement.
#[subscriber(KinesisStream::new(installed()).poll_interval(POLL_INTERVAL))]
async fn consume(order: &Order, ctx: &mut Context<'_, KinesisContext, Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

async fn start(broker: KinesisBroker, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("kinesis-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(broker, |b| {
            b.include(consume.start_at(KinesisPosition::horizon()));
        })
        .start()
        .await
        .expect("the service starts")
}

// ---------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Scenario {
    /// The default store: a checkpoint is an in-process write.
    InProcess,
    /// The `DynamoDB` store: a checkpoint is a conditional write to a second service.
    Dynamo,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Self::InProcess => "one shard, 512 B JSON, in-process checkpoints",
            Self::Dynamo => "one shard, 512 B JSON, DynamoDB checkpoints",
        }
    }

    /// Deliveries the probe run takes to measure this scenario's rate. A checkpoint that crosses
    /// a service is orders slower than one that does not, so the two cannot probe with the same
    /// count without one of them taking an afternoon.
    fn probe_messages(self) -> usize {
        match self {
            Self::InProcess => 5_000,
            Self::Dynamo => 500,
        }
    }

    fn checkpointing(self) -> bool {
        matches!(self, Self::Dynamo)
    }

    /// Round trips settling one delivery costs. The in-process store writes a cell; the `DynamoDB`
    /// store writes one conditional `UpdateItem`, which is a trip of its own.
    fn settle_round_trips(self) -> f64 {
        if self.checkpointing() { 1.0 } else { 0.0 }
    }
}

/// Everything a run needs from the stack, stood up before the window and torn down after it.
struct Stack {
    kinesis: KinesisClient,
    dynamo: DynamoClient,
    names: Names,
    shard: String,
    checkpointing: bool,
}

impl Stack {
    async fn raise(config: &SdkConfig, scenario: Scenario) -> Self {
        let names = Names::fresh();
        let kinesis = KinesisClient::new(config);
        let dynamo = DynamoClient::new(config);
        create_stream(&kinesis, &names.stream).await;
        if scenario.checkpointing() {
            create_table(&dynamo, &names.table).await;
        }
        let shard = only_shard(&kinesis, &names.stream).await;
        Self {
            kinesis,
            dynamo,
            names,
            shard,
            checkpointing: scenario.checkpointing(),
        }
    }

    /// The store the adapter half checkpoints through.
    fn store(&self, config: &SdkConfig) -> Option<Arc<dyn LeaseStore>> {
        self.checkpointing.then(|| {
            Arc::new(DynamoLeaseStore::new(config, &self.names.table)) as Arc<dyn LeaseStore>
        })
    }

    /// The same store for the hand-written half.
    fn checkpoint(&self) -> Checkpoint {
        if self.checkpointing {
            Checkpoint::Dynamo {
                client: self.dynamo.clone(),
                table: self.names.table.clone(),
                row: format!("{}:{}", self.names.stream, self.shard),
                owner: self.names.owner.clone(),
            }
        } else {
            Checkpoint::InProcess(Mutex::new(None))
        }
    }

    async fn lower(self) {
        delete_stream(&self.kinesis, &self.names.stream).await;
        if self.checkpointing {
            delete_table(&self.dynamo, &self.names.table).await;
        }
    }
}

/// The hand-written half: one shard, read in a loop, decoded and checkpointed inline.
async fn raw(config: &SdkConfig, scenario: Scenario, messages: usize) -> Sample {
    let stack = Stack::raise(config, scenario).await;
    // One client for reading and for publishing, which is the shape the crate gives its half: a
    // connected broker hands out one client and the publisher shares it with the subscription.
    // `stack.kinesis` created the stream and listed its shards, so it stays out of the run.
    let client = KinesisClient::new(config);
    let checkpoint = Arc::new(stack.checkpoint());
    checkpoint.acquire().await;
    let mut iterator = client
        .get_shard_iterator()
        .stream_name(&stack.names.stream)
        .shard_id(&stack.shard)
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await
        .expect("the stack opens a shard iterator")
        .shard_iterator()
        .expect("the iterator is there")
        .to_owned();

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        let client = client.clone();
        let checkpoint = Arc::clone(&checkpoint);
        async move {
            let mut last_renew = Instant::now();
            // Every read is a round trip, and the count of them is what decides whether this row
            // is a measurement of dispatch or of the trip. Reads before the first delivery are
            // outside the window and are not counted.
            let mut reads = 0usize;
            loop {
                if last_renew.elapsed() >= RENEW_EVERY {
                    checkpoint.renew().await;
                    last_renew = Instant::now();
                }
                let answer = client
                    .get_records()
                    .shard_iterator(&iterator)
                    .limit(READ_LIMIT)
                    .send()
                    .await;
                if run.handled() > 0 {
                    reads += 1;
                }
                let output = match answer {
                    Ok(output) => output,
                    // Throttling is the stack asking for a slower cadence, not a failed read: the
                    // iterator is still good, so the reader waits and reuses it. This is what the
                    // crate's own reader does, and a half that panicked here instead would
                    // measure a different contract.
                    Err(err)
                        if err.as_service_error().is_some_and(
                            GetRecordsError::is_provisioned_throughput_exceeded_exception,
                        ) =>
                    {
                        sleep(POLL_INTERVAL).await;
                        continue;
                    }
                    Err(err) => panic!("the stack answers the read: {err}"),
                };
                let mut done = false;
                for record in output.records() {
                    let order: Order =
                        serde_json::from_slice(record.data().as_ref()).expect("the body decodes");
                    black_box((order.id, order.quantity));
                    // The crate checkpoints once the delivery is settled, so the window closes
                    // before the checkpoint on this half too.
                    done = run.arrived();
                    checkpoint.record(record.sequence_number()).await;
                    if done {
                        break;
                    }
                }
                if done {
                    break;
                }
                let empty = output.records().is_empty();
                let Some(next) = output.next_shard_iterator() else {
                    break;
                };
                iterator = next.to_owned();
                if empty {
                    sleep(POLL_INTERVAL).await;
                }
            }
            reads
        }
    });

    // The feeder runs as a task of its own, so a consumer that stops is caught by the stall
    // detector below with what it was waiting for. Awaited here, it would sit on the in-flight
    // guard forever instead and the run would hang with nothing to read.
    let feeding = tokio::spawn(feed(
        Ingress::Raw(client.clone()),
        stack.names.stream.clone(),
        messages,
        run.clone(),
    ));
    drain(&run, "raw").await;
    let reads = consuming.await.expect("the consuming task ends");
    let fed = feeding.await.expect("the feeder ends");
    let sample = Sample {
        window: run.window(),
        fed,
        reads,
    };
    stack.lower().await;
    sample
}

/// The measured half: the same shard, read through this crate's own subscription and fed through
/// its own publisher.
///
/// A loop in this file pulls from the `Subscriber` stream, decodes the record, reads a field and
/// settles it. No service is started: what this half is asked is what the crate's own consumer and
/// publisher cost over the client they wrap, and the runtime's own cost is the core crate's to
/// publish.
async fn adapter(config: &SdkConfig, scenario: Scenario, messages: usize) -> Sample {
    let stack = Stack::raise(config, scenario).await;
    let mut broker = KinesisBroker::from_config(config.clone());
    if let Some(store) = stack.store(config) {
        broker = broker.lease_store(store);
    }
    let connected = broker.connect().await.expect("the broker connects");
    let mut subscriber = StartAt::new(
        KinesisStream::new(stack.names.stream.clone()).poll_interval(POLL_INTERVAL),
        KinesisPosition::horizon(),
    )
    .subscribe(&connected)
    .await
    .expect("the subscription opens");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            let mut deliveries = pin!(subscriber.stream());
            while let Some(delivery) = deliveries.next().await {
                let message = delivery.expect("the subscription delivers");
                let order: Order =
                    serde_json::from_slice(message.payload()).expect("the body decodes");
                black_box((order.id, order.quantity));
                // Settled after the field is read, which is where the raw half settles too.
                let done = run.arrived();
                message.ack().await.expect("the ack succeeds");
                if done {
                    break;
                }
            }
        }
    });

    // The crate hands out one client behind the connected broker, and the publisher shares it with
    // the subscription. The raw half is arranged the same way, so neither is measured under
    // contention the other does not see.
    let feeding = tokio::spawn(feed(
        Ingress::Adapter(connected.publisher()),
        stack.names.stream.clone(),
        messages,
        run.clone(),
    ));
    drain(&run, "adapter").await;
    consuming.await.expect("the consuming task ends");
    let fed = feeding.await.expect("the feeder ends");
    connected.shutdown().await.expect("the broker shuts down");
    let sample = Sample {
        window: run.window(),
        fed,
        reads: 0,
    };
    stack.lower().await;
    sample
}

/// The service half: the same shard, read by the service a user writes.
///
/// The publisher is the crate's own here too, so what separates this loop from the adapter loop is
/// the runtime alone: the handler, the dispatch and the settling around it.
async fn framework(config: &SdkConfig, scenario: Scenario, messages: usize) -> Sample {
    let stack = Stack::raise(config, scenario).await;
    let mut broker = KinesisBroker::from_config(config.clone());
    if let Some(store) = stack.store(config) {
        broker = broker.lease_store(store);
    }
    // Minted before the service starts and resolved on first use, which is the crate's documented
    // early-publisher path. It shares the client the service connects, exactly as the adapter
    // half's publisher shares the client its subscription reads through.
    let publisher = broker.publisher();

    let run = Run::new(messages);
    install(&stack.names.stream);
    let app = start(broker, run.clone()).await;
    let feeding = tokio::spawn(feed(
        Ingress::Adapter(publisher),
        stack.names.stream.clone(),
        messages,
        run.clone(),
    ));
    drain(&run, "framework").await;
    let fed = feeding.await.expect("the feeder ends");
    app.shutdown().await.expect("the service stops");
    let sample = Sample {
        window: run.window(),
        fed,
        reads: 0,
    };
    stack.lower().await;
    sample
}

/// Best, median and worst of the rounds.
///
/// Noise on the machine only ever slows a run down, so the fastest round is the closest to the
/// undisturbed cost, the median is the typical one, and the slowest says how far from quiet the
/// machine was.
#[derive(Clone, Copy, Debug)]
struct Stats {
    best: f64,
    median: f64,
    worst: f64,
}

impl Stats {
    fn of(rates: &[f64]) -> Self {
        assert!(!rates.is_empty(), "no round was run");
        let mut sorted = rates.to_vec();
        sorted.sort_by(f64::total_cmp);
        let middle = sorted.len() / 2;
        let median = if sorted.len() % 2 == 1 {
            sorted[middle]
        } else {
            f64::midpoint(sorted[middle - 1], sorted[middle])
        };
        Self {
            best: sorted[sorted.len() - 1],
            median,
            worst: sorted[0],
        }
    }

    fn spread(self) -> f64 {
        self.best - self.worst
    }
}

#[derive(Debug)]
struct Measured {
    scenario: Scenario,
    messages: usize,
    pairs: usize,
    raw: Stats,
    /// This crate's own consumer and publisher, hand-driven.
    adapter: Stats,
    /// The whole service: the same crate under the runtime.
    framework: Stats,
    overhead_percent: f64,
    adapter_overhead_percent: f64,
    adapter_verdict: &'static str,
    verdict: &'static str,
    broker_bound: bool,
    /// Round trips one delivery costs the hand-written reader: its share of a `GetRecords` call,
    /// plus the checkpoint write where the store is a second service.
    round_trips: f64,
}

async fn measure(
    scenario: Scenario,
    config: &SdkConfig,
    pairs: usize,
    seconds: f64,
    trip: Duration,
) -> Measured {
    // The probe is the warm-up as well: its result is thrown away, and the rate it measured sets
    // a count that makes every run below last at least `seconds`.
    let probe_messages = scenario.probe_messages();
    let probe = raw(config, scenario, probe_messages).await;
    let messages = ((probe.rate(probe_messages) * seconds * MARGIN) as usize)
        .clamp(probe_messages, MAX_MESSAGES);
    println!(
        "{}: {messages} messages per run\n{}",
        scenario.name(),
        probe.report("probe", probe_messages)
    );

    let mut raws = Vec::with_capacity(pairs);
    let mut adapters = Vec::with_capacity(pairs);
    let mut frameworks = Vec::with_capacity(pairs);
    let mut reads = 0usize;
    for round in 1..=pairs {
        let raw_sample = raw(config, scenario, messages).await;
        println!("{}", raw_sample.report("raw", messages));
        let adapter_sample = adapter(config, scenario, messages).await;
        println!("{}", adapter_sample.report("adapter", messages));
        let framework_sample = framework(config, scenario, messages).await;
        println!("{}", framework_sample.report("framework", messages));
        println!("  (round {round})");
        raws.push(raw_sample.rate(messages));
        adapters.push(adapter_sample.rate(messages));
        frameworks.push(framework_sample.rate(messages));
        reads += raw_sample.reads;
    }

    let raw = Stats::of(&raws);
    let adapter = Stats::of(&adapters);
    let framework = Stats::of(&frameworks);
    // The verdict is about the column the procedure compares: the whole service against the raw
    // client. The adapter's own difference is published beside it with a verdict of its own, by
    // the same rule.
    let difference = (raw.best - framework.best).abs();
    let adapter_difference = (raw.best - adapter.best).abs();

    // What a delivery costs the raw reader in round trips: its share of a `GetRecords` call, which
    // carries up to `READ_LIMIT` records and is charged whether or not it carried any, plus the
    // checkpoint write where the store is a second service. Set against the time a delivery took,
    // it says whether this row measures dispatch or measures the wire.
    let round_trips = reads as f64 / (messages * pairs) as f64 + scenario.settle_round_trips();
    let waiting = round_trips * trip.as_secs_f64();
    let per_message = 1.0 / raw.best;
    println!(
        "  {round_trips:.4} round trips per delivery x {:.3} ms = {:.3} ms, against {:.3} ms a delivery took",
        trip.as_secs_f64() * 1e3,
        waiting * 1e3,
        per_message * 1e3,
    );

    Measured {
        scenario,
        messages,
        pairs,
        raw,
        adapter,
        framework,
        overhead_percent: (raw.best - framework.best) / raw.best * 100.0,
        adapter_overhead_percent: (raw.best - adapter.best) / raw.best * 100.0,
        adapter_verdict: if adapter_difference < raw.spread().max(adapter.spread()) {
            "indistinguishable"
        } else {
            "measured"
        },
        verdict: if difference < raw.spread().max(framework.spread()) {
            "indistinguishable"
        } else {
            "measured"
        },
        broker_bound: waiting >= BOUND_SHARE * per_message,
        round_trips,
    }
}

fn document(measured: &[Measured], trip: Duration, calls: usize) -> String {
    let mut out = String::new();
    write!(
        out,
        concat!(
            "{{\n",
            "  \"round_trip\": {{ \"microseconds\": {micros:.1}, \"calls\": {calls},",
            " \"request\": \"DescribeStreamSummary\" }},\n",
            "  \"scenarios\": [\n",
        ),
        micros = trip.as_secs_f64() * 1e6,
        calls = calls,
    )
    .expect("writing to a String");
    for (index, row) in measured.iter().enumerate() {
        let comma = if index + 1 == measured.len() { "" } else { "," };
        write!(
            out,
            concat!(
                "    {{\n",
                "      \"name\": \"{name}\",\n",
                "      \"unit\": \"msg/s\",\n",
                "      \"messages\": {messages},\n",
                "      \"pairs\": {pairs},\n",
                "      \"raw\": {{ \"best\": {raw_best:.0}, \"median\": {raw_median:.0}, \"worst\": {raw_worst:.0} }},\n",
                "      \"adapter\": {{ \"best\": {ad_best:.0}, \"median\": {ad_median:.0}, \"worst\": {ad_worst:.0} }},\n",
                "      \"framework\": {{ \"best\": {fw_best:.0}, \"median\": {fw_median:.0}, \"worst\": {fw_worst:.0} }},\n",
                "      \"adapter_overhead_percent\": {adapter_overhead:.1},\n",
                "      \"adapter_verdict\": \"{adapter_verdict}\",\n",
                "      \"overhead_percent\": {overhead:.1},\n",
                "      \"verdict\": \"{verdict}\",\n",
                "      \"broker_bound\": {broker_bound},\n",
                "      \"round_trips_per_message\": {round_trips:.4}\n",
                "    }}{comma}\n",
            ),
            name = row.scenario.name(),
            messages = row.messages,
            pairs = row.pairs,
            raw_best = row.raw.best,
            raw_median = row.raw.median,
            raw_worst = row.raw.worst,
            ad_best = row.adapter.best,
            ad_median = row.adapter.median,
            ad_worst = row.adapter.worst,
            fw_best = row.framework.best,
            fw_median = row.framework.median,
            fw_worst = row.framework.worst,
            adapter_overhead = row.adapter_overhead_percent,
            adapter_verdict = row.adapter_verdict,
            overhead = row.overhead_percent,
            verdict = row.verdict,
            broker_bound = row.broker_bound,
            round_trips = row.round_trips,
            comma = comma,
        )
        .expect("writing to a String");
    }
    out.push_str("  ]\n}\n");
    out
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("the tokio runtime builds")
}

/// A positive count from the environment, or the default.
///
/// The parse target rejects zero, so a pairs count of zero is refused here rather than after the
/// probe run, where it would panic in the statistics with no round to report.
fn number(name: &str, fallback: usize) -> usize {
    env::var(name).ok().map_or(fallback, |value| {
        value
            .parse::<NonZeroUsize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive number"))
            .get()
    })
}

fn main() {
    let endpoint = env::var("KINESIS_TEST_ENDPOINT")
        .expect("KINESIS_TEST_ENDPOINT names the stack to measure against; `just bench` sets it");
    let pairs = number("RUSTSTREAM_BENCH_PAIRS", PAIRS);
    let seconds = number("RUSTSTREAM_BENCH_SECONDS", SECONDS as usize) as f64;
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let runtime = runtime();
    let config = runtime.block_on(config(&endpoint));

    // The latency probe runs outside every pair, against a stream of its own, before a single
    // record is published. What it measures belongs to the stack rather than to either half.
    let (trip, calls) = runtime.block_on(async {
        let client = KinesisClient::new(&config);
        let stream = Names::fresh().stream;
        create_stream(&client, &stream).await;
        let measured = round_trip(&config, &stream).await;
        delete_stream(&client, &stream).await;
        measured
    });
    println!(
        "round trip: {:.3} ms over {calls} calls\n",
        trip.as_secs_f64() * 1e3
    );

    let measured: Vec<Measured> = [Scenario::InProcess, Scenario::Dynamo]
        .into_iter()
        .map(|scenario| runtime.block_on(measure(scenario, &config, pairs, seconds, trip)))
        .collect();

    println!();
    for row in &measured {
        println!(
            "{}: raw {:.0}, adapter {:.0} ({:+.1}%), framework {:.0} ({:+.1}%) msg/s ({}{})",
            row.scenario.name(),
            row.raw.best,
            row.adapter.best,
            row.adapter_overhead_percent,
            row.framework.best,
            row.overhead_percent,
            row.verdict,
            if row.broker_bound {
                ", broker-bound"
            } else {
                ""
            }
        );
    }

    std::fs::write(&out, document(&measured, trip, calls)).expect("the summary is written");
    println!("\nwrote {out}");
}
