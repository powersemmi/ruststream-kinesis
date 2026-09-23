//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! service setup, the stand the service talks to, the latch a handler counts deliveries down on,
//! and the measurement configuration. The method is the core's, described in its
//! `benches/common` and on the
//! [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! A scenario runs the service a user writes: the app on [`KinesisBroker`], built with the
//! constructor a service writes and pointed at the `LocalStack` stand the live suites use, and
//! started through [`RustStream::start`]. The broker reads the stream with its shard readers and
//! `GetRecords`, settles through its default lease store, and publishes with `PutRecord`, so what
//! comes out is what a message costs in this crate's code, the AWS SDK's and the framework's.
//!
//! # A run
//!
//! Every run creates a one-shard stream of its own and opens its subscription at the trim
//! horizon, so the records it reads are exactly the records it published. The records go in from
//! a second thread with a runtime of its own, after the service has started and before the drain
//! begins, and the drain begins only once the stand has accepted every one of them. The service
//! runs on a current-thread runtime, which only runs inside `block_on`, so nothing is consumed
//! while the stream fills.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] deliveries and over twice as
//! many. The slope between the last two is the steady-state cost of a message: everything that
//! happens once is in both totals and cancels in the subtraction. The one-delivery run is the
//! cold start, reported on its own: connecting, opening the subscription, listing the shard,
//! taking its lease and its iterator, and the first delivery.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. Everything on the service's thread inside the region is counted: the dispatcher, the
//! codec, this crate's code, the SDK building, signing and parsing its requests, and tokio's share
//! of driving them. The stand's work is another process, and threads the client starts on its own
//! are not the service's thread, so neither is counted. [`measure`] is the only frame that
//! carries its name, because a toggle on a name that also appears inside closure types switches
//! collection off again one frame deeper. DHAT is pointed at the same frame; the number read is
//! `Total blocks`, allocations per run.
//!
//! Real I/O makes the counts move a little between runs of one binary. A drain ends where the
//! latch is released, and a read the shard reader sent before that moment may or may not have
//! been answered by then; a long run also meets a varying number of the lease renewals and shard
//! syncs that run on timers. The instructions move by a fraction of a percent and the allocations
//! by a few blocks, so each scenario's limit sits at least a tenth of a percent above the highest
//! count observed, and below what one more allocation per message would reach.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

use std::convert::Infallible;
use std::env;
use std::future::Future;
use std::hint::black_box;
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_kinesis::Client as KinesisClient;
use aws_sdk_kinesis::client::Waiters as _;
use aws_sdk_kinesis::operation::create_stream::CreateStreamError;
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::PutRecordsRequestEntry;
use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use ruststream::runtime::{AppInfo, BrokerScope, Identity, RunningApp, RustStream};
use ruststream_kinesis::KinesisBroker;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;

// A benchmark measures what ships, and the framework's harness feature changes what a delivery
// does on its way to the handler.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench-code`"
);

/// The variable naming the stand, and where the stand is when nothing names it: the port
/// `docker-compose.test.yml` publishes.
const ENDPOINT_VAR: &str = "KINESIS_TEST_ENDPOINT";
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:4566";

/// The region the service and the side client are built for. The emulator answers for any.
const REGION: &str = "us-east-1";

/// The pause a shard reader takes after an empty read. A drain ends with one empty read, and the
/// pause is longer than the batch window, so a partial batch goes out before the reader asks
/// again.
pub const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The partition key every filled record carries. One shard takes every key, and a constant one
/// keeps a record's bytes constant too.
const PARTITION_KEY: &str = "bench";

/// Records one `PutRecords` call carries while the stream fills: the service's ceiling.
const FILL_CHUNK: usize = 500;

/// How many times the stand may refuse part of a fill before the run stops.
const FILL_ATTEMPTS: usize = 1_000;

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the crate and the framework rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run: large enough that entering and leaving the region is lost in the
/// per-message number, small enough that a scenario stays within minutes of valgrind time.
/// `scripts/bench_results.py` divides by the same count.
pub const MESSAGES: usize = 1_000;

/// The measurement configuration every gated scenario shares.
///
/// `steady` is what one delivery allocates in the steady state and `cold` what starting the
/// service and taking the first delivery allocate once; together they are the hard limit the
/// longest run of the scenario (twice [`MESSAGES`] deliveries) is held to, so the run
/// fails when the path allocates more than it does today. Both are floors the code is held to,
/// so a number that goes down is lowered here in the same change. The instruction limit is
/// relative: `just bench-code --save-baseline=main` records a baseline and
/// `just bench-code --baseline=main` compares against it.
pub fn config(steady: u64, cold: u64) -> LibraryBenchmarkConfig {
    config_every(steady, 1, cold)
}

/// The same for a scenario whose allocations do not come a whole number per delivery: `steady`
/// blocks per `per` deliveries, as a batch handler allocates per batch.
pub fn config_every(steady: u64, per: u64, cold: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        .pass_through_env(ENDPOINT_VAR)
        .tool(callgrind().soft_limits([(EventKind::Ir, 2f64)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks(steady, per, cold))]));
    config
}

/// The limit for the configured count: the cold part once, plus the steady rate over the longest
/// run of the scenario, which is twice [`MESSAGES`]. The division rounds up.
const fn blocks(steady: u64, per: u64, cold: u64) -> u64 {
    cold + (steady * 2 * MESSAGES as u64).div_ceil(per)
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION_FRAME}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    // `black_box` runs after the body returns, so the call cannot become a tail jump: DHAT
    // attributes an allocation to this region only while this frame is on the stack.
    black_box(body())
}

/// DHAT with a stack window deep enough to reach the measured frame from a publish inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION_FRAME.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION_FRAME: &str = "*common::measure*";

/// A single-threaded runtime: one thread means one order of execution, and the service runs only
/// while the body drives it.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

fn endpoint() -> String {
    env::var(ENDPOINT_VAR).unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned())
}

/// The stream this run reads, fresh for every process.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, so the
/// handler names this function and gets the stream the run created. The name has a fixed width,
/// so every request that carries it is the same size in every run.
pub fn stream() -> &'static str {
    static STREAM: OnceLock<String> = OnceLock::new();
    STREAM.get_or_init(|| {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|since| (since.as_secs() << 20) ^ u64::from(since.subsec_micros()))
            .unwrap_or_default();
        format!("rs-code-{:016x}", stamp ^ u64::from(process::id()))
    })
}

/// Runs `work` against the stand on a thread and a runtime of its own, and waits for it.
///
/// Everything a run does besides the service - creating the streams, filling one, removing it -
/// goes through here, so none of it runs on the thread the regions count, and none of it leaves
/// a task behind on the service's runtime.
fn aside<Work, Done, Out>(work: Work) -> Out
where
    Work: FnOnce(KinesisClient) -> Done + Send + 'static,
    Done: Future<Output = Out>,
    Out: Send + 'static,
{
    thread::spawn(move || {
        runtime().block_on(async move {
            let config = aws_config::defaults(BehaviorVersion::latest())
                .endpoint_url(endpoint())
                .region(Region::new(REGION))
                .test_credentials()
                .load()
                .await;
            work(KinesisClient::new(&config)).await
        })
    })
    .join()
    .expect("the side thread finishes")
}

/// Creates `stream` with one shard unless it exists, and waits until it is readable.
async fn ensure_stream(client: &KinesisClient, stream: &str) {
    let created = client
        .create_stream()
        .stream_name(stream)
        .shard_count(1)
        .send()
        .await;
    if let Err(err) = created {
        let exists = err
            .as_service_error()
            .is_some_and(CreateStreamError::is_resource_in_use_exception);
        assert!(exists, "the stand refuses the stream {stream}: {err}");
    }
    client
        .wait_until_stream_exists()
        .stream_name(stream)
        .wait(Duration::from_mins(2))
        .await
        .expect("the stream becomes active");
}

/// Puts `count` bodies on `stream` and returns once the stand has accepted every one.
///
/// A shard takes a limited number of records a second, and `PutRecords` reports a refused record
/// in its response rather than as an error, so the refused ones go again until none is left.
async fn fill(client: &KinesisClient, stream: &str, count: usize) {
    let body = json_body();
    let mut left = count;
    let mut refusals = 0;
    while left > 0 {
        let chunk = left.min(FILL_CHUNK);
        let entries = (0..chunk)
            .map(|_| {
                PutRecordsRequestEntry::builder()
                    .data(Blob::new(body.clone()))
                    .partition_key(PARTITION_KEY)
                    .build()
                    .expect("a record with data and a key")
            })
            .collect::<Vec<_>>();
        let accepted = client
            .put_records()
            .stream_name(stream)
            .set_records(Some(entries))
            .send()
            .await
            .map_or(0, |output| {
                let refused = output.failed_record_count().unwrap_or_default();
                chunk.saturating_sub(usize::try_from(refused).unwrap_or(chunk))
            });
        left -= accepted;
        if accepted < chunk {
            refusals += 1;
            assert!(
                refusals < FILL_ATTEMPTS,
                "the stand keeps refusing records on {stream}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state. What a delivery pays for it is one relaxed
/// decrement and the branch that reads it.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// The JSON body every record carries: the two fields a handler reads.
pub fn json_body() -> Vec<u8> {
    format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}").into_bytes()
}

/// A service that is built but not started.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that every scenario hands over the same
/// type; the one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<KinesisBroker, Identity, (), Latch>;

/// Builds a one-handler service on the production broker, with the run's stream and every stream
/// in `outputs` created on the stand, ready to be started by the body.
pub fn pending(
    messages: usize,
    outputs: &'static [&'static str],
    mount: impl FnOnce(Mount<'_>),
) -> Pending {
    let input = stream();
    aside(async move |client| {
        ensure_stream(&client, input).await;
        for output in outputs {
            ensure_stream(&client, output).await;
        }
    });
    let latch = Latch::default();
    let state = latch.clone();
    let broker = KinesisBroker::new()
        .endpoint(endpoint())
        .test_credentials()
        .region(REGION);
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker, mount);
    Pending {
        runtime: runtime(),
        latch,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the service starts")),
        messages,
    }
}

/// Starts the service, fills its stream, and drains it: the shape of every scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first is the start, the
/// second the deliveries.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        start,
        messages,
    } = pending;
    let input = stream();
    let running = measure(|| start(&runtime));
    latch.expect(messages);
    aside(async move |client| fill(&client, input, messages).await);
    measure(|| runtime.block_on(latch.drained()));
    black_box(&latch);
    drop(running);
    drop(runtime);
    aside(async move |client| {
        let _ = client.delete_stream().stream_name(input).send().await;
    });
}
