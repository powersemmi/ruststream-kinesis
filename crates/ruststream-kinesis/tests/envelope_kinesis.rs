//! The envelope seen from a service: a payload reaches the handler exactly as it was published,
//! whatever its first bytes are, and its headers arrive in every envelope that carries them.
//! Each body runs in process and against a local stack.
#![cfg(feature = "testing")]

use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ruststream::codec::{CborCodec, JsonCodec};
use ruststream::testing::TestApp;
use ruststream::{BytesMut, HeaderMap};
use ruststream_kinesis::envelope::{Body, EnvelopeError, EnvelopeHeaders};
use ruststream_kinesis::prelude::*;
use ruststream_kinesis::{Envelope, Framed, NoOp, Rsk};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::live;

/// The stream the test publishes to.
const FRAMES: &str = "frames.wire";

/// The stream the service forwards every frame to.
const ARCHIVE: &str = "frames.archive";

/// Payloads that begin like the envelope this crate writes and like the one earlier releases
/// wrote, each followed by a header block that parses. Read as envelopes they would lose their
/// first bytes, and the second would also invent a header.
const LOOKALIKES: [&[u8]; 2] = [b"RSK2\0\0\0\0frame", b"RSK1\0\0\0\x05a: b\nframe"];

/// A frame is the payload as the record carries it.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// The frame the service forwards, written as it was read.
#[derive(Outgoing, Serialized)]
struct Archived(Vec<u8>);

#[subscriber(
    KinesisStream::new(FRAMES)
        .create_if_missing(1)
        .poll_interval(Duration::from_millis(200)),
    reply(ARCHIVE)
)]
async fn forward(frame: &Frame<'_>) -> Archived {
    Archived(frame.0.to_vec())
}

#[subscriber(
    KinesisStream::new(ARCHIVE)
        .create_if_missing(1)
        .poll_interval(Duration::from_millis(200))
)]
async fn archive(_frame: &Frame<'_>) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// The app `main` runs, on the broker it is handed. Both subscriptions start at the moment the
/// app is built, so records a previous run left on the stack are not read.
fn app(broker: KinesisBroker) -> RustStream {
    let opened = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past the epoch");
    let opened = u64::try_from(opened.as_millis()).expect("the instant fits in milliseconds");
    RustStream::new(AppInfo::new("frames", "0.1.0")).with_broker(broker, |b| {
        b.include(forward.start_at(KinesisPosition::timestamp(opened)));
        b.include(archive.start_at(KinesisPosition::timestamp(opened)));
    })
}

/// The body both modes run: a test publish without headers, then the service's own reply
/// without headers, each read back byte for byte.
async fn a_payload_that_looks_like_an_envelope_arrives_unchanged(tb: TestApp<()>) {
    let broker = tb.broker::<KinesisBroker>();
    for payload in LOOKALIKES {
        broker
            .message(&Archived(payload.to_vec()))
            .to(FRAMES)
            .publish()
            .await
            .expect("publish drives the frame and its reply to their settlement");
    }

    let expected: Vec<&[u8]> = LOOKALIKES.to_vec();
    for stream in [FRAMES, ARCHIVE] {
        let received = broker.subscriber(stream).assert_called(2).received_raw();
        assert_eq!(
            received.iter().map(AsRef::as_ref).collect::<Vec<&[u8]>>(),
            expected,
            "on {stream}"
        );
    }

    tb.shutdown().await.expect("the harness shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_payload_that_looks_like_an_envelope_arrives_unchanged_in_process() {
    let tb = TestApp::start(app(KinesisBroker::new()))
        .await
        .expect("the harness starts");
    a_payload_that_looks_like_an_envelope_arrives_unchanged(tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_payload_that_looks_like_an_envelope_arrives_unchanged_live() {
    let Some(endpoint) = live::url("KINESIS_TEST_ENDPOINT") else {
        return;
    };
    let broker = KinesisBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1");
    let tb = TestApp::start_live(app(broker))
        .await
        .expect("the harness starts against the stand");
    a_payload_that_looks_like_an_envelope_arrives_unchanged(tb).await;
}

/// The header a producer stamps on an order.
const TENANT: &str = "x-tenant";

#[derive(Debug, Clone, PartialEq, Eq, Outgoing, Serialize, Deserialize, JsonSchema)]
struct Order {
    id: u64,
}

/// What the service saw: the order and the tenant header, when the envelope carried one.
#[derive(Debug, Clone, PartialEq, Eq, Outgoing, Serialize, Deserialize, JsonSchema)]
struct Receipt {
    id: u64,
    tenant: Option<String>,
}

/// Answers an order with what it saw of it.
struct Stamp;

impl Handle<Order, Receipt> for Stamp {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt {
            id: order.id,
            tenant: ctx.headers().get_str(TENANT).map(str::to_owned),
        }))
    }
}

/// Files a receipt.
struct File;

impl Handle<Receipt> for File {
    fn handle(
        &self,
        _receipt: &Receipt,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

/// The two streams of one round trip. Each envelope has its own, so a live run never reads a
/// record another envelope's run left on the stack.
#[derive(Debug, Clone, Copy)]
struct Streams {
    orders: &'static str,
    receipts: &'static str,
}

const DEFAULT_STREAMS: Streams = Streams {
    orders: "envelope.rsk.orders",
    receipts: "envelope.rsk.receipts",
};

const JSON_STREAMS: Streams = Streams {
    orders: "envelope.json.orders",
    receipts: "envelope.json.receipts",
};

const CBOR_STREAMS: Streams = Streams {
    orders: "envelope.cbor.orders",
    receipts: "envelope.cbor.receipts",
};

const RAW_STREAMS: Streams = Streams {
    orders: "envelope.raw.orders",
    receipts: "envelope.raw.receipts",
};

/// A stream the run creates when the stack does not have it, read often enough for a test.
fn stream(name: &'static str) -> KinesisStream {
    KinesisStream::new(name)
        .create_if_missing(1)
        .poll_interval(Duration::from_millis(200))
}

/// The milliseconds since the epoch at which an app is built: its subscriptions start there, so
/// records a previous run left on the stack are not read.
fn now_millis() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past the epoch");
    u64::try_from(now.as_millis()).expect("the instant fits in milliseconds")
}

/// The service, on a broker in whichever envelope it is given: the order and its receipt both
/// travel in it.
fn orders<Env: Envelope>(broker: KinesisBroker<Env>, streams: Streams) -> RustStream {
    let opened = KinesisPosition::timestamp(now_millis());
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(stream(streams.orders), Stamp)
                .reply()
                .to(streams.receipts)
                .build()
                .start_at(opened.clone()),
        );
        b.include(
            subscriber(stream(streams.receipts), File)
                .build()
                .start_at(opened),
        );
    })
}

/// The body every envelope runs: an order published with the tenant header, when `tenant` is
/// given, reaches the handler with it, and the receipt reaches its own subscription.
async fn an_order_and_its_receipt_round_trip<Env: Envelope>(
    tb: TestApp<()>,
    streams: Streams,
    tenant: Option<&str>,
) {
    let broker = tb.broker::<KinesisBroker<Env>>();
    let mut headers = HeaderMap::new();
    if let Some(tenant) = tenant {
        headers.insert(TENANT, tenant.to_owned());
    }
    broker
        .message(&Order { id: 7 })
        .to(streams.orders)
        .with_headers(headers)
        .publish()
        .await
        .expect("publish drives the order and its receipt to their settlement");

    broker
        .subscriber(streams.receipts)
        .assert_called(1)
        .with(&Receipt {
            id: 7,
            tenant: tenant.map(str::to_owned),
        });

    tb.shutdown().await.expect("the harness shuts down");
}

/// The broker of a live run, in `envelope`, or `None` to skip the test.
fn live_broker<Env: Envelope>(envelope: Env) -> Option<KinesisBroker<Env>> {
    let endpoint = live::url("KINESIS_TEST_ENDPOINT")?;
    Some(
        KinesisBroker::new()
            .endpoint(endpoint)
            .test_credentials()
            .region("us-east-1")
            .envelope(envelope),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_round_trips_in_the_default_envelope_in_process() {
    let tb = TestApp::start(orders(KinesisBroker::new(), DEFAULT_STREAMS))
        .await
        .expect("the harness starts");
    an_order_and_its_receipt_round_trip::<Rsk>(tb, DEFAULT_STREAMS, Some("acme")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_round_trips_in_the_default_envelope_live() {
    let Some(broker) = live_broker(Rsk) else {
        return;
    };
    let tb = TestApp::start_live(orders(broker, DEFAULT_STREAMS))
        .await
        .expect("the harness starts");
    an_order_and_its_receipt_round_trip::<Rsk>(tb, DEFAULT_STREAMS, Some("acme")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_round_trips_in_a_json_document_in_process() {
    let broker = KinesisBroker::new().envelope(Framed::new(JsonCodec));
    let tb = TestApp::start(orders(broker, JSON_STREAMS))
        .await
        .expect("the harness starts");
    an_order_and_its_receipt_round_trip::<Framed<JsonCodec>>(tb, JSON_STREAMS, Some("acme")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_round_trips_in_a_json_document_live() {
    let Some(broker) = live_broker(Framed::new(JsonCodec)) else {
        return;
    };
    let tb = TestApp::start_live(orders(broker, JSON_STREAMS))
        .await
        .expect("the harness starts");
    an_order_and_its_receipt_round_trip::<Framed<JsonCodec>>(tb, JSON_STREAMS, Some("acme")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_round_trips_in_a_cbor_document_in_process() {
    let broker = KinesisBroker::new().envelope(Framed::new(CborCodec));
    let tb = TestApp::start(orders(broker, CBOR_STREAMS))
        .await
        .expect("the harness starts");
    an_order_and_its_receipt_round_trip::<Framed<CborCodec>>(tb, CBOR_STREAMS, Some("acme")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_payload_round_trips_with_no_envelope_in_process() {
    let broker = KinesisBroker::new().envelope(NoOp);
    let tb = TestApp::start(orders(broker, RAW_STREAMS))
        .await
        .expect("the harness starts");
    an_order_and_its_receipt_round_trip::<NoOp>(tb, RAW_STREAMS, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_payload_round_trips_with_no_envelope_live() {
    let Some(broker) = live_broker(NoOp) else {
        return;
    };
    let tb = TestApp::start_live(orders(broker, RAW_STREAMS))
        .await
        .expect("the harness starts");
    an_order_and_its_receipt_round_trip::<NoOp>(tb, RAW_STREAMS, None).await;
}

/// The stream a producer outside the service writes plain JSON to.
const FOREIGN: &str = "envelope.foreign";

/// Reads in a JSON document the stream's producer does not write.
#[subscriber(
    KinesisStream::new(FOREIGN)
        .create_if_missing(1)
        .poll_interval(Duration::from_millis(200))
)]
async fn foreign(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    if order.id == 7 && ctx.headers().get(TENANT).is_none() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

fn foreign_reader(broker: KinesisBroker) -> RustStream {
    let opened = now_millis();
    RustStream::new(AppInfo::new("foreign", "0.1.0")).with_broker(broker, |b| {
        b.include(
            foreign
                .envelope(Framed::new(JsonCodec))
                .start_at(KinesisPosition::timestamp(opened)),
        );
    })
}

/// A record that is not a frame - here the plain JSON the default envelope writes for a record
/// without headers - is the payload, with no headers.
async fn a_record_that_is_no_frame_is_read_as_the_payload(tb: TestApp<()>) {
    let broker = tb.broker::<KinesisBroker>();
    broker
        .message(&Order { id: 7 })
        .to(FOREIGN)
        .publish()
        .await
        .expect("publish drives the record to its settlement");

    broker
        .subscriber(FOREIGN)
        .assert_called(1)
        .with(&Order { id: 7 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("the harness shuts down");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_that_is_no_frame_is_read_as_the_payload_in_process() {
    let tb = TestApp::start(foreign_reader(KinesisBroker::new()))
        .await
        .expect("the harness starts");
    a_record_that_is_no_frame_is_read_as_the_payload(tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_that_is_no_frame_is_read_as_the_payload_live() {
    let Some(broker) = live_broker(Rsk) else {
        return;
    };
    let tb = TestApp::start_live(foreign_reader(broker))
        .await
        .expect("the harness starts");
    a_record_that_is_no_frame_is_read_as_the_payload(tb).await;
}

/// The stream a raw service reads its orders from.
const RAW_ORDERS: &str = "envelope.audit.orders";

/// The stream a raw service writes its audit trail to.
const AUDIT: &str = "envelope.audit";

#[derive(Debug, Clone, PartialEq, Eq, Outgoing, Serialize, Deserialize, JsonSchema)]
struct Audited {
    id: u64,
}

/// The slot the audit entries leave through.
#[derive(OutSlot)]
#[publishes(Audited)]
struct Audit;

/// Tries to stamp a header on a record of a stream that carries none, and settles by whether
/// the publish went through.
#[subscriber(KinesisStream::new(RAW_ORDERS))]
async fn audit(order: &Order, Out(trail): Out<impl Publisher, Audit>) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert(TENANT, "acme");
    let sent = trail
        .message(&Audited { id: order.id })
        .to(AUDIT)
        .with_headers(headers)
        .publish()
        .await;
    if sent.is_ok() {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_on_a_stream_without_an_envelope_fails_the_publish() {
    let app = RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(
        KinesisBroker::new().envelope(NoOp),
        |b| {
            b.include(audit).out(Audit, Publish::default()).build();
        },
    );
    let tb = TestApp::start(app).await.expect("the harness starts");
    let broker = tb.broker::<KinesisBroker<NoOp>>();

    broker
        .message(&Order { id: 7 })
        .to(RAW_ORDERS)
        .publish()
        .await
        .expect("publish drives the order to its settlement");

    broker
        .subscriber(RAW_ORDERS)
        .assert_called(1)
        .settled(HandlerOutcome::drop());
    broker.published::<Audited>(AUDIT).assert_not_called();
    tb.shutdown().await.expect("the harness shuts down");
}

/// The default envelope, counting how often a delivery's headers are read.
#[derive(Debug, Clone, Default)]
struct Counted {
    reads: Arc<AtomicUsize>,
}

impl Envelope for Counted {
    const NAME: &'static str = "counted";

    fn seal(
        &self,
        headers: EnvelopeHeaders<'_>,
        payload: BytesMut,
    ) -> Result<Vec<u8>, EnvelopeError> {
        Rsk.seal(headers, payload)
    }

    fn body(&self, record: &[u8]) -> Body {
        Rsk.body(record)
    }

    fn read_headers(&self, record: &[u8], headers: &mut HeaderMap) {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Rsk.read_headers(record, headers);
    }
}

/// Reads the payload alone.
struct Plain;

impl Handle<Order> for Plain {
    fn handle(
        &self,
        _order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

/// Reads the tenant header, twice.
struct Tenanted;

impl Handle<Order> for Tenanted {
    fn handle(
        &self,
        _order: &Order,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let first = ctx.headers().get_str(TENANT).map(str::to_owned);
        let again = ctx.headers().get_str(TENANT).map(str::to_owned);
        ready(if first.is_some() && first == again {
            Ok(())
        } else {
            Err(HandlerOutcome::drop())
        })
    }
}

/// A subscription per handler, each reading through its own counting envelope.
fn counted(plain: Counted, tenanted: Counted) -> RustStream {
    RustStream::new(AppInfo::new("lazy", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(subscriber(KinesisStream::new("lazy.plain").envelope(plain), Plain).build());
        b.include(
            subscriber(
                KinesisStream::new("lazy.tenanted").envelope(tenanted),
                Tenanted,
            )
            .build(),
        );
    })
}

/// A handler that reads no header never reads the envelope's header block; one that reads them
/// reads the block once, however often it asks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_are_read_only_when_asked_for_and_once() {
    let (plain, tenanted) = (Counted::default(), Counted::default());
    let tb = TestApp::start(counted(plain.clone(), tenanted.clone()))
        .await
        .expect("the harness starts");
    let broker = tb.broker::<KinesisBroker>();
    for stream in ["lazy.plain", "lazy.tenanted"] {
        let mut headers = HeaderMap::new();
        headers.insert(TENANT, "acme");
        broker
            .message(&Order { id: 7 })
            .to(stream)
            .with_headers(headers)
            .publish()
            .await
            .expect("publish drives the order to its settlement");
    }

    broker
        .subscriber("lazy.plain")
        .assert_called(1)
        .settled(HandlerOutcome::ack());
    broker
        .subscriber("lazy.tenanted")
        .assert_called(1)
        .settled(HandlerOutcome::ack());
    assert_eq!(
        plain.reads.load(Ordering::SeqCst),
        0,
        "the payload alone was read"
    );
    assert_eq!(
        tenanted.reads.load(Ordering::SeqCst),
        1,
        "the headers were read once"
    );
    tb.shutdown().await.expect("the harness shuts down");
}
