//! The broker ladder: [`KinesisBroker`] -> [`ConnectedKinesisBroker`].
//!
//! Construction is synchronous and I/O-free; credential resolution happens in the consuming
//! [`Broker::connect`], and the connected form holds the live SDK client directly. One shared
//! cell remains so publishers can be handed out while the application is still being
//! assembled, before `connect` runs.

// Without the `testing` feature a connection link has one variant, so a `match` on it has a
// single arm; the matches stay so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use std::future::{Future, ready};
use std::hash::{BuildHasher, Hasher, RandomState};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_kinesis::client::Waiters;
#[cfg(all(feature = "testing", feature = "cbor"))]
use ruststream::codec::CborCodec;
#[cfg(all(feature = "testing", feature = "json"))]
use ruststream::codec::JsonCodec;
#[cfg(all(feature = "testing", feature = "msgpack"))]
use ruststream::codec::MsgpackCodec;
#[cfg(feature = "testing")]
use ruststream::testing::{Coordinator, InProcess, TestableBroker};
use ruststream::{AddressedCopies, Broker, ConnectedBroker, DefaultPublish, Subscribe};
#[cfg(feature = "testing")]
use ruststream::{OutgoingMessage, RawMessage};
use tokio::runtime::Handle;
use tokio::sync::OnceCell;

use crate::claim::ReadStreams;
use crate::clients::{Overrides, RuntimeClients};
#[cfg(all(
    feature = "testing",
    any(feature = "json", feature = "cbor", feature = "msgpack")
))]
use crate::envelope::Framed;
#[cfg(feature = "testing")]
use crate::envelope::NoOp;
use crate::envelope::{ChooseEnvelope, Envelope, Rsk};
use crate::error::{KinesisError, sdk_err};
#[cfg(feature = "testing")]
use crate::in_process::Bus;
use crate::lease::{LeaseStore, MemoryLeaseStore};
use crate::publisher::{KinesisPublish, KinesisPublisher};
use crate::stream::KinesisStream;
use crate::subscriber::KinesisSubscriber;

mod describe;

/// The live client state shared by the connected form and every handle derived from it.
///
/// Why runtime checks exist here at all: the SDK client has no shutdown and keeps working
/// forever, and publishers may be handed out before `connect` and outlive `shutdown`
/// (aliasing) - so the closed state is an explicit flag a stale handle trips over instead of
/// silently succeeding.
pub(crate) struct Core {
    /// The client of whichever runtime a request runs on: the one built at connect on the runtime
    /// `connect` ran on, a thread's own anywhere else.
    pub(crate) client: RuntimeClients<aws_sdk_kinesis::Client>,
    pub(crate) store: Arc<dyn LeaseStore>,
    pub(crate) owner: String,
    pub(crate) closed: AtomicBool,
    /// The runtime `connect` ran on, which every task the broker starts runs on: a subscription
    /// opened from a handler's own thread would otherwise read on that thread's runtime, behind
    /// its computation, and stop reading when that runtime stops.
    pub(crate) runtime: Handle,
}

impl Core {
    pub(crate) fn ensure_open(&self) -> Result<(), KinesisError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(KinesisError::NotConnected);
        }
        Ok(())
    }
}

impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Core")
            .field("owner", &self.owner)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// What a connected broker and every handle derived from it speak over: the live SDK client, or,
/// under the `testing` feature, the in-process transport the test harness connected instead.
///
/// Without the feature there is one variant, so the type is the client state itself and every
/// `match` on it is irrefutable: a production build carries no second transport and no branch to
/// it.
#[derive(Debug, Clone)]
pub(crate) enum Link {
    Aws(Arc<Core>),
    #[cfg(feature = "testing")]
    InProcess(Arc<Bus>),
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives the
// link exactly the size of the client state it wraps.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Link>() == size_of::<Arc<Core>>());

pub(crate) type CoreCell = Arc<OnceCell<Link>>;

/// An Amazon Kinesis Data Streams broker for the `RustStream` messaging framework.
///
/// `new` is synchronous and records only configuration; the runtime resolves credentials and
/// builds the client once at startup via the consuming [`Broker::connect`]. That is what lets
/// a service compose with the synchronous `#[ruststream::app]` builder.
///
/// TLS comes from the SDK's default HTTPS client. Each runtime keeps its own connections: requests
/// from the runtime the broker connected on share the client built at connect, and a handler on a
/// dedicated thread publishes and checkpoints through a client built for that thread from the same
/// config, so a busy thread never holds up another's request.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_kinesis::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(KinesisStream::new("orders"))]
/// async fn handle(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "accepted");
///     HandlerOutcome::ack()
/// }
///
/// /// Against a local stack. A production service writes `KinesisBroker::new()` alone, and the
/// /// region and the credentials resolve from the environment when the runtime connects.
/// #[ruststream::app]
/// fn app() -> impl App {
///     let broker = KinesisBroker::new()
///         .endpoint("http://localhost:4566")
///         .test_credentials()
///         .region("us-east-1");
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
///         b.include(handle);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Clone)]
#[must_use]
pub struct KinesisBroker<Env = Rsk> {
    endpoint: Option<String>,
    region: Option<String>,
    test_credentials: bool,
    sdk_config: Option<SdkConfig>,
    store: Option<Arc<dyn LeaseStore>>,
    owner: Option<String>,
    cell: CoreCell,
    envelope: Env,
}

/// A broker in the default layout, [`Rsk`]. Only this form has a default, so
/// `KinesisBroker::default()` infers it.
impl Default for KinesisBroker {
    fn default() -> Self {
        Self {
            endpoint: None,
            region: None,
            test_credentials: false,
            sdk_config: None,
            store: None,
            owner: None,
            cell: CoreCell::default(),
            envelope: Rsk,
        }
    }
}

impl<Env> std::fmt::Debug for KinesisBroker<Env> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisBroker")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl KinesisBroker {
    /// Records configuration only; region and credentials resolve from the environment on
    /// `connect`. No I/O.
    pub fn new() -> Self {
        Self::default()
    }

    /// Uses an already built AWS config instead of resolving one from the environment.
    ///
    /// The broker keeps the config's settings, its credentials and timeouts included. An
    /// [`endpoint`](Self::endpoint) or a [`region`](Self::region) named on the broker replaces the
    /// config's own; what the broker does not name stays as the config has it.
    /// [`test_credentials`](Self::test_credentials) applies to a broker built with
    /// [`new`](Self::new): a prebuilt config signs with its own credentials provider.
    pub fn from_config(config: SdkConfig) -> Self {
        Self {
            sdk_config: Some(config),
            ..Self::default()
        }
    }
}

impl<Env: Envelope> KinesisBroker<Env> {
    /// Writes and reads records in `envelope`'s layout, wherever a descriptor or a publish policy
    /// names none of its own.
    ///
    /// The default is [`Rsk`], the layout every release of this crate reads. A service whose
    /// records are read by another system picks the layout that system reads:
    /// [`Framed`](crate::Framed) for a codec document, [`NoOp`](crate::NoOp) for the raw payload.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::codec::JsonCodec;
    /// use ruststream_kinesis::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber(KinesisStream::new("orders"))]
    /// async fn on_order(order: &Order) -> HandlerOutcome {
    ///     tracing::info!(order.id, "order");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// /// Every record this service writes and reads is a JSON document of headers and payload.
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     let broker = KinesisBroker::new().envelope(Framed::new(JsonCodec));
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
    ///         b.include(on_order);
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn envelope<Named: Envelope>(self, envelope: Named) -> KinesisBroker<Named> {
        KinesisBroker {
            endpoint: self.endpoint,
            region: self.region,
            test_credentials: self.test_credentials,
            sdk_config: self.sdk_config,
            store: self.store,
            owner: self.owner,
            cell: self.cell,
            envelope,
        }
    }

    /// Overrides the service endpoint (a local stack for development), the one of a config given
    /// to [`from_config`](Self::from_config) included.
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Overrides the region, the one of a config given to [`from_config`](Self::from_config)
    /// included.
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Uses dummy static credentials, for local stacks that require credentials to be
    /// present but ignore their values. Applies to a broker built with [`new`](Self::new); a
    /// [`from_config`](Self::from_config) broker uses its config's credentials.
    pub fn test_credentials(mut self) -> Self {
        self.test_credentials = true;
        self
    }

    /// Plugs in a lease store so multiple service instances share the shards. Defaults to
    /// the in-process [`MemoryLeaseStore`], which is correct for a single instance.
    pub fn lease_store(mut self, store: Arc<dyn LeaseStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Overrides this instance's lease-owner id.
    ///
    /// The default is `rs-<host>-<pid>-<random>`, unique per broker across processes, hosts and
    /// restarts. A fixed id names an instance in the lease table; two live instances must not
    /// share one, or both hold every shard. A store grants an owner the leases it already holds,
    /// so an instance that restarts under the same fixed id takes its shards back at once.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "dynamodb-lease")]
    /// # mod demo {
    /// use std::error::Error;
    /// use std::sync::Arc;
    ///
    /// use aws_config::BehaviorVersion;
    /// use ruststream_kinesis::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber(KinesisStream::new("orders"))]
    /// async fn handle(order: &Order) -> HandlerOutcome {
    ///     tracing::info!(order.id, "accepted");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// // The lease store needs the AWS config too, so the service resolves it and hands the broker
    /// // the same one.
    /// pub async fn run() -> Result<(), Box<dyn Error>> {
    ///     let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    ///     let leases = Arc::new(DynamoLeaseStore::new(&config, "orders-leases"));
    ///     let broker = KinesisBroker::from_config(config)
    ///         .lease_store(leases)
    ///         // The name this instance holds its shards under in the lease table.
    ///         .owner_id("instance-a");
    ///
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .with_broker(broker, |b| {
    ///             b.include(handle);
    ///         })
    ///         .run()
    ///         .await?;
    ///     Ok(())
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn owner_id(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    /// A publisher sharing this broker's connection cell; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> KinesisPublisher<Env> {
        KinesisPublisher::new(Arc::clone(&self.cell), self.envelope.clone())
    }
}

fn default_owner() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    owner_on(
        std::env::var("HOSTNAME").ok().as_deref(),
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
    )
}

/// The owner id a broker takes from what its process knows about itself: the host name, the
/// process id and the broker's place in the process.
///
/// None of these tells two hosts apart (replicas in containers all run as PID 1, and the host name
/// may be missing or reused), so the id ends in 64 random bits. They come from std's hasher keys,
/// which the OS seeds; the id is drawn once per broker, at connect.
fn owner_on(host: Option<&str>, pid: u32, seq: u64) -> String {
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(seq);
    let nonce = hasher.finish();
    match host {
        Some(host) if !host.is_empty() => format!("rs-{host}-{pid}-{nonce:016x}"),
        _ => format!("rs-{pid}-{nonce:016x}"),
    }
}

impl<Env: Envelope> Broker for KinesisBroker<Env> {
    type Error = KinesisError;
    type Connected = ConnectedKinesisBroker<Env>;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let link = self
            .cell
            .get_or_try_init(async || {
                let config = if let Some(config) = self.sdk_config.clone() {
                    config
                } else {
                    // BehaviorVersion::latest(): every pinned version eventually deprecates
                    // (which -D warnings turns into a build failure); a consumer who needs a
                    // frozen behaviour passes a prebuilt config via from_config.
                    let mut loader = aws_config::defaults(BehaviorVersion::latest());
                    if let Some(endpoint) = &self.endpoint {
                        loader = loader.endpoint_url(endpoint.clone());
                    }
                    if let Some(region) = &self.region {
                        loader = loader.region(Region::new(region.clone()));
                    }
                    if self.test_credentials {
                        loader = loader.test_credentials();
                    }
                    loader.load().await
                };
                let overrides = Overrides {
                    endpoint: self.endpoint.clone(),
                    region: self.region.clone().map(Region::new),
                };
                let runtime = Handle::current();
                Ok::<_, KinesisError>(Link::Aws(Arc::new(Core {
                    client: RuntimeClients::homed(config, overrides, &runtime),
                    store: self
                        .store
                        .clone()
                        .unwrap_or_else(|| Arc::new(MemoryLeaseStore::new())),
                    owner: self.owner.clone().unwrap_or_else(default_owner),
                    closed: AtomicBool::new(false),
                    runtime,
                })))
            })
            .await?
            .clone();
        Ok(ConnectedKinesisBroker {
            link,
            cell: self.cell,
            streams: Arc::default(),
            envelope: self.envelope,
        })
    }
}

/// The in-process mode: the connected form a test runs the production app against, carrying the
/// in-process transport in place of the SDK client.
///
/// The transport is written into the broker's own connection cell, so a publisher handed out
/// before the harness connected ([`KinesisBroker::publisher`]) publishes into it, exactly as it
/// publishes through the client once `connect` has run. Nothing here resolves credentials or
/// dials an endpoint, so the transition cannot fail.
///
/// The transport keeps one retained log per stream, with one shard, so every record is ordered
/// against every other. A record is written the way the publisher writes it for the service and
/// read back by the same decoding, so a delivery carries the position, the headers and the
/// context a record from a shard carries. It refuses what the service refuses: a stream name the
/// service does not accept, an empty partition key or one longer than 256 characters, and a
/// record over 10 MiB (data plus partition key), the most a stream can be configured to take. A
/// stream takes 1 MiB unless its owner raised it. A handle used after `shutdown` reports
/// [`KinesisError::NotConnected`].
///
/// A subscription opens at the tip, and `start_at(..)` and a handler's
/// [`SeekHandle`](crate::SeekHandle) read the retained log again from the position they name. A
/// record a handler leaves unhandled does not come back here: the service reads it again when
/// the shard's lease is next taken, and leases, checkpoints, retention and resharding belong to
/// the live mode, `TestApp::start_live`, over the same test body.
///
/// # Examples
///
/// A test runs the app `main` runs, and addresses the broker by its production type:
///
/// ```
/// # mod demo {
/// use std::error::Error;
///
/// use ruststream::testing::TestApp;
/// use ruststream_kinesis::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
/// #[outgoing(name = "orders")]
/// pub struct Order {
///     pub id: u64,
/// }
///
/// #[subscriber(KinesisStream::new("orders"))]
/// async fn handle(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "accepted");
///     HandlerOutcome::ack()
/// }
///
/// /// The app `main` runs.
/// pub fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
///         b.include(handle);
///     })
/// }
///
/// pub async fn accepts_an_order() -> Result<(), Box<dyn Error>> {
///     let tb = TestApp::start(app()).await?;
///
///     // The publish returns once the handler it woke has settled.
///     tb.broker::<KinesisBroker>()
///         .message(&Order { id: 7 })
///         .publish()
///         .await?;
///
///     tb.broker::<KinesisBroker>()
///         .subscriber("orders")
///         .assert_called_once()
///         .with(&Order { id: 7 })
///         .settled(HandlerOutcome::ack());
///
///     tb.shutdown().await?;
///     Ok(())
/// }
/// # }
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// #     tokio::runtime::Builder::new_multi_thread()
/// #         .enable_all()
/// #         .build()?
/// #         .block_on(demo::accepts_an_order())
/// # }
/// ```
#[cfg(feature = "testing")]
impl<Env: Envelope> InProcess for KinesisBroker<Env> {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        let link = self
            .cell
            .get_or_init(async || Link::InProcess(Bus::new()))
            .await
            .clone();
        Ok(ConnectedKinesisBroker {
            link,
            cell: self.cell,
            streams: Arc::default(),
            envelope: self.envelope,
        })
    }
}

#[cfg(feature = "testing")]
ruststream::register_testable_broker!(KinesisBroker);
#[cfg(feature = "testing")]
ruststream::register_testable_broker!(KinesisBroker<NoOp>);
#[cfg(all(feature = "testing", feature = "json"))]
ruststream::register_testable_broker!(KinesisBroker<Framed<JsonCodec>>);
#[cfg(all(feature = "testing", feature = "cbor"))]
ruststream::register_testable_broker!(KinesisBroker<Framed<CborCodec>>);
#[cfg(all(feature = "testing", feature = "msgpack"))]
ruststream::register_testable_broker!(KinesisBroker<Framed<MsgpackCodec>>);

/// The typed witness that `connect` succeeded: holds the live SDK client directly.
#[derive(Debug)]
pub struct ConnectedKinesisBroker<Env = Rsk> {
    link: Link,
    // Keeps the cell of publishers handed out before connect alive and filled.
    cell: CoreCell,
    /// The streams this broker has an open subscription to: it reads each of them once.
    streams: Arc<ReadStreams>,
    /// The broker's envelope, for the descriptors and policies that name none.
    envelope: Env,
}

impl<Env: Envelope> ConnectedKinesisBroker<Env> {
    /// A publisher from the connected form. It rides the same cell-backed publisher type as
    /// the early path; by now `connect` has filled the cell, so it resolves immediately.
    #[must_use]
    pub fn publisher(&self) -> KinesisPublisher<Env> {
        KinesisPublisher::new(Arc::clone(&self.cell), self.envelope.clone())
    }

    /// A publisher carrying the partition key and the envelope a mount site named on its policy.
    pub(crate) fn publisher_keyed<Named: ChooseEnvelope<Env>>(
        &self,
        partition_key: Option<Arc<str>>,
        envelope: Named,
    ) -> KinesisPublisher<Named::Chosen> {
        KinesisPublisher::keyed(
            Arc::clone(&self.cell),
            partition_key,
            envelope.choose(&self.envelope),
        )
    }

    /// Opens the subscription described by `descriptor`, returning once it reads: every shard it
    /// took has its reader and its starting cursor, so a record published after this returns is
    /// read, and a seek reaches the reader of the shard it names.
    ///
    /// # Errors
    ///
    /// Returns [`KinesisError`] when the descriptor is invalid, the stream does not exist and
    /// the descriptor does not create it, stream creation (when opted in) fails, or the broker
    /// is shut down. Returns [`KinesisError::AlreadySubscribed`] when this broker already has an
    /// open subscription to the stream: its shards are leased under the broker's one owner id,
    /// so a second subscription would share the first one's leases and checkpoints.
    pub async fn subscribe_stream<Named: ChooseEnvelope<Env>>(
        &self,
        descriptor: KinesisStream<Named>,
    ) -> Result<KinesisSubscriber<Named::Chosen>, KinesisError> {
        let (descriptor, named) = descriptor.split();
        let envelope = named.choose(&self.envelope);
        descriptor.validate()?;
        let claim = self.streams.claim(descriptor.stream())?;
        let core = match &self.link {
            Link::Aws(core) => core,
            #[cfg(feature = "testing")]
            Link::InProcess(bus) => return bus.subscribe(&descriptor, claim, envelope),
        };
        core.ensure_open()?;
        if let Some(shards) = descriptor.create_value() {
            Self::ensure_stream(core, descriptor.stream(), shards).await?;
        } else {
            Self::require_stream(core, descriptor.stream()).await?;
        }
        Ok(KinesisSubscriber::open(core, descriptor, claim, envelope).await)
    }

    /// Refuses a stream the service does not have, before the subscription exists.
    ///
    /// The shards are listed by the subscription's coordinator, which runs detached, so a
    /// mistyped stream name would otherwise start the service and report itself once per shard
    /// sync for as long as it runs. One describe per subscription, at startup, is what turns
    /// that into a failure the caller sees.
    async fn require_stream(core: &Core, stream: &str) -> Result<(), KinesisError> {
        core.client
            .with(|client| client.describe_stream_summary().stream_name(stream))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| KinesisError::Stream {
                stream: stream.to_owned(),
                source: sdk_err(&e),
            })
    }

    /// Creates the stream when missing and waits until it is active.
    async fn ensure_stream(core: &Core, stream: &str, shards: i32) -> Result<(), KinesisError> {
        let exists = core
            .client
            .with(|client| client.describe_stream_summary().stream_name(stream))
            .send()
            .await
            .is_ok();
        if !exists {
            let created = core
                .client
                .with(|client| {
                    client
                        .create_stream()
                        .stream_name(stream)
                        .shard_count(shards)
                })
                .send()
                .await;
            if let Err(err) = created {
                // A lost creation race is fine; anything else is not.
                let raced = err
                    .as_service_error()
                    .is_some_and(|e| e.to_string().contains("ResourceInUse"));
                if !raced {
                    return Err(KinesisError::Stream {
                        stream: stream.to_owned(),
                        source: sdk_err(&err),
                    });
                }
            }
        }
        core.client
            .with(|client| client.wait_until_stream_exists().stream_name(stream))
            .wait(Duration::from_mins(1))
            .await
            .map_err(|e| KinesisError::Stream {
                stream: stream.to_owned(),
                source: Box::new(e),
            })?;
        Ok(())
    }
}

impl<Env: Envelope> ConnectedBroker for ConnectedKinesisBroker<Env> {
    type Error = KinesisError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        match self.link {
            // The SDK client has no close; the closed flag stops readers and stale handles, and
            // leases lapse or are released by the readers as they exit.
            Link::Aws(core) => core.closed.store(true, Ordering::Release),
            #[cfg(feature = "testing")]
            Link::InProcess(bus) => bus.close(),
        }
        ready(Ok(()))
    }
}

impl<Env: Envelope> Subscribe for ConnectedKinesisBroker<Env> {
    type Subscriber = KinesisSubscriber<Env>;
    /// A stream is both what a subscription reads and what a publish writes to, so a bare name
    /// is the address of its own copies: the framework publishes the deferred copy of a
    /// `retry_after`, and a spent delivery, back under the name the subscription opened.
    ///
    /// A copy reaches the subscription from the tip, so a service that defers a record gets it
    /// back at the end of the stream rather than in place. Ordering against the records already
    /// in the stream is not preserved; the copy keeps the record's partition key, so it lands on
    /// that key's shard.
    type Copies = AddressedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_stream(KinesisStream::new(name)).await
    }
}

impl<Env: Envelope> DefaultPublish for ConnectedKinesisBroker<Env> {
    type Policy = KinesisPublish;
}

/// The harness's view of the in-process transport: what it injects, what it reads back, the
/// coordinator it counts the in-flight deliveries with, and which subscriptions a record reaches.
///
/// # Panics
///
/// `inject` and `published` panic on a broker connected with `connect`: the harness drives only
/// the transport `connect_in_process` produced, and a live stream has no log to read here and no
/// synchronous way to take a record. `inject` also panics on a record the service refuses (a
/// stream name or a partition key it does not accept, a record over its size limit), because the
/// harness has no error to return it through.
#[cfg(feature = "testing")]
impl<Env: Envelope> TestableBroker for ConnectedKinesisBroker<Env> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Link::InProcess(bus) = &self.link {
            bus.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let stream = message.name().to_owned();
        if let Err(err) = self.bus("inject").inject(message, &self.envelope) {
            panic!("the injected record to {stream:?} is not one the service takes: {err}");
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.bus("published").published(name)
    }

    /// A record reaches the subscription on the stream it was published to, which reads every
    /// shard of its stream, and a subscription on another stream never receives it. A broker
    /// reads a stream once, so it opens no second subscription to the same stream.
    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        subscriptions
            .iter()
            .enumerate()
            .filter(|(_, stream)| **stream == destination)
            .map(|(position, _)| position)
            .collect()
    }
}

#[cfg(feature = "testing")]
impl<Env> ConnectedKinesisBroker<Env> {
    /// The in-process transport, which is all the harness drives.
    fn bus(&self, what: &str) -> &Arc<Bus> {
        match &self.link {
            Link::InProcess(bus) => bus,
            Link::Aws(_) => panic!(
                "TestableBroker::{what} reached a broker connected with `connect`; the harness \
                 drives the transport `connect_in_process` produces"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use aws_sdk_kinesis::Client as KinesisClient;
    use aws_sdk_kinesis::config::{Credentials, SharedCredentialsProvider};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::runtime::Builder;
    use tokio::time::timeout;

    use super::*;

    /// An address nothing listens on: a request that goes there is refused.
    const NOWHERE: &str = "http://127.0.0.1:9";

    /// A prebuilt config that names a region and an endpoint of its own.
    fn prebuilt(region: &str, endpoint: &str) -> SdkConfig {
        SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.to_owned()))
            .endpoint_url(endpoint)
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "test", "test", None, None, "test",
            )))
            .build()
    }

    /// A listener standing in for an endpoint, and the URL that names it.
    async fn endpoint() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a local port binds");
        let url = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("a bound listener has an address")
        );
        (listener, url)
    }

    fn core(connected: &ConnectedKinesisBroker) -> &Core {
        match &connected.link {
            Link::Aws(core) => core,
            #[cfg(feature = "testing")]
            Link::InProcess(_) => unreachable!("the broker connected to AWS"),
        }
    }

    /// The lowercased head of the first request `request` makes reach `listener`.
    async fn request_head<Sent>(listener: TcpListener, request: Sent) -> String
    where
        Sent: Future + Send + 'static,
        Sent::Output: Send,
    {
        let sending = tokio::spawn(request);
        let (mut socket, _) = timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("no request reached the endpoint the broker should send to")
            .expect("the listener accepts");
        let mut head = Vec::new();
        let mut chunk = [0; 4096];
        while !head.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = socket.read(&mut chunk).await.expect("the request reads");
            assert_ne!(
                read, 0,
                "the connection closed before the request head ended"
            );
            head.extend_from_slice(&chunk[..read]);
        }
        sending.abort();
        String::from_utf8_lossy(&head).to_lowercase()
    }

    /// The head of the first Kinesis request the connected broker sends, as `listener` receives
    /// it.
    async fn kinesis_request(listener: TcpListener, connected: &ConnectedKinesisBroker) -> String {
        request_head(
            listener,
            core(connected)
                .client
                .with(|client| client.list_streams().send()),
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_endpoint_and_region_named_on_the_broker_override_a_prebuilt_config() {
        let (listener, named) = endpoint().await;
        let connected = KinesisBroker::from_config(prebuilt("us-east-1", NOWHERE))
            .endpoint(named)
            .region("eu-west-1")
            .connect()
            .await
            .expect("the broker connects");
        let head = kinesis_request(listener, &connected).await;
        assert!(head.contains("/eu-west-1/kinesis/aws4_request"), "{head}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_named_endpoint_keeps_the_region_of_a_prebuilt_config() {
        let (listener, named) = endpoint().await;
        let connected = KinesisBroker::from_config(prebuilt("ap-south-1", NOWHERE))
            .endpoint(named)
            .connect()
            .await
            .expect("the broker connects");
        let head = kinesis_request(listener, &connected).await;
        assert!(head.contains("/ap-south-1/kinesis/aws4_request"), "{head}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_named_region_keeps_the_endpoint_of_a_prebuilt_config() {
        let (listener, configured) = endpoint().await;
        let connected = KinesisBroker::from_config(prebuilt("us-east-1", &configured))
            .region("eu-west-1")
            .connect()
            .await
            .expect("the broker connects");
        let head = kinesis_request(listener, &connected).await;
        assert!(head.contains("/eu-west-1/kinesis/aws4_request"), "{head}");
    }

    /// A thread of another runtime builds a client of its own, and it honours the broker too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_thread_of_another_runtime_sends_where_the_broker_names() {
        let (listener, named) = endpoint().await;
        let connected = KinesisBroker::from_config(prebuilt("us-east-1", NOWHERE))
            .endpoint(named)
            .region("eu-west-1")
            .connect()
            .await
            .expect("the broker connects");
        let request = thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("a current-thread runtime builds");
                    runtime.block_on(async {
                        core(&connected).client.with(KinesisClient::list_streams)
                    })
                })
                .join()
                .expect("the other thread finished")
        });
        let head = request_head(listener, request.send()).await;
        assert!(head.contains("/eu-west-1/kinesis/aws4_request"), "{head}");
    }

    /// Two replicas in containers both run as PID 1 and both start their first broker, and a
    /// lease store tells owners apart by this id alone: equal ids would hold every shard twice.
    #[test]
    fn two_hosts_with_the_same_pid_and_sequence_get_different_owner_ids() {
        let first = owner_on(None, 1, 0);
        let second = owner_on(None, 1, 0);
        assert_ne!(first, second);
    }
}
