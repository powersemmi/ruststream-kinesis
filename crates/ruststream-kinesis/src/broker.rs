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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_kinesis::client::Waiters;
#[cfg(feature = "testing")]
use ruststream::testing::{Coordinator, InProcess, TestableBroker};
use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, DefaultPublish, DescribeServer, ServerSpec, Subscribe,
};
#[cfg(feature = "testing")]
use ruststream::{OutgoingMessage, RawMessage};
use tokio::sync::OnceCell;

use crate::error::{KinesisError, sdk_err};
#[cfg(feature = "testing")]
use crate::in_process::Bus;
use crate::lease::{LeaseStore, MemoryLeaseStore};
use crate::publisher::{KinesisPublish, KinesisPublisher};
use crate::stream::KinesisStream;
use crate::subscriber::KinesisSubscriber;

/// The live client state shared by the connected form and every handle derived from it.
///
/// Why runtime checks exist here at all: the SDK client has no shutdown and keeps working
/// forever, and publishers may be handed out before `connect` and outlive `shutdown`
/// (aliasing) - so the closed state is an explicit flag a stale handle trips over instead of
/// silently succeeding.
pub(crate) struct Core {
    pub(crate) client: aws_sdk_kinesis::Client,
    pub(crate) store: Arc<dyn LeaseStore>,
    pub(crate) owner: String,
    pub(crate) closed: AtomicBool,
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
/// # Examples
///
/// ```
/// use ruststream_kinesis::KinesisBroker;
///
/// let broker = KinesisBroker::new(); // region and credentials from the environment
/// let local = KinesisBroker::new()
///     .endpoint("http://localhost:4566")
///     .test_credentials()
///     .region("us-east-1");
/// # let _ = (broker, local);
/// ```
#[derive(Clone, Default)]
#[must_use]
pub struct KinesisBroker {
    endpoint: Option<String>,
    region: Option<String>,
    test_credentials: bool,
    sdk_config: Option<SdkConfig>,
    store: Option<Arc<dyn LeaseStore>>,
    owner: Option<String>,
    cell: CoreCell,
}

impl std::fmt::Debug for KinesisBroker {
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
    pub fn from_config(config: SdkConfig) -> Self {
        Self {
            sdk_config: Some(config),
            ..Self::default()
        }
    }

    /// Overrides the service endpoint (a local stack for development).
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Overrides the region.
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Uses dummy static credentials, for local stacks that require credentials to be
    /// present but ignore their values.
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

    /// Overrides this instance's lease-owner id (defaults to a process-unique value).
    pub fn owner_id(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    /// A publisher sharing this broker's connection cell; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> KinesisPublisher {
        KinesisPublisher::new(Arc::clone(&self.cell))
    }
}

fn default_owner() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "rs-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

impl Broker for KinesisBroker {
    type Error = KinesisError;
    type Connected = ConnectedKinesisBroker;

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
                Ok::<_, KinesisError>(Link::Aws(Arc::new(Core {
                    client: aws_sdk_kinesis::Client::new(&config),
                    store: self
                        .store
                        .clone()
                        .unwrap_or_else(|| Arc::new(MemoryLeaseStore::new())),
                    owner: self.owner.clone().unwrap_or_else(default_owner),
                    closed: AtomicBool::new(false),
                })))
            })
            .await?
            .clone();
        Ok(ConnectedKinesisBroker {
            link,
            cell: self.cell,
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
#[cfg(feature = "testing")]
impl InProcess for KinesisBroker {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        let link = self
            .cell
            .get_or_init(async || Link::InProcess(Bus::new()))
            .await
            .clone();
        Ok(ConnectedKinesisBroker {
            link,
            cell: self.cell,
        })
    }
}

#[cfg(feature = "testing")]
ruststream::register_testable_broker!(KinesisBroker);

impl DescribeServer for KinesisBroker {
    /// The coordinate clients connect to: the host and port, never the endpoint URL as written.
    ///
    /// An overridden endpoint is a URL, so the scheme and any userinfo come off it here - the
    /// generated document is published and shared, and a URL carrying credentials would leave the
    /// service with it. Without an override the coordinate is the regional Kinesis endpoint; a
    /// broker that takes its region from the environment cannot name it before `connect`, so the
    /// document reports the service-wide host instead.
    fn describe_server(&self) -> ServerSpec {
        let host = self.endpoint.as_deref().map_or_else(
            || {
                self.region.as_ref().map_or_else(
                    || "kinesis.amazonaws.com".to_owned(),
                    |region| format!("kinesis.{region}.amazonaws.com"),
                )
            },
            ServerSpec::host_from_url,
        );
        ServerSpec::new(host, "kinesis")
    }
}

/// The typed witness that `connect` succeeded: holds the live SDK client directly.
#[derive(Debug)]
pub struct ConnectedKinesisBroker {
    link: Link,
    // Keeps the cell of publishers handed out before connect alive and filled.
    cell: CoreCell,
}

impl ConnectedKinesisBroker {
    /// A publisher from the connected form. It rides the same cell-backed publisher type as
    /// the early path; by now `connect` has filled the cell, so it resolves immediately.
    #[must_use]
    pub fn publisher(&self) -> KinesisPublisher {
        KinesisPublisher::new(Arc::clone(&self.cell))
    }

    /// The same publisher carrying the partition key a mount site named on its policy.
    pub(crate) fn publisher_keyed(&self, partition_key: Option<Arc<str>>) -> KinesisPublisher {
        KinesisPublisher::keyed(Arc::clone(&self.cell), partition_key)
    }

    /// Opens the subscription described by `descriptor`.
    ///
    /// # Errors
    ///
    /// Returns [`KinesisError`] when the descriptor is invalid, the stream does not exist and
    /// the descriptor does not create it, stream creation (when opted in) fails, or the broker
    /// is shut down.
    pub async fn subscribe_stream(
        &self,
        descriptor: KinesisStream,
    ) -> Result<KinesisSubscriber, KinesisError> {
        descriptor.validate()?;
        let core = match &self.link {
            Link::Aws(core) => core,
            #[cfg(feature = "testing")]
            Link::InProcess(bus) => return bus.subscribe(&descriptor),
        };
        core.ensure_open()?;
        if let Some(shards) = descriptor.create_value() {
            Self::ensure_stream(core, descriptor.stream(), shards).await?;
        } else {
            Self::require_stream(core, descriptor.stream()).await?;
        }
        Ok(KinesisSubscriber::open(core, descriptor))
    }

    /// Refuses a stream the service does not have, before the subscription exists.
    ///
    /// The shards are listed by the subscription's coordinator, which runs detached, so a
    /// mistyped stream name would otherwise start the service and report itself once per shard
    /// sync for as long as it runs. One describe per subscription, at startup, is what turns
    /// that into a failure the caller sees.
    async fn require_stream(core: &Core, stream: &str) -> Result<(), KinesisError> {
        core.client
            .describe_stream_summary()
            .stream_name(stream)
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
            .describe_stream_summary()
            .stream_name(stream)
            .send()
            .await
            .is_ok();
        if !exists {
            let created = core
                .client
                .create_stream()
                .stream_name(stream)
                .shard_count(shards)
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
            .wait_until_stream_exists()
            .stream_name(stream)
            .wait(Duration::from_mins(1))
            .await
            .map_err(|e| KinesisError::Stream {
                stream: stream.to_owned(),
                source: Box::new(e),
            })?;
        Ok(())
    }
}

impl ConnectedBroker for ConnectedKinesisBroker {
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

impl Subscribe for ConnectedKinesisBroker {
    type Subscriber = KinesisSubscriber;
    /// A stream is both what a subscription reads and what a publish writes to, so a bare name
    /// is the address of its own copies: the framework publishes the deferred copy of a
    /// `retry_after`, and a spent delivery, back under the name the subscription opened.
    ///
    /// A copy reaches the subscription from the tip, so a service that defers a record gets it
    /// back at the end of the stream rather than in place. Ordering against the records already
    /// in the stream is not preserved, and the shard the copy lands on is the one its partition
    /// key picks.
    type Copies = AddressedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_stream(KinesisStream::new(name)).await
    }
}

impl DefaultPublish for ConnectedKinesisBroker {
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
impl TestableBroker for ConnectedKinesisBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Link::InProcess(bus) = &self.link {
            bus.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let stream = message.name().to_owned();
        if let Err(err) = self.bus("inject").inject(message) {
            panic!("the injected record to {stream:?} is not one the service takes: {err}");
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.bus("published").published(name)
    }

    /// A record reaches every subscription on the stream it was published to. Each subscription
    /// reads every shard of its stream through iterators of its own (shared-throughput consumers),
    /// so two subscriptions on one stream both receive the record, and a subscription on another
    /// stream never does.
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
impl ConnectedKinesisBroker {
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
    use super::*;

    /// The generated document is published and shared, so the server it names is a coordinate:
    /// the host and port a client dials, never the endpoint URL the service was configured with.
    #[test]
    fn the_described_server_is_a_host_not_the_configured_url() {
        let spec = KinesisBroker::new()
            .endpoint("https://kinesis.eu-west-1.amazonaws.com:443")
            .describe_server();
        assert_eq!(
            spec.host.as_deref(),
            Some("kinesis.eu-west-1.amazonaws.com:443")
        );
    }

    /// A URL may carry credentials, and the document must not.
    #[test]
    fn credentials_in_an_endpoint_url_stay_out_of_the_description() {
        let spec = KinesisBroker::new()
            .endpoint("http://key:secret@localhost:4566")
            .describe_server();
        assert_eq!(spec.host.as_deref(), Some("localhost:4566"));
    }

    /// Without an override the coordinate is the regional endpoint the SDK dials.
    #[test]
    fn a_named_region_names_the_regional_endpoint() {
        let spec = KinesisBroker::new().region("eu-west-1").describe_server();
        assert_eq!(
            spec.host.as_deref(),
            Some("kinesis.eu-west-1.amazonaws.com")
        );
    }
}
