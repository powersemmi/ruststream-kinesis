//! The broker ladder: [`KinesisBroker`] -> [`ConnectedKinesisBroker`].
//!
//! Construction is synchronous and I/O-free; credential resolution happens in the consuming
//! [`Broker::connect`], and the connected form holds the live SDK client directly. One shared
//! cell remains so publishers can be handed out while the application is still being
//! assembled, before `connect` runs.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_kinesis::client::Waiters;
use ruststream::{Broker, ConnectedBroker, DefaultPublish, DescribeServer, ServerSpec, Subscribe};
use tokio::sync::OnceCell;

use crate::error::{KinesisError, sdk_err};
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

pub(crate) type CoreCell = Arc<OnceCell<Arc<Core>>>;

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
        let core = self
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
                Ok::<_, KinesisError>(Arc::new(Core {
                    client: aws_sdk_kinesis::Client::new(&config),
                    store: self
                        .store
                        .clone()
                        .unwrap_or_else(|| Arc::new(MemoryLeaseStore::new())),
                    owner: self.owner.clone().unwrap_or_else(default_owner),
                    closed: AtomicBool::new(false),
                }))
            })
            .await?
            .clone();
        Ok(ConnectedKinesisBroker {
            core,
            cell: self.cell,
        })
    }
}

impl DescribeServer for KinesisBroker {
    fn describe_server(&self) -> ServerSpec {
        let host = self
            .endpoint
            .clone()
            .unwrap_or_else(|| "kinesis.amazonaws.com".to_owned());
        ServerSpec::new(host, "kinesis")
    }
}

/// The typed witness that `connect` succeeded: holds the live SDK client directly.
#[derive(Debug)]
pub struct ConnectedKinesisBroker {
    pub(crate) core: Arc<Core>,
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

    /// Opens the subscription described by `descriptor`.
    ///
    /// # Errors
    ///
    /// Returns [`KinesisError`] when the descriptor is invalid, stream creation (when opted
    /// in) fails, or the broker is shut down.
    pub async fn subscribe_stream(
        &self,
        descriptor: KinesisStream,
    ) -> Result<KinesisSubscriber, KinesisError> {
        descriptor.validate()?;
        self.core.ensure_open()?;
        if let Some(shards) = descriptor.create_value() {
            self.ensure_stream(descriptor.stream(), shards).await?;
        }
        Ok(KinesisSubscriber::open(&self.core, descriptor))
    }

    /// Creates the stream when missing and waits until it is active.
    async fn ensure_stream(&self, stream: &str, shards: i32) -> Result<(), KinesisError> {
        let exists = self
            .core
            .client
            .describe_stream_summary()
            .stream_name(stream)
            .send()
            .await
            .is_ok();
        if !exists {
            let created = self
                .core
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
        self.core
            .client
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

    async fn shutdown(self) -> Result<(), Self::Error> {
        // The SDK client has no close; the closed flag stops readers and stale handles, and
        // leases lapse or are released by the readers as they exit.
        self.core.closed.store(true, Ordering::Release);
        Ok(())
    }
}

impl Subscribe for ConnectedKinesisBroker {
    type Subscriber = KinesisSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_stream(KinesisStream::new(name)).await
    }
}

impl DefaultPublish for ConnectedKinesisBroker {
    type Policy = KinesisPublish;
}
