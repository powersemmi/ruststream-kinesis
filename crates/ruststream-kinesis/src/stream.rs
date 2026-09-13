//! [`KinesisStream`]: the subscription descriptor, and the mount-site settings over it.
//!
//! The consumer model decides both cost and latency, so it is explicit on the descriptor.
//! What it does not carry is how many records a delivered batch holds: that is the framework's
//! `batch(n)`, which reaches the reader as the `GetRecords` limit. Where the subscription
//! starts is not part of it either - that vocabulary is
//! [`KinesisPosition`](crate::KinesisPosition), spoken through the framework's `start_at(..)`
//! clause and the `Seekable` capability.

use std::future::{Future, ready};
use std::time::Duration;

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::{Binding, Bindings};
use ruststream::runtime::{Declared, IntoSource, SubscriberBuilder, SubscriberSettings};
use ruststream::{AddressedCopies, RedeliveryAddress, RedeliveryAddressed, SubscriptionSource};
#[cfg(feature = "asyncapi")]
use serde::Serialize;

use crate::broker::ConnectedKinesisBroker;
use crate::error::KinesisError;
use crate::subscriber::KinesisSubscriber;
#[cfg(feature = "testing")]
use crate::testing::{ConnectedKinesisTestBroker, KinesisTestSubscriber};

/// A subscription descriptor for one Kinesis stream.
///
/// Every shard resumes from its stored checkpoint, and a shard without one starts at the tip.
/// To open somewhere else, wrap the descriptor in the framework's `start_at(..)` clause with a
/// [`KinesisPosition`](crate::KinesisPosition).
///
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]`
/// decorator, and [`IntoSource`], so the manual path's
/// [`subscriber`](ruststream::runtime::subscriber) constructor names a stream with it too:
///
/// ```
/// use std::time::Duration;
///
/// use ruststream::prelude::*;
/// use ruststream_kinesis::KinesisStream;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// struct Audit;
///
/// impl Handle<Order> for Audit {
///     async fn handle(
///         &self,
///         order: &Order,
///         _outs: &(),
///         _ctx: &mut Context<'_>,
///     ) -> Result<(), HandlerOutcome> {
///         println!("order {}", order.id);
///         Ok(())
///     }
/// }
///
/// let source = KinesisStream::new("orders").poll_interval(Duration::from_millis(500));
/// let mountable = subscriber(source, Audit).build();
/// # let _ = mountable;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct KinesisStream {
    stream: String,
    poll_interval: Duration,
    create_shards: Option<i32>,
}

impl KinesisStream {
    /// Names the stream (name or ARN).
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            // The service recommends waiting a second between reads to stay within the
            // per-shard budget.
            poll_interval: Duration::from_secs(1),
            create_shards: None,
        }
    }

    /// The pause between reads on an idle shard. Defaults to 1 second, the service's own
    /// recommendation; lower values spend the 5-reads-per-second budget faster.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Creates the stream with `shards` provisioned shards on subscribe when it does not
    /// exist yet. Meant for local development and tests; production streams are usually
    /// managed as infrastructure.
    pub fn create_if_missing(mut self, shards: i32) -> Self {
        self.create_shards = Some(shards);
        self
    }

    /// The stream name this descriptor resolves.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }

    pub(crate) fn poll_value(&self) -> Duration {
        self.poll_interval
    }

    pub(crate) fn create_value(&self) -> Option<i32> {
        self.create_shards
    }

    /// Rejects descriptors that cannot form a subscription, before any I/O.
    pub(crate) fn validate(&self) -> Result<(), KinesisError> {
        if self.stream.is_empty() {
            return Err(KinesisError::Invalid("stream must be non-empty".into()));
        }
        if let Some(shards) = self.create_shards
            && shards < 1
        {
            return Err(KinesisError::Invalid(
                "create_if_missing needs at least one shard".into(),
            ));
        }
        Ok(())
    }
}

/// The Kinesis subscription settings, chained on a mount site after the framework's own.
///
/// The framework carries one subscription parameter down to a broker - the batch size, named
/// with `batch(n)` - and leaves every other knob to the broker's own vocabulary. These are this
/// crate's, and they read as a chain after it:
///
/// ```
/// use std::time::Duration;
///
/// use ruststream_kinesis::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64 }
///
/// #[subscriber(KinesisStream::new("jobs"))]
/// async fn work(batch: &[Job]) -> Vec<HandlerOutcome> {
///     batch.iter().map(|_| HandlerOutcome::ack()).collect()
/// }
///
/// # fn wire() {
/// let _mountable = work
///     .batch(nonzero!(500))
///     .poll_interval(Duration::from_millis(500))
///     .create_if_missing(1);
/// # }
/// ```
///
/// Both settings transform the descriptor, so they need one to transform: `start_at(..)`
/// replaces the source with the framework's position wrapper, and these chain before it. A
/// subscriber whose attribute already named a start position names them on the descriptor
/// instead - `#[subscriber(KinesisStream::new("jobs").poll_interval(..), start_at(..))]` - which
/// is the same two methods on the same type.
pub trait KinesisSubscriberExt {
    /// The pause between reads on an idle shard. The mount-site spelling of
    /// [`KinesisStream::poll_interval`].
    #[must_use]
    fn poll_interval(self, interval: Duration) -> Self;

    /// Creates the stream with `shards` provisioned shards on subscribe when it does not exist
    /// yet. The mount-site spelling of [`KinesisStream::create_if_missing`].
    #[must_use]
    fn create_if_missing(self, shards: i32) -> Self;
}

impl<Def, State, DefCodec> KinesisSubscriberExt
    for SubscriberBuilder<Def, KinesisStream, State, DefCodec>
where
    Def: Declared,
{
    fn poll_interval(self, interval: Duration) -> Self {
        self.map_source(|source| source.poll_interval(interval))
    }

    fn create_if_missing(self, shards: i32) -> Self {
        self.map_source(|source| source.create_if_missing(shards))
    }
}

/// The manual path's constructor takes a subject string or a source, and this is what makes the
/// crate's own descriptor the second: a service without the `macros` feature names its stream,
/// and the settings that price a read, exactly as an attribute one does.
impl IntoSource for KinesisStream {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

/// What a Kinesis subscription writes into the generated document.
///
/// The `AsyncAPI` specification lists no Kinesis binding, and the protocol keys of a binding object
/// are a closed list, so this travels as an `x-` extension at the same level. Only what the
/// descriptor itself holds goes in: the real shard count of an existing stream, its ARN and its
/// retention are the service's to answer and the document is built before anything connects.
#[cfg(feature = "asyncapi")]
#[derive(Debug, Serialize)]
struct KinesisChannelBinding<'a> {
    stream: &'a str,
    #[serde(rename = "pollIntervalMs")]
    poll_interval_ms: u64,
    /// The shards the descriptor would provision, present only where it creates the stream.
    #[serde(rename = "shardCount", skip_serializing_if = "Option::is_none")]
    shard_count: Option<i32>,
}

/// The extension key this crate writes its bindings under, on the channel and on the message.
#[cfg(feature = "asyncapi")]
pub(crate) const BINDING_KEY: &str = "x-ruststream-kinesis";

impl KinesisStream {
    /// The channel binding of this descriptor, or nothing when it cannot be built.
    ///
    /// A document is a description of the service, so a binding that fails to serialize is one
    /// the document goes without rather than a start-up the service loses.
    #[cfg(feature = "asyncapi")]
    fn binding(&self) -> Bindings {
        let body = KinesisChannelBinding {
            stream: self.stream(),
            poll_interval_ms: u64::try_from(self.poll_interval.as_millis()).unwrap_or(u64::MAX),
            shard_count: self.create_shards,
        };
        Binding::extension(BINDING_KEY, &body)
            .map_or_else(|_| Bindings::new(), |binding| Bindings::new().with(binding))
    }
}

impl SubscriptionSource<ConnectedKinesisBroker> for KinesisStream {
    type Subscriber = KinesisSubscriber;
    /// Kinesis moves nothing on its own: it holds no redelivery timer, no delivery counter and
    /// no dead-letter mechanism, so the framework publishes every copy. A stream is what a
    /// subscription reads and what a publish writes to, so this descriptor knows where a copy
    /// goes.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.stream()
    }

    async fn subscribe(
        self,
        connected: &ConnectedKinesisBroker,
    ) -> Result<KinesisSubscriber, KinesisError> {
        connected.subscribe_stream(self).await
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        self.binding()
    }
}

impl RedeliveryAddressed<ConnectedKinesisBroker> for KinesisStream {
    /// The stream this descriptor names, which is where a deferred copy and a spent delivery of
    /// this registration are published.
    ///
    /// The copy arrives at the tip rather than in the place the original held, and its partition
    /// key picks the shard it lands on, so a deferred record loses its position in the stream's
    /// order.
    fn redelivery_address(
        &self,
        _connected: &ConnectedKinesisBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, KinesisError>> + Send {
        // The stream name is the answer, so nothing is asked of the broker and nothing is awaited.
        ready(Ok(RedeliveryAddress::new(self.stream().to_owned())))
    }
}

/// The same descriptor opens a subscription on the in-process stand-in, so a service keeps its
/// own mount - `#[subscriber(KinesisStream::new("orders"))]` - in its unit tests.
///
/// The setting that prices a real read (`poll_interval`) has nothing to bill in process, and
/// `create_if_missing` has no stream to create: a name is a log as soon as something is
/// published to it. The descriptor is still validated, so a mount the service would reject
/// fails here too.
#[cfg(feature = "testing")]
impl SubscriptionSource<ConnectedKinesisTestBroker> for KinesisStream {
    type Subscriber = KinesisTestSubscriber;
    /// The same copy path the descriptor declares against the service, so a registration that
    /// caps its retries or names a dead-letter stream is driven in a unit test exactly as it
    /// runs in production.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.stream()
    }

    fn subscribe(
        self,
        connected: &ConnectedKinesisTestBroker,
    ) -> impl Future<Output = Result<Self::Subscriber, KinesisError>> + Send {
        ready(self.validate().map(|()| connected.open(self.stream())))
    }

    /// The same binding the descriptor writes against the service, so a document built in a
    /// unit test is the document the service publishes.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        self.binding()
    }
}

#[cfg(feature = "testing")]
impl RedeliveryAddressed<ConnectedKinesisTestBroker> for KinesisStream {
    /// The same answer the descriptor gives against the service, so a copy the framework
    /// publishes in a unit test lands where it lands in production.
    fn redelivery_address(
        &self,
        _connected: &ConnectedKinesisTestBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, KinesisError>> + Send {
        ready(Ok(RedeliveryAddress::new(self.stream().to_owned())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_descriptors_are_rejected_before_io() {
        assert!(KinesisStream::new("").validate().is_err());
        assert!(
            KinesisStream::new("s")
                .create_if_missing(0)
                .validate()
                .is_err()
        );
    }
}
