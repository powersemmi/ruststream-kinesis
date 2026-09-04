//! [`KinesisStream`]: the subscription descriptor, and the mount-site settings over it.
//!
//! The consumer model decides both cost and latency, so it is explicit on the descriptor.
//! What it does not carry is how many records a delivered page holds: that is the framework's
//! `batch(n)`, which reaches the reader as the `GetRecords` limit. Where the subscription
//! starts is not part of it either - that vocabulary is
//! [`KinesisPosition`](crate::KinesisPosition), spoken through the framework's `start_at(..)`
//! clause and the `Seekable` capability.

use std::time::Duration;

#[cfg(feature = "testing")]
use std::future::{Future, ready};

use ruststream::SubscriptionSource;
use ruststream::runtime::{Declared, SubscriberBuilder, SubscriberSettings};

use crate::broker::ConnectedKinesisBroker;
use crate::error::KinesisError;
use crate::subscriber::KinesisSubscriber;

/// A subscription descriptor for one Kinesis stream.
///
/// Every shard resumes from its stored checkpoint, and a shard without one starts at the tip.
/// To open somewhere else, wrap the descriptor in the framework's `start_at(..)` clause with a
/// [`KinesisPosition`](crate::KinesisPosition).
///
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]`
/// decorator:
///
/// ```
/// use std::time::Duration;
///
/// use ruststream_kinesis::KinesisStream;
///
/// let source = KinesisStream::new("orders").poll_interval(Duration::from_millis(500));
/// # let _ = source;
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
/// The framework carries one subscription parameter down to a broker - the page size, named
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
/// async fn work(page: &[Job]) -> Vec<HandlerOutcome> {
///     page.iter().map(|_| HandlerOutcome::ack()).collect()
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

impl SubscriptionSource<ConnectedKinesisBroker> for KinesisStream {
    type Subscriber = KinesisSubscriber;

    fn name(&self) -> &str {
        self.stream()
    }

    async fn subscribe(
        self,
        connected: &ConnectedKinesisBroker,
    ) -> Result<KinesisSubscriber, KinesisError> {
        connected.subscribe_stream(self).await
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
impl SubscriptionSource<crate::testing::ConnectedKinesisTestBroker> for KinesisStream {
    type Subscriber = crate::testing::KinesisTestSubscriber;

    fn name(&self) -> &str {
        self.stream()
    }

    fn subscribe(
        self,
        connected: &crate::testing::ConnectedKinesisTestBroker,
    ) -> impl Future<Output = Result<Self::Subscriber, KinesisError>> + Send {
        ready(self.validate().map(|()| connected.open(self.stream())))
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
