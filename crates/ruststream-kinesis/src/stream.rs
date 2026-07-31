//! [`KinesisStream`]: the subscription descriptor.
//!
//! The consumer model and the start position decide both cost and latency, so they are
//! explicit on the descriptor. The start position is an enum with per-variant data - a
//! sequence-position without a sequence number is unrepresentable.

use std::time::Duration;

use ruststream::SubscriptionSource;

use crate::broker::ConnectedKinesisBroker;
use crate::error::KinesisError;
use crate::subscriber::KinesisSubscriber;

/// Where a shard starts when it has no checkpoint yet.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StartPosition {
    /// Only records published after the subscription attached. The default.
    #[default]
    Latest,
    /// Everything retained (the trim horizon).
    Horizon,
    /// Records at or after the timestamp (milliseconds since the Unix epoch).
    Timestamp(u64),
    /// Records after the given sequence number.
    Sequence(String),
}

/// A subscription descriptor for one Kinesis stream.
///
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]`
/// decorator:
///
/// ```
/// use ruststream_kinesis::{KinesisStream, StartPosition};
///
/// let source = KinesisStream::new("orders").start(StartPosition::Horizon);
/// # let _ = source;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct KinesisStream {
    stream: String,
    start: StartPosition,
    batch: i32,
    poll_interval: Duration,
    create_shards: Option<i32>,
}

impl KinesisStream {
    /// Names the stream (name or ARN).
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            start: StartPosition::default(),
            batch: 1000,
            // The service recommends waiting a second between reads to stay within the
            // per-shard budget.
            poll_interval: Duration::from_secs(1),
            create_shards: None,
        }
    }

    /// Where shards without a checkpoint start. Defaults to [`StartPosition::Latest`].
    pub fn start(mut self, start: StartPosition) -> Self {
        self.start = start;
        self
    }

    /// Records per read call (1..=10000). Defaults to 1000.
    pub fn batch(mut self, batch: i32) -> Self {
        self.batch = batch;
        self
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

    pub(crate) fn start_value(&self) -> &StartPosition {
        &self.start
    }

    pub(crate) fn batch_value(&self) -> i32 {
        self.batch
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
        if !(1..=10_000).contains(&self.batch) {
            return Err(KinesisError::Invalid(
                "batch must be within 1..=10000 (the read cap)".into(),
            ));
        }
        if let StartPosition::Sequence(sequence) = &self.start
            && sequence.is_empty()
        {
            return Err(KinesisError::Invalid(
                "a sequence start position needs a sequence number".into(),
            ));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_descriptors_are_rejected_before_io() {
        assert!(KinesisStream::new("").validate().is_err());
        assert!(KinesisStream::new("s").batch(0).validate().is_err());
        assert!(KinesisStream::new("s").batch(10_001).validate().is_err());
        assert!(
            KinesisStream::new("s")
                .start(StartPosition::Sequence(String::new()))
                .validate()
                .is_err()
        );
        assert!(
            KinesisStream::new("s")
                .create_if_missing(0)
                .validate()
                .is_err()
        );
    }
}
