//! [`KinesisPublisher`], its [`KinesisPublish`] policy, and the crate's publish steps.

use aws_sdk_kinesis::primitives::Blob;
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedKinesisBroker, Core, CoreCell};
use crate::error::{KinesisError, sdk_err};
use crate::message::{PARTITION_KEY_HEADER, encode_envelope};

/// Publishes records to Kinesis streams (the destination is the stream name or ARN).
///
/// The `partition-key` header becomes the record's partition key - the unit of shard routing
/// and per-key ordering; without one, a process-unique key spreads records across shards.
/// [`KinesisPublishExt::with_partition_key`] names that key per publish without spelling the
/// header out. User headers beyond the partition key travel in a small conditional envelope (Kinesis
/// records carry only a data blob and a partition key); plain payloads stay unenveloped.
/// Buildable before `connect` and usable until `shutdown`; afterwards every publish reports
/// [`KinesisError::NotConnected`].
#[derive(Clone)]
pub struct KinesisPublisher {
    cell: CoreCell,
}

impl std::fmt::Debug for KinesisPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisPublisher").finish_non_exhaustive()
    }
}

impl KinesisPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self { cell }
    }

    fn core(&self) -> Result<&Core, KinesisError> {
        let core = self.cell.get().ok_or(KinesisError::NotConnected)?;
        core.ensure_open()?;
        Ok(core)
    }
}

fn spread_key() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "rs-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

impl Publisher for KinesisPublisher {
    type Error = KinesisError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let core = self.core()?;
        let partition_key = msg
            .headers()
            .get(PARTITION_KEY_HEADER)
            .map_or_else(spread_key, |key| String::from_utf8_lossy(key).into_owned());
        let data = encode_envelope(msg.headers(), msg.payload());
        core.client
            .put_record()
            .stream_name(msg.name())
            .partition_key(partition_key)
            .data(Blob::new(data))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| KinesisError::Publish {
                stream: msg.name().to_owned(),
                source: sdk_err(&e),
            })
    }
}

/// The publish policy for [`KinesisPublisher`]: pure declaration, constructible anywhere,
/// paired with the connected broker by the runtime after `connect`.
///
/// # Examples
///
/// ```
/// use ruststream_kinesis::KinesisPublish;
///
/// let policy = KinesisPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct KinesisPublish;

impl PublishPolicy<ConnectedKinesisBroker> for KinesisPublish {
    type Live = KinesisPublisher;

    async fn pair(self, connected: &ConnectedKinesisBroker) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
    }
}

/// The Kinesis publish steps, on this crate's publishers.
///
/// The framework routes every publish through one builder, entered with `message(..)` or
/// `raw(..)`. A per-message argument of the transport joins that chain one step earlier, on the
/// publisher itself: the step returns a publisher of its own that carries the argument and
/// applies it to each message on the way through, so the builder that follows is the framework's
/// unchanged.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "testing")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::runtime::PublishExt;
/// use ruststream_kinesis::KinesisPublishExt;
/// use ruststream_kinesis::testing::KinesisTestBroker;
///
/// let publisher = KinesisTestBroker::new().publisher();
/// publisher
///     .with_partition_key("tenant-acme")
///     .raw(br#"{"id":1}"#)
///     .to("jobs")
///     .publish()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait KinesisPublishExt: Publisher + Clone + crate::sealed::Sealed {
    /// Publishes through this publisher with `key` as the record's partition key.
    ///
    /// The partition key selects the shard, and with it per-key ordering. Naming it here is the
    /// per-message alternative to setting [`PARTITION_KEY_HEADER`] by hand, so a mistyped header
    /// name cannot quietly spread a keyed stream across every shard. A key named on the step
    /// wins over one already present in the message's headers.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "testing")]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::runtime::PublishExt;
    /// use ruststream_kinesis::KinesisPublishExt;
    /// use ruststream_kinesis::testing::KinesisTestBroker;
    ///
    /// let publisher = KinesisTestBroker::new().publisher();
    /// publisher
    ///     .with_partition_key("tenant-acme")
    ///     .raw(br#"{"id":1}"#)
    ///     .to("jobs")
    ///     .publish()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    fn with_partition_key(&self, key: impl Into<String>) -> PartitionKeyed<Self> {
        PartitionKeyed {
            inner: self.clone(),
            key: key.into(),
        }
    }
}

impl crate::sealed::Sealed for KinesisPublisher {}
impl KinesisPublishExt for KinesisPublisher {}

/// The publisher returned by
/// [`with_partition_key`](KinesisPublishExt::with_partition_key): it stamps the captured key
/// onto every message and hands it to the publisher it wraps.
///
/// It is a [`Publisher`] like any other, so the framework's publish builder (`message(..)`,
/// `raw(..)`) applies to it unchanged.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "testing")]
/// # fn demo() {
/// use ruststream_kinesis::{KinesisPublishExt, PartitionKeyed};
/// use ruststream_kinesis::testing::{KinesisTestBroker, KinesisTestPublisher};
///
/// let keyed: PartitionKeyed<KinesisTestPublisher> =
///     KinesisTestBroker::new().publisher().with_partition_key("tenant-acme");
/// # let _ = keyed;
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PartitionKeyed<P> {
    inner: P,
    key: String,
}

impl<P: Publisher> Publisher for PartitionKeyed<P> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        // The header is the crate's own wire for the key, so the envelope, the delivered
        // `Partitioned` view, and the in-process broker all keep reading it from one place.
        let mut headers = msg.headers().clone();
        headers.insert(PARTITION_KEY_HEADER, self.key.clone());
        self.inner.publish(msg.with_headers(headers)).await
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use ruststream::runtime::PublishExt;
    use ruststream::testing::TestableBroker;
    use ruststream::{Broker, Headers};

    use super::*;
    use crate::testing::KinesisTestBroker;

    async fn connected() -> crate::testing::ConnectedKinesisTestBroker {
        KinesisTestBroker::new()
            .connect()
            .await
            .expect("the in-process broker connects")
    }

    #[tokio::test]
    async fn the_step_carries_the_key_through_the_publish_builder() {
        let broker = connected().await;
        broker
            .publisher()
            .with_partition_key("tenant-acme")
            .raw(b"payload")
            .to("jobs")
            .publish()
            .await
            .expect("the publish succeeds");

        let published = broker.published("jobs");
        assert_eq!(published.len(), 1);
        assert_eq!(
            published[0].headers().get_str(PARTITION_KEY_HEADER),
            Some("tenant-acme")
        );
    }

    #[tokio::test]
    async fn the_step_wins_over_a_key_already_in_the_headers() {
        let broker = connected().await;
        let mut headers = Headers::new();
        headers.insert(PARTITION_KEY_HEADER, "inherited");
        broker
            .publisher()
            .with_partition_key("named-on-the-step")
            .publish(OutgoingMessage::new("jobs", b"payload").with_headers(headers))
            .await
            .expect("the publish succeeds");

        let published = broker.published("jobs");
        assert_eq!(
            published[0].headers().get_str(PARTITION_KEY_HEADER),
            Some("named-on-the-step")
        );
    }

    #[tokio::test]
    async fn a_publish_without_the_step_keeps_the_header_route() {
        let broker = connected().await;
        let mut headers = Headers::new();
        headers.insert(PARTITION_KEY_HEADER, "by-hand");
        broker
            .publisher()
            .publish(OutgoingMessage::new("jobs", b"payload").with_headers(headers))
            .await
            .expect("the publish succeeds");

        let published = broker.published("jobs");
        assert_eq!(
            published[0].headers().get_str(PARTITION_KEY_HEADER),
            Some("by-hand")
        );
    }
}
