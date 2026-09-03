//! [`KinesisPublisher`], its [`KinesisPublish`] policy, and the crate's publish steps.

use std::future::{Future, ready};

use aws_sdk_kinesis::primitives::Blob;
use ruststream::{HeaderMap, OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedKinesisBroker, Core, CoreCell};
use crate::error::{KinesisError, sdk_err};
use crate::message::{PARTITION_KEY_HEADER, encode_envelope};

/// Publishes records to Kinesis streams (the destination is the stream name or ARN).
///
/// The `partition-key` header becomes the record's partition key - the unit of shard routing
/// and per-key ordering; without one, a process-unique key spreads records across shards.
/// [`KinesisPublishExt::with_partition_key`] names that key without spelling the header out.
/// User headers beyond the partition key travel in a small conditional envelope (Kinesis
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

    fn pair(
        self,
        connected: &ConnectedKinesisBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

/// The Kinesis publish steps, on this crate's publishers.
///
/// A step goes in front of the framework's publish builder and returns a publisher, so
/// `message(..)` follows it unchanged.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "testing")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Outgoing, Serialized};
/// use ruststream_kinesis::KinesisPublishExt;
/// use ruststream_kinesis::testing::KinesisTestBroker;
///
/// // The record is already encoded, so it declares itself serialized: no codec runs on it,
/// // and the name still puts it in the generated document.
/// #[derive(Outgoing, Serialized)]
/// struct Job(Vec<u8>);
///
/// let publisher = KinesisTestBroker::new().publisher();
/// publisher
///     .with_partition_key("tenant-acme")
///     .message(&Job(br#"{"id":1}"#.to_vec()))
///     .to("jobs")
///     .publish()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait KinesisPublishExt: Publisher + Clone + crate::sealed::Sealed {
    /// Publishes through this publisher with `key` as the record's partition key.
    ///
    /// The partition key selects the shard, and with it per-key ordering. It is the named
    /// alternative to setting [`PARTITION_KEY_HEADER`] by hand.
    ///
    /// The key applies to every publish through the returned publisher. A publish that names
    /// `partition-key` itself overrides it for that message; one that names other headers keeps it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "testing")]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::runtime::PublishExt;
    /// use ruststream::{Outgoing, Serialized};
    /// use ruststream_kinesis::KinesisPublishExt;
    /// use ruststream_kinesis::testing::KinesisTestBroker;
    ///
    /// #[derive(Outgoing, Serialized)]
    /// struct Job(Vec<u8>);
    ///
    /// let publisher = KinesisTestBroker::new().publisher();
    /// publisher
    ///     .with_partition_key("tenant-acme")
    ///     .message(&Job(br#"{"id":1}"#.to_vec()))
    ///     .to("jobs")
    ///     .publish()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    fn with_partition_key(&self, key: impl Into<String>) -> PartitionKeyed<Self> {
        // Built once here, not per publish: `base_headers` hands the builder a borrow.
        let mut base = self.base_headers().cloned().unwrap_or_default();
        base.insert(PARTITION_KEY_HEADER, key.into());
        PartitionKeyed {
            inner: self.clone(),
            base,
        }
    }
}

impl crate::sealed::Sealed for KinesisPublisher {}
impl KinesisPublishExt for KinesisPublisher {}

/// The publisher returned by [`with_partition_key`](KinesisPublishExt::with_partition_key): it
/// carries the key as a base header and otherwise delegates to the publisher it wraps.
///
/// It is a [`Publisher`] like any other, so the framework's publish builder (`message(..)`)
/// applies to it unchanged.
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
    base: HeaderMap,
}

impl<P: Publisher> Publisher for PartitionKeyed<P> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.inner.publish(msg).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        // The header is the crate's one wire for the key: the envelope, `Partitioned`, and the
        // in-process broker all read it from here.
        Some(&self.base)
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use ruststream::runtime::PublishExt;
    use ruststream::testing::TestableBroker;
    use ruststream::{Broker, HeaderMap, Outgoing, Serialized};

    use super::*;
    use crate::testing::KinesisTestBroker;

    /// Bytes the test already holds encoded: a serialized type publishes them as they are, so
    /// these checks stay about the headers rather than about a codec.
    #[derive(Outgoing, Serialized)]
    struct Payload(Vec<u8>);

    fn payload() -> Payload {
        Payload(b"payload".to_vec())
    }

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
            .message(&payload())
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
    async fn a_key_named_at_the_call_site_wins_over_the_step() {
        let broker = connected().await;
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "named-on-the-call");
        broker
            .publisher()
            .with_partition_key("named-on-the-step")
            .message(&payload())
            .to("jobs")
            .with_headers(headers)
            .publish()
            .await
            .expect("the publish succeeds");

        let published = broker.published("jobs");
        assert_eq!(
            published[0].headers().get_str(PARTITION_KEY_HEADER),
            Some("named-on-the-call")
        );
    }

    #[tokio::test]
    async fn the_steps_key_survives_a_call_that_names_other_headers() {
        let broker = connected().await;
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        broker
            .publisher()
            .with_partition_key("named-on-the-step")
            .message(&payload())
            .to("jobs")
            .with_headers(headers)
            .publish()
            .await
            .expect("the publish succeeds");

        let published = broker.published("jobs");
        assert_eq!(
            published[0].headers().get_str(PARTITION_KEY_HEADER),
            Some("named-on-the-step")
        );
        assert_eq!(published[0].headers().get_str("x-tenant"), Some("acme"));
    }

    #[tokio::test]
    async fn a_publish_without_the_step_keeps_the_header_route() {
        let broker = connected().await;
        let mut headers = HeaderMap::new();
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
