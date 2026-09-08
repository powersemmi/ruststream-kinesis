//! [`KinesisPublisher`], its [`KinesisPublish`] policy, and the crate's publish steps.

use std::future::{Future, ready};
use std::sync::Arc;

use aws_sdk_kinesis::primitives::Blob;
use ruststream::runtime::MapPublisher;
use ruststream::{HeaderMap, OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedKinesisBroker, Core, CoreCell};
use crate::error::{KinesisError, sdk_err};
use crate::message::{PARTITION_KEY_HEADER, encode_envelope};

/// Publishes records to Kinesis streams (the destination is the stream name or ARN).
///
/// The `partition-key` header becomes the record's partition key - the unit of shard routing
/// and per-key ordering. A record that names none takes the key the mount site named on the
/// policy ([`KinesisPublish::partition_key`]), and without that a process-unique key spreads the
/// records across the shards. [`KinesisPublishExt::with_partition_key`] names the header without
/// spelling it out. User headers beyond the partition key travel in a small conditional envelope (Kinesis
/// records carry only a data blob and a partition key); plain payloads stay unenveloped.
/// Buildable before `connect` and usable until `shutdown`; afterwards every publish reports
/// [`KinesisError::NotConnected`].
#[derive(Clone)]
pub struct KinesisPublisher {
    cell: CoreCell,
    /// The partition key the mount site named on the policy this publisher was paired from,
    /// used by every record that does not name one of its own.
    partition_key: Option<Arc<str>>,
}

impl std::fmt::Debug for KinesisPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisPublisher").finish_non_exhaustive()
    }
}

impl KinesisPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self {
            cell,
            partition_key: None,
        }
    }

    /// The publisher a policy pairs into: the same connection, plus what the mount site named
    /// on that policy.
    pub(crate) fn keyed(cell: CoreCell, partition_key: Option<Arc<str>>) -> Self {
        Self {
            cell,
            partition_key,
        }
    }

    fn core(&self) -> Result<&Core, KinesisError> {
        let core = self.cell.get().ok_or(KinesisError::NotConnected)?;
        core.ensure_open()?;
        Ok(core)
    }

    /// The record's partition key: what the message names, else what the mount site named on
    /// the policy, else a process-unique key that spreads the records across the shards.
    ///
    /// Resolved here rather than through [`Publisher::base_headers`], because the key is not a
    /// header on the wire and the base map reaches only publishes that go through the
    /// framework's builder - a reply and an injected slot publish through neither.
    fn partition_key(&self, msg: &OutgoingMessage<'_>) -> String {
        if let Some(named) = msg.headers().get(PARTITION_KEY_HEADER) {
            return String::from_utf8_lossy(named).into_owned();
        }
        self.partition_key
            .as_deref()
            .map_or_else(spread_key, str::to_owned)
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
        let partition_key = self.partition_key(&msg);
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
/// let policy = KinesisPublish::default().partition_key("tenant-acme");
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct KinesisPublish {
    partition_key: Option<String>,
}

impl KinesisPublish {
    /// The partition key every record published through this policy carries.
    ///
    /// The named alternative to setting [`PARTITION_KEY_HEADER`] on each message, for the
    /// publishers a service never holds: the reply of a `publish(..)` handler and the slots it
    /// injects are paired from a policy, so this is where their key is decided. Without one the
    /// records spread across the stream's shards under a process-unique key.
    ///
    /// One key means one shard, and with it one shard's throughput: name it when the records
    /// have to stay mutually ordered, and leave it off otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_kinesis::KinesisPublish;
    ///
    /// let policy = KinesisPublish::default().partition_key("tenant-acme");
    /// # let _ = policy;
    /// ```
    pub fn partition_key(mut self, key: impl Into<String>) -> Self {
        self.partition_key = Some(key.into());
        self
    }

    /// The key a publisher paired from this policy carries.
    fn key(&self) -> Option<Arc<str>> {
        self.partition_key.as_deref().map(Arc::from)
    }
}

impl crate::sealed::PolicyKey for KinesisPublish {
    fn with_partition_key(self, key: String) -> Self {
        self.partition_key(key)
    }
}

impl PublishPolicy<ConnectedKinesisBroker> for KinesisPublish {
    type Live = KinesisPublisher;

    fn pair(
        self,
        connected: &ConnectedKinesisBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher_keyed(self.key())))
    }
}

/// The Kinesis publish settings, chained on a mount site after `.out(marker, policy)`.
///
/// The publish-side mirror of [`KinesisSubscriberExt`](crate::KinesisSubscriberExt): the
/// framework names the position and the policy, and what this transport does with a record
/// beyond that is the broker's own vocabulary. A publisher a handler holds names the same thing
/// with [`KinesisPublishExt::with_partition_key`]; this is for the ones it never holds - the
/// reply of a `publish(..)` handler, and the slots it injects.
///
/// # Examples
///
/// ```
/// use ruststream_kinesis::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
/// # #[derive(Outgoing, serde::Serialize)]
/// # struct Receipt { id: u64 }
///
/// #[subscriber(KinesisStream::new("orders"), publish("receipts"))]
/// async fn confirm(order: &Order) -> Receipt {
///     Receipt { id: order.id }
/// }
///
/// # fn wire() {
/// let _app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///     KinesisBroker::new(),
///     |b| {
///         b.include(confirm)
///             .out(Reply, Publish::default())
///             .partition_key("tenant-acme");
///     },
/// );
/// # }
/// ```
///
/// The settings live on the policy the chain named, so a chain that named another broker's
/// policy - or no policy at all - does not have them.
pub trait KinesisPublishSettings: Sized {
    /// The partition key every record published from this position carries. The mount-site
    /// spelling of [`KinesisPublish::partition_key`].
    // Not `#[must_use]`: on the `with_broker` path a mount chain commits when it drops, so the
    // settled chain is meant to be discarded, exactly as the framework's own steps are.
    #[allow(clippy::return_self_not_must_use)]
    fn partition_key(self, key: impl Into<String>) -> Self;
}

impl<T> KinesisPublishSettings for T
where
    T: MapPublisher<Policy: crate::sealed::PolicyKey>,
{
    fn partition_key(self, key: impl Into<String>) -> Self {
        let key = key.into();
        self.map_publisher(|policy| crate::sealed::PolicyKey::with_partition_key(policy, key))
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

    /// The mount site's key is the bottom of the ladder, not an override: it applies to every
    /// record that names none, and steps aside for one that does.
    #[tokio::test]
    async fn the_mount_sites_key_applies_until_a_publish_names_its_own() {
        let broker = connected().await;
        let publisher = crate::testing::KinesisTestPublish::default()
            .partition_key("named-on-the-mount")
            .pair(&broker)
            .await
            .expect("the policy pairs with the connected broker");

        publisher
            .message(&payload())
            .to("jobs")
            .publish()
            .await
            .expect("the publish succeeds");
        publisher
            .with_partition_key("named-on-the-call")
            .message(&payload())
            .to("jobs")
            .publish()
            .await
            .expect("the publish succeeds");

        let records = broker.published("jobs");
        assert_eq!(
            records[0].headers().get_str(PARTITION_KEY_HEADER),
            Some("named-on-the-mount")
        );
        assert_eq!(
            records[1].headers().get_str(PARTITION_KEY_HEADER),
            Some("named-on-the-call")
        );
    }
}
