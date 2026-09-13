//! [`KinesisPublisher`], its [`KinesisPublish`] policy, and the crate's publish steps.

use std::future::{Future, ready};
use std::sync::Arc;

use aws_sdk_kinesis::primitives::Blob;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::{Binding, Bindings};
use ruststream::runtime::{MapPublisher, PublishBuilder, PublishSink};
use ruststream::{HeaderMap, OutgoingMessage, PairError, PublishPolicy, Publisher};
#[cfg(feature = "asyncapi")]
use serde::Serialize;

#[cfg(feature = "asyncapi")]
use crate::stream::BINDING_KEY;

use crate::broker::{ConnectedKinesisBroker, Core, CoreCell};
use crate::error::{KinesisError, sdk_err};
use crate::message::{PARTITION_KEY_HEADER, encode_envelope};
#[cfg(feature = "testing")]
use crate::testing::{ConnectedKinesisTestBroker, KinesisTestPublisher};

/// The per-message publish settings of this broker: what one call varies, against what the mount
/// site fixed on the policy.
///
/// Every field is optional, so a call says only what it changes, and a field no step touched
/// keeps what the policy holds. A handler body that names a setting bounds its injected slot on
/// this type - `Out<impl Publisher<Options = KinesisPublishOptions>, Marker>` - and takes the
/// steps themselves from [`KinesisPublishSteps`].
///
/// # Examples
///
/// ```
/// use ruststream_kinesis::KinesisPublishOptions;
///
/// let options = KinesisPublishOptions {
///     partition_key: Some("tenant-acme".to_owned()),
/// };
/// # let _ = options;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KinesisPublishOptions {
    /// The record's partition key, named by
    /// [`partition_key`](KinesisPublishSteps::partition_key) on the publish builder.
    pub partition_key: Option<String>,
}

/// Publishes records to Kinesis streams (the destination is the stream name or ARN).
///
/// The record's partition key is the unit of shard routing and per-key ordering. A publish names
/// it with [`partition_key`](KinesisPublishSteps::partition_key) on the publish builder; a record
/// that names none takes the key the mount site named on the policy
/// ([`KinesisPublish::partition_key`]), and without that a process-unique key spreads the records
/// across the shards. User headers beyond the partition key travel in a small conditional envelope
/// (Kinesis records carry only a data blob and a partition key); plain payloads stay unenveloped.
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
}

/// The record's partition key, in the order the four places that can name one resolve.
///
/// The call's step first, then the `partition-key` header a call site wrote by hand (the portable
/// spelling, and what a message crossing from another broker carries), then the key the mount site
/// named on the policy - the only place a reply and an injected slot can be given one - and last a
/// process-unique key, because a record cannot go out without one.
///
/// One function for both transports: the in-process stand-in answers the key the service would,
/// never one of its own.
pub(crate) fn resolve_partition_key(
    options: Option<&KinesisPublishOptions>,
    headers: &HeaderMap,
    from_policy: Option<&str>,
) -> String {
    if let Some(key) = options.and_then(|options| options.partition_key.as_deref()) {
        return key.to_owned();
    }
    if let Some(named) = headers.get(PARTITION_KEY_HEADER) {
        return String::from_utf8_lossy(named).into_owned();
    }
    from_policy.map_or_else(spread_key, str::to_owned)
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
    /// The partition key is the one setting a Kinesis record varies per message, and
    /// [`KinesisPublishSteps`] is where a call site sets it.
    type Options = KinesisPublishOptions;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let core = self.core()?;
        let partition_key =
            resolve_partition_key(options, msg.headers(), self.partition_key.as_deref());
        // The resolved key rides the record's own field, which is this transport's wire for it:
        // a delivery reads it back into `PARTITION_KEY_HEADER`, where the cross-broker
        // `Partitioned` capability finds it. Stamping the header here as well would cost a map
        // copy per publish for a value the envelope drops on the next line.
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
    /// The partition key every record published through this policy carries unless the call
    /// names its own.
    ///
    /// This is the mount site's default, and the only key a reply can be given: a replying
    /// handler returns a value and never reaches a publish builder, so
    /// [`partition_key`](KinesisPublishSteps::partition_key) has no call site there. A slot the
    /// handler publishes through has one, and a step on that call wins over this. Without either
    /// the records spread across the stream's shards under a process-unique key.
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

    /// What the records published through this policy add to their message object in the
    /// generated document.
    ///
    /// The key a single call names is not here: the document describes the mount, and a step on
    /// the publish builder is a property of one record. A policy that names no key says nothing,
    /// so the document carries no field the service did not fix.
    #[cfg(feature = "asyncapi")]
    fn binding(&self) -> Bindings {
        let Some(partition_key) = self.partition_key.as_deref() else {
            return Bindings::new();
        };
        let body = KinesisMessageBinding { partition_key };
        Binding::extension(BINDING_KEY, &body)
            .map_or_else(|_| Bindings::new(), |binding| Bindings::new().with(binding))
    }
}

/// What a record published through [`KinesisPublish`] writes into the generated document.
///
/// The partition key is the record's own routing field, which is where the specification puts an
/// ordering key on the brokers it does cover; the specification lists no Kinesis binding at all,
/// so this travels as an `x-` extension.
#[cfg(feature = "asyncapi")]
#[derive(Debug, Serialize)]
struct KinesisMessageBinding<'a> {
    #[serde(rename = "partitionKey")]
    partition_key: &'a str,
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

    #[cfg(feature = "asyncapi")]
    fn message_bindings(&self) -> Bindings {
        self.binding()
    }
}

/// The same policy pairs with the in-process stand-in, so a routes file mounts unchanged in a
/// unit test: `.out_reply(Publish::default())` names one type whichever broker it runs against,
/// and the key it carries reaches the record here too - the stand-in carries the partition key in
/// the header its deliveries read.
#[cfg(feature = "testing")]
impl PublishPolicy<ConnectedKinesisTestBroker> for KinesisPublish {
    type Live = KinesisTestPublisher;

    fn pair(
        self,
        connected: &ConnectedKinesisTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher_keyed(self.key())))
    }

    /// The same binding the policy writes against the service, so a document built in a unit
    /// test is the document the service publishes.
    #[cfg(feature = "asyncapi")]
    fn message_bindings(&self) -> Bindings {
        self.binding()
    }
}

/// The Kinesis publish settings, chained on a mount site after the position that named the
/// policy: `.out_reply(policy)`, `.out_retry(policy)` or `.out(marker, policy)`.
///
/// The publish-side mirror of [`KinesisSubscriberExt`](crate::KinesisSubscriberExt): the
/// framework names the position and the policy, and what this transport does with a record
/// beyond that is the broker's own vocabulary. A call that reaches the publish builder names its
/// own key with [`KinesisPublishSteps::partition_key`]; this settles the key for every record
/// published from the position, including the reply, which has no call site of its own.
///
/// # Examples
///
/// ```
/// use ruststream_kinesis::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// // The reply type names the stream it goes to; the mount site names how it is published.
/// #[derive(Outgoing, serde::Serialize)]
/// #[outgoing(name = "receipts")]
/// struct Receipt { id: u64 }
///
/// #[subscriber(KinesisStream::new("orders"), publish)]
/// async fn confirm(order: &Order) -> Receipt {
///     Receipt { id: order.id }
/// }
///
/// # fn wire() {
/// let _app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///     KinesisBroker::new(),
///     |b| {
///         b.include(confirm)
///             .out_reply(Publish::default())
///             .partition_key("tenant-acme");
///     },
/// );
/// # }
/// ```
///
/// The settings live on the policy the chain named, so a chain that named another broker's
/// policy - or no policy at all - does not have them.
pub trait KinesisPublishSettings: Sized {
    /// The partition key every record published from this position carries unless the call
    /// names its own. The mount-site spelling of [`KinesisPublish::partition_key`].
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

/// The Kinesis publish steps, on the framework's publish builder.
///
/// A step sets one field of [`KinesisPublishOptions`] for the single record the call sends. It is
/// a position on the builder rather than a value wrapping the publisher, so the record still
/// leaves through the entry the mount site named, carrying that entry's codec and transforms and
/// attributed to that entry's slot.
///
/// The bound is on the publisher's options type, so these steps appear on a builder over a
/// Kinesis publisher and on no other broker's.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "testing")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream_kinesis::prelude::*;
/// use ruststream_kinesis::testing::KinesisTestBroker;
///
/// // The record is already encoded, so it declares itself serialized: no codec runs on it,
/// // and the name still puts it in the generated document.
/// #[derive(Outgoing, Serialized)]
/// struct Job(Vec<u8>);
///
/// KinesisTestBroker::new()
///     .publisher()
///     .message(&Job(br#"{"id":1}"#.to_vec()))
///     .to("jobs")
///     .partition_key("tenant-acme")
///     .publish()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait KinesisPublishSteps {
    /// Sends this one record under `key` as its partition key.
    ///
    /// The partition key picks the shard, and with it the order the record keeps against its
    /// neighbours. It wins over a `partition-key` header the call wrote by hand and over the key
    /// the mount site named on the policy.
    #[must_use]
    fn partition_key(self, key: impl Into<String>) -> Self;
}

impl<Sink, Body, Enc, Hdrs, Dest> KinesisPublishSteps
    for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = KinesisPublishOptions>,
{
    fn partition_key(mut self, key: impl Into<String>) -> Self {
        self.options_mut()
            .get_or_insert_with(KinesisPublishOptions::default)
            .partition_key = Some(key.into());
        self
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

    async fn connected() -> ConnectedKinesisTestBroker {
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
            .message(&payload())
            .to("jobs")
            .partition_key("tenant-acme")
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

    /// The step is the call's own answer, so it beats the header spelling of the same answer.
    #[tokio::test]
    async fn the_step_wins_over_a_key_the_call_site_wrote_by_hand() {
        let broker = connected().await;
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "written-by-hand");
        broker
            .publisher()
            .message(&payload())
            .to("jobs")
            .with_headers(headers)
            .partition_key("named-by-the-step")
            .publish()
            .await
            .expect("the publish succeeds");

        let published = broker.published("jobs");
        assert_eq!(
            published[0].headers().get_str(PARTITION_KEY_HEADER),
            Some("named-by-the-step")
        );
    }

    #[tokio::test]
    async fn the_steps_key_survives_a_call_that_names_other_headers() {
        let broker = connected().await;
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        broker
            .publisher()
            .message(&payload())
            .to("jobs")
            .with_headers(headers)
            .partition_key("named-by-the-step")
            .publish()
            .await
            .expect("the publish succeeds");

        let published = broker.published("jobs");
        assert_eq!(
            published[0].headers().get_str(PARTITION_KEY_HEADER),
            Some("named-by-the-step")
        );
        assert_eq!(published[0].headers().get_str("x-tenant"), Some("acme"));
    }

    /// The portable spelling stays: a message that arrives carrying the header - from another
    /// broker, or from a caller that writes headers by hand - keeps the key it carries.
    #[tokio::test]
    async fn a_publish_without_a_step_keeps_the_header_route() {
        let broker = connected().await;
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "by-hand");
        broker
            .publisher()
            .publish(
                OutgoingMessage::new("jobs", b"payload").with_headers(headers),
                None,
            )
            .await
            .expect("the publish succeeds");

        let published = broker.published("jobs");
        assert_eq!(
            published[0].headers().get_str(PARTITION_KEY_HEADER),
            Some("by-hand")
        );
    }

    /// A record cannot go out without a key, so the bottom of the ladder is a key that spreads
    /// the records rather than no key at all.
    #[tokio::test]
    async fn a_record_no_one_named_a_key_for_still_gets_one() {
        let broker = connected().await;
        broker
            .publisher()
            .message(&payload())
            .to("jobs")
            .publish()
            .await
            .expect("the publish succeeds");

        let published = broker.published("jobs");
        let key = published[0]
            .headers()
            .get_str(PARTITION_KEY_HEADER)
            .expect("the record carries a partition key");
        assert!(
            key.starts_with("rs-"),
            "expected the spreading fallback key, got {key:?}",
        );
    }

    /// The mount site's key is a default, not an override: it applies to every record that names
    /// none, and steps aside for one that does. The policy is the production one, paired against
    /// the stand-in - the same type a routes file names.
    #[tokio::test]
    async fn the_mount_sites_key_applies_until_a_publish_names_its_own() {
        let broker = connected().await;
        let publisher = KinesisPublish::default()
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
            .message(&payload())
            .to("jobs")
            .partition_key("named-on-the-call")
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
