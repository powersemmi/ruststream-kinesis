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
use ruststream::{
    AddressedCopies, RedeliveryAddress, RedeliveryAddressed, StartAt, SubscriptionSource,
};
#[cfg(feature = "asyncapi")]
use serde::Serialize;

use crate::broker::ConnectedKinesisBroker;
use crate::envelope::{ChooseEnvelope, Envelope, Inherit};
use crate::error::KinesisError;
use crate::sealed::EnvelopedSource;
use crate::subscriber::KinesisSubscriber;

/// A subscription descriptor for one Kinesis stream.
///
/// Every shard resumes from its stored checkpoint, and a shard without one starts at the tip
/// (a child whose parent was read to its end starts at its own beginning). To open somewhere else, wrap the descriptor in the framework's `start_at(..)` clause with a
/// [`KinesisPosition`](crate::KinesisPosition).
///
/// A string literal names a stream too: `#[subscriber("orders")]` builds this descriptor with its
/// defaults. An invalid descriptor (an empty stream name, a zero shard count) fails the mount
/// before any I/O, and a stream the service does not have fails it when the subscription opens,
/// unless the descriptor [creates it](Self::create_if_missing).
///
/// The subscription lists the stream's shards and lists them again as splits and merges change
/// the set, takes a lease per shard, and runs one reader per shard it holds. A child of a split
/// or a merge starts only after its parents are fully consumed, which keeps per-key ordering
/// across resharding.
///
/// A broker reads a stream through one subscription: a second subscription to the same stream
/// on one broker is refused when it opens. A second [`KinesisBroker`](crate::KinesisBroker) with
/// a lease store of its own reads the stream again. A KPL-aggregated record is reported as
/// [`KinesisError::AggregatedRecord`](crate::KinesisError::AggregatedRecord).
///
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]`
/// decorator, and [`IntoSource`], so the manual path's
/// [`subscriber`](ruststream::runtime::subscriber) constructor names a stream with it too:
///
/// ```
/// # mod demo {
/// use std::error::Error;
/// use std::time::Duration;
///
/// use ruststream::prelude::*;
/// use ruststream_kinesis::{KinesisBroker, KinesisStream};
/// use schemars::JsonSchema;
/// use serde::Deserialize;
///
/// #[derive(Deserialize, JsonSchema)]
/// struct Order {
///     id: u64,
/// }
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
///         tracing::info!(order.id, "audited");
///         Ok(())
///     }
/// }
///
/// pub async fn run() -> Result<(), Box<dyn Error>> {
///     let orders = KinesisStream::new("orders").poll_interval(Duration::from_millis(500));
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(KinesisBroker::new(), |b| {
///             b.include(subscriber(orders, Audit).build());
///         })
///         .run()
///         .await?;
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct KinesisStream<Env = Inherit> {
    stream: String,
    poll_interval: Duration,
    create_shards: Option<i32>,
    envelope: Env,
}

impl KinesisStream {
    /// Names the stream by its name, which is also the stream half of every [`LeaseKey`](crate::LeaseKey) the
    /// subscription's shards are leased under.
    ///
    /// A stream is named by its name; every call the subscription makes passes it as the stream
    /// name.
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            // The service recommends waiting a second between reads to stay within the
            // per-shard budget.
            poll_interval: Duration::from_secs(1),
            create_shards: None,
            envelope: Inherit,
        }
    }
}

impl<Env> KinesisStream<Env> {
    /// Reads this subscription's records through `envelope` instead of the broker's.
    ///
    /// A stream another system writes is read in that system's layout: [`NoOp`](crate::NoOp)
    /// for raw records, [`Framed`](crate::Framed) for a codec document, an envelope of the
    /// service's own for anything else. The choice is a type, resolved when the app is built.
    ///
    /// # Examples
    ///
    /// The manual path names the envelope on the descriptor; the `#[subscriber(..)]` path names
    /// it at the mount site with [`KinesisEnvelopeExt::envelope`].
    ///
    /// ```
    /// # mod demo {
    /// use std::future::{Future, ready};
    ///
    /// use ruststream_kinesis::prelude::*;
    /// use schemars::JsonSchema;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize, JsonSchema)]
    /// struct Reading {
    ///     celsius: f64,
    /// }
    ///
    /// /// Logs a reading. Devices write plain JSON records through an AWS IoT rule.
    /// struct Log;
    ///
    /// impl Handle<Reading> for Log {
    ///     fn handle(
    ///         &self,
    ///         reading: &Reading,
    ///         _outs: &(),
    ///         _ctx: &mut Context<'_>,
    ///     ) -> impl Future<Output = Result<(), HandlerOutcome>> {
    ///         tracing::info!(reading.celsius, "reading");
    ///         ready(Ok(()))
    ///     }
    /// }
    ///
    /// pub fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("readings", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
    ///         let readings = KinesisStream::new("readings").envelope(NoOp);
    ///         b.include(subscriber(readings, Log).build());
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn envelope<Named: Envelope>(self, envelope: Named) -> KinesisStream<Named> {
        KinesisStream {
            stream: self.stream,
            poll_interval: self.poll_interval,
            create_shards: self.create_shards,
            envelope,
        }
    }

    /// The descriptor's settings and its envelope apart: the reader needs only the first.
    pub(crate) fn split(self) -> (KinesisStream, Env) {
        let Self {
            stream,
            poll_interval,
            create_shards,
            envelope,
        } = self;
        (
            KinesisStream {
                stream,
                poll_interval,
                create_shards,
                envelope: Inherit,
            },
            envelope,
        )
    }

    /// The pause between reads on an idle shard. Defaults to 1 second, the service's own
    /// recommendation; lower values spend the 5-reads-per-second budget faster.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Creates the stream with `shards` provisioned shards on subscribe when it does not
    /// exist yet, then waits up to a minute for it to become active. Meant for local development
    /// and tests; production streams are usually managed as infrastructure.
    ///
    /// Without it a stream the service does not have refuses the subscription when it opens,
    /// so the name is checked while a service is starting rather than after it has started.
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
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream_kinesis::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[subscriber(KinesisStream::new("jobs"))]
/// async fn work(batch: &[Job]) -> Vec<HandlerOutcome> {
///     batch
///         .iter()
///         .map(|job| {
///             tracing::info!(job.id, "done");
///             HandlerOutcome::ack()
///         })
///         .collect()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
///         b.include(
///             work.batch(nonzero!(500))
///                 .poll_interval(Duration::from_millis(500))
///                 .create_if_missing(1),
///         );
///     })
/// }
/// # }
/// # fn main() {}
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

impl<Def, State, DefCodec, Env> KinesisSubscriberExt
    for SubscriberBuilder<Def, KinesisStream<Env>, State, DefCodec>
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

/// The envelope a subscription reads in, named at the mount site.
///
/// The `#[subscriber(..)]` attribute takes the descriptor's type from its constructor, so a
/// descriptor whose envelope changes its type is named here instead, after the handler, the
/// way the framework's own steps are. The manual path names it on the descriptor itself,
/// [`KinesisStream::envelope`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_kinesis::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Click {
///     page: String,
/// }
///
/// #[subscriber(KinesisStream::new("clicks"))]
/// async fn on_click(click: &Click) -> HandlerOutcome {
///     tracing::info!(click.page, "click");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("clicks", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
///         // A browser SDK writes the clicks as plain JSON records.
///         b.include(on_click.envelope(NoOp));
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub trait KinesisEnvelopeExt: Sized {
    /// The mount chain reading in `Named`.
    type Enveloped<Named: Envelope>;

    /// Reads this subscription's records through `envelope` instead of the broker's. The
    /// mount-site spelling of [`KinesisStream::envelope`].
    #[must_use]
    fn envelope<Named: Envelope>(self, envelope: Named) -> Self::Enveloped<Named>;
}

impl<Decl, Def, Source, State, DefCodec> KinesisEnvelopeExt for Decl
where
    Decl: Declared<Settings = SubscriberBuilder<Def, Source, State, DefCodec>>,
    Def: Declared,
    Source: EnvelopedSource,
{
    type Enveloped<Named: Envelope> =
        SubscriberBuilder<Def, Source::Enveloped<Named>, State, DefCodec>;

    fn envelope<Named: Envelope>(self, envelope: Named) -> Self::Enveloped<Named> {
        self.declare()
            .map_source(|source| source.with_envelope(envelope))
    }
}

impl<Env> EnvelopedSource for KinesisStream<Env> {
    type Enveloped<Named: Envelope> = KinesisStream<Named>;

    fn with_envelope<Named: Envelope>(self, envelope: Named) -> KinesisStream<Named> {
        self.envelope(envelope)
    }
}

/// A start position wraps the descriptor, so the envelope reaches through it: the two steps
/// compose in either order, and after a position the attribute named.
impl<Env, Position> EnvelopedSource for StartAt<KinesisStream<Env>, Position> {
    type Enveloped<Named: Envelope> = StartAt<KinesisStream<Named>, Position>;

    fn with_envelope<Named: Envelope>(self, envelope: Named) -> Self::Enveloped<Named> {
        self.map_inner(|stream| stream.envelope(envelope))
    }
}

/// The manual path's constructor takes a subject string or a source, and this is what makes the
/// crate's own descriptor the second: a service without the `macros` feature names its stream,
/// and the settings that price a read, exactly as an attribute one does.
impl<Env> IntoSource for KinesisStream<Env> {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

/// What a Kinesis subscription writes into the generated document.
///
/// The `AsyncAPI` specification lists no Kinesis binding, and the protocol keys of a binding object
/// are a closed list, so this travels as an `x-` extension at the same level. Only what the
/// descriptor itself holds goes in, because the document is built before anything connects.
#[cfg(feature = "asyncapi")]
#[derive(Debug, Serialize)]
struct KinesisChannelBinding<'a> {
    stream: &'a str,
    #[serde(rename = "pollIntervalMs")]
    poll_interval_ms: u64,
    /// The shards the descriptor would provision, present only where it creates the stream.
    #[serde(rename = "shardCount", skip_serializing_if = "Option::is_none")]
    shard_count: Option<i32>,
    /// The envelope the descriptor names, present only where it names one.
    #[serde(skip_serializing_if = "Option::is_none")]
    envelope: Option<EnvelopeBinding>,
}

/// The envelope a descriptor, a publish policy or a broker names, as the generated document
/// reports it: its name, and the media type of the record's data where the envelope frames the
/// payload in a document of its own.
#[cfg(feature = "asyncapi")]
#[derive(Debug, Serialize)]
pub(crate) struct EnvelopeBinding {
    name: &'static str,
    #[serde(rename = "contentType", skip_serializing_if = "Option::is_none")]
    content_type: Option<&'static str>,
}

#[cfg(feature = "asyncapi")]
impl EnvelopeBinding {
    /// The binding of what a mount site names, or nothing where it reads the broker's.
    pub(crate) fn declared<Named, Default>() -> Option<Self>
    where
        Named: ChooseEnvelope<Default>,
        Default: Envelope,
    {
        Named::declared().map(|(name, content_type)| Self { name, content_type })
    }

    /// The binding of an envelope itself.
    pub(crate) const fn of<Env: Envelope>() -> Self {
        Self {
            name: Env::NAME,
            content_type: Env::CONTENT_TYPE,
        }
    }
}

/// The extension key this crate writes its bindings under, on the channel and on the message.
#[cfg(feature = "asyncapi")]
pub(crate) const BINDING_KEY: &str = "x-ruststream-kinesis";

impl<Env> KinesisStream<Env> {
    /// The channel binding of this descriptor on a broker whose envelope is `Default`, or nothing
    /// when it cannot be built.
    ///
    /// A document is a description of the service, so a binding that fails to serialize is one
    /// the document goes without rather than a start-up the service loses.
    #[cfg(feature = "asyncapi")]
    fn binding<Default: Envelope>(&self) -> Bindings
    where
        Env: ChooseEnvelope<Default>,
    {
        let body = KinesisChannelBinding {
            stream: &self.stream,
            poll_interval_ms: u64::try_from(self.poll_interval.as_millis()).unwrap_or(u64::MAX),
            shard_count: self.create_shards,
            envelope: EnvelopeBinding::declared::<Env, Default>(),
        };
        Binding::extension(BINDING_KEY, &body)
            .map_or_else(|_| Bindings::new(), |binding| Bindings::new().with(binding))
    }
}

impl<Env, Default> SubscriptionSource<ConnectedKinesisBroker<Default>> for KinesisStream<Env>
where
    Env: ChooseEnvelope<Default>,
    Default: Envelope,
{
    type Subscriber = KinesisSubscriber<Env::Chosen>;
    /// Kinesis moves nothing on its own: it holds no redelivery timer, no delivery counter and
    /// no dead-letter mechanism, so the framework publishes every copy. A stream is what a
    /// subscription reads and what a publish writes to, so this descriptor knows where a copy
    /// goes.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.stream
    }

    async fn subscribe(
        self,
        connected: &ConnectedKinesisBroker<Default>,
    ) -> Result<KinesisSubscriber<Env::Chosen>, KinesisError> {
        connected.subscribe_stream(self).await
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        self.binding::<Default>()
    }
}

impl<Env, Default> RedeliveryAddressed<ConnectedKinesisBroker<Default>> for KinesisStream<Env>
where
    Env: ChooseEnvelope<Default>,
    Default: Envelope,
{
    /// The stream this descriptor names, which is where a deferred copy and a spent delivery of
    /// this registration are published.
    ///
    /// The copy keeps the record's partition key and lands on that key's shard, at the tip rather
    /// than in the place the original held, so a deferred record loses its position in the
    /// stream's order. Where that order matters, declare no cap and let `HandlerOutcome::retry()` hold the
    /// shard: the checkpoint stops at the record, and the shard is read again from it.
    ///
    /// `max_attempts(n)` counts the first delivery, and under it `HandlerOutcome::retry()` becomes
    /// a published copy as well, because only a copy carries the attempt count forward.
    /// `dead_letter(name)` is the stream a spent record leaves for, payload and headers as it
    /// arrived; a cap declared without one checkpoints past the spent record, as `drop()` does.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use std::time::Duration;
    ///
    /// use ruststream_kinesis::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// /// Asks for a pause when the order cannot be settled yet, and counts nothing: how many pauses
    /// /// it gets is the mount site's to say.
    /// #[subscriber(KinesisStream::new("orders"))]
    /// async fn reconcile(order: &Order) -> HandlerOutcome {
    ///     if order.id.is_multiple_of(2) {
    ///         HandlerOutcome::ack()
    ///     } else {
    ///         HandlerOutcome::retry_after(Duration::from_secs(30))
    ///     }
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
    ///         // Four deliveries per order, then the record leaves for a stream an operator reads.
    ///         b.include(reconcile)
    ///             .max_attempts(nonzero!(4))
    ///             .dead_letter("orders.unsettled");
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    fn redelivery_address(
        &self,
        _connected: &ConnectedKinesisBroker<Default>,
    ) -> impl Future<Output = Result<RedeliveryAddress, KinesisError>> + Send {
        // The stream name is the answer, so nothing is asked of the broker and nothing is awaited.
        ready(Ok(RedeliveryAddress::new(self.stream.clone())))
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
