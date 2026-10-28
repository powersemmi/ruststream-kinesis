//! [`KinesisMessage`]: a delivered record whose acknowledgement is a checkpoint.

// Without the `testing` feature a settlement has one variant, so a `match` on it has a single arm;
// the match stays so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use std::sync::{Arc, OnceLock};

use futures::{Stream, StreamExt};
use ruststream::{
    AckError, HeaderMap, IncomingMessage, Partitioned, Positioned, Seekable, Str, Subscriber,
};

use crate::envelope::{Body, Envelope, Rsk};

use crate::error::KinesisError;
#[cfg(feature = "testing")]
use crate::in_process::Release;
use crate::lease::{LeaseKey, LeaseStore};
use crate::subscriber::{Deliveries, KinesisSeeker};
use crate::track::Watermark;

/// Header carrying the partition key, mapped onto the record's own partition key.
///
/// Every delivery carries it, and it is where `IncomingMessage::partition_key` and the
/// framework's per-key dispatch find the key. On a publish it is the portable way to name the key.
///
/// Mirrors the in-memory broker's convention, so services can switch brokers without changing
/// their headers.
pub const PARTITION_KEY_HEADER: &str = "partition-key";

/// Header exposing the record's sequence number; every delivery carries it.
pub const SEQUENCE_HEADER: &str = "kinesis-sequence-number";

/// Header exposing the shard a record arrived on; every delivery carries it.
pub const SHARD_HEADER: &str = "kinesis-shard-id";

/// The KPL aggregation magic prefix; such records are refused loudly (deaggregation is a
/// follow-up), never handed to a handler as opaque protobuf.
pub(crate) const KPL_MAGIC: [u8; 4] = [0xF3, 0x89, 0x9A, 0xC2];

/// A position in the stream's retained log: the whole start vocabulary of this broker,
/// accepted by [`Seeker::seek`](ruststream::Seeker::seek) and by the `start_at(..)` clause of
/// `#[subscriber(..)]`.
///
/// Repositioning resets the checkpoint bookkeeping of every shard it moves: acknowledgements
/// of records delivered before the seek stop advancing the watermark, so a stale checkpoint
/// cannot drag the cursor back over the position just taken. Records from the new position
/// onward are delivered again, which at-least-once permits.
///
/// Without a position a subscription resumes from the stored checkpoint of each shard, and
/// starts at the tip on a shard that has none. A position given to `start_at(..)` applies to
/// the shards the subscription opens with; a shard it takes on later resumes from its
/// checkpoint.
///
/// Opening a subscription returns once every shard it took has its reader and its starting
/// cursor, so the tip is the moment the subscription opened: a record published after that is
/// read.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_kinesis::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// /// Rebuilds a read model from every record the stream still retains, on every shard.
/// #[subscriber(KinesisStream::new("orders"), start_at(KinesisPosition::horizon()))]
/// async fn backfill(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "backfilled");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
///         b.include(backfill);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KinesisPosition {
    /// The trim horizon: everything the stream still retains.
    ///
    /// Stream-wide - it applies to every shard the subscription reads when it lands, and to a
    /// shard whose reader starts later without a checkpoint.
    Horizon,
    /// The tip: only records published after the reposition.
    ///
    /// Stream-wide, and the position a shard without a checkpoint starts at by default. A child
    /// of a split or a merge whose parent was read to its end opens at its own beginning
    /// instead, so nothing written to it before its reader started is skipped.
    Latest,
    /// The first record at or after this timestamp, in milliseconds since the Unix epoch.
    ///
    /// Stream-wide; each shard opens at its own first record from that instant. The instant is
    /// matched against the arrival timestamp the service stamped on the record, and the service
    /// calls that stamp approximate: records that arrived around the same moment are not
    /// separated exactly.
    Timestamp(u64),
    /// Exactly one record on one shard.
    ///
    /// This is the pinned form the framework defines for captured positions
    /// ([`Positioned::position`]): seeking to one redelivers that very record. It addresses a
    /// single shard, so it moves that shard's reader only, the way a partitioned log seeks
    /// per partition, and the shard must be owned and live.
    Sequence {
        /// The shard the position lives on.
        shard: String,
        /// The record's sequence number.
        sequence: String,
    },
}

impl KinesisPosition {
    /// The trim horizon, for every shard: see [`KinesisPosition::Horizon`].
    #[must_use]
    pub const fn horizon() -> Self {
        Self::Horizon
    }

    /// The tip, for every shard: see [`KinesisPosition::Latest`].
    #[must_use]
    pub const fn latest() -> Self {
        Self::Latest
    }

    /// A wall-clock instant (milliseconds since the Unix epoch), for every shard: see
    /// [`KinesisPosition::Timestamp`].
    #[must_use]
    pub const fn timestamp(millis: u64) -> Self {
        Self::Timestamp(millis)
    }

    /// One record on one shard: see [`KinesisPosition::Sequence`].
    ///
    /// Captured positions come from [`Positioned::position`]; this constructor is for a
    /// sequence number carried in from elsewhere (an operator's replay request, say).
    #[must_use]
    pub fn sequence(shard: impl Into<String>, sequence: impl Into<String>) -> Self {
        Self::Sequence {
            shard: shard.into(),
            sequence: sequence.into(),
        }
    }
}

pub(crate) struct Settlement {
    pub(crate) tracker: Arc<Watermark>,
    pub(crate) index: u64,
    pub(crate) store: Arc<dyn LeaseStore>,
    // The lease key and the owner are per-reader constants stamped onto every record it
    // forwards, and the per-delivery context reads the shard back: sharing them keeps both paths
    // to reference-count bumps instead of a string copy per record.
    pub(crate) lease: Arc<LeaseKey>,
    pub(crate) owner: Arc<str>,
    /// The reader's delivery generation at delivery time; a seek bumps the shared gate, and
    /// stale settlements skip checkpointing (the watermark was reset).
    pub(crate) epoch: u64,
    pub(crate) gate: Arc<std::sync::atomic::AtomicU64>,
}

/// How a delivery settles: against its shard's watermark and the lease store, or, under the
/// `testing` feature, against the in-process transport the test harness connected instead.
///
/// Without the feature there is one variant, so the type is the checkpoint settlement itself and
/// every `match` on it resolves at compile time: a production build carries no second settlement
/// and no branch to it.
pub(crate) enum Settle {
    Checkpoint(Settlement),
    #[cfg(feature = "testing")]
    InProcess(Release),
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives the
// settlement exactly the size of the checkpoint it wraps.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Settle>() == size_of::<Settlement>());

impl Settle {
    /// The stream the record was read from, which names the subscription.
    fn stream(&self) -> &str {
        match self {
            Self::Checkpoint(settlement) => settlement.lease.stream(),
            #[cfg(feature = "testing")]
            Self::InProcess(release) => release.stream(),
        }
    }

    fn shard(&self) -> &Arc<str> {
        match self {
            Self::Checkpoint(settlement) => settlement.lease.shard_shared(),
            #[cfg(feature = "testing")]
            Self::InProcess(release) => release.shard(),
        }
    }
}

/// A record as a reader received it, before an envelope reads it: the data blob and the
/// partition key as the service returned them, and what settles the delivery.
pub(crate) struct Received {
    pub(crate) data: Vec<u8>,
    pub(crate) partition_key: String,
    pub(crate) sequence: Arc<str>,
    pub(crate) seeker: KinesisSeeker,
    pub(crate) settlement: Settle,
}

/// A record delivered by a [`KinesisSubscriber`](crate::KinesisSubscriber), read through the
/// subscription's [`Envelope`].
///
/// The delivery keeps the record as it arrived. The envelope finds the payload when the delivery
/// is made; the headers are read on the first call that asks for them and kept for the rest of
/// the delivery, so a handler that reads no header never reads the envelope's header block.
///
/// Acknowledgement is a per-shard checkpoint, not per-message settlement: `ack` marks this
/// record handled, and when every earlier record on the shard is handled too, the watermark
/// advances and is persisted to the lease store. `nack(requeue = true)` answers
/// [`AckError::Unsupported`]: a sharded log repositions, it cannot requeue one message. The record
/// stays unhandled, so the watermark stops advancing, and the records from it onward are delivered
/// again when the shard's lease is next taken. `nack(requeue = false)` skips the record
/// (checkpoints past it).
///
/// In a handler, `HandlerOutcome::ack()` is the ack, `HandlerOutcome::drop()` the skip, and
/// `HandlerOutcome::retry()` the requeue, unless the registration declares `max_attempts(..)` or
/// a dead-letter stream: then the retry publishes a counted copy of the record back to its stream.
pub struct KinesisMessage<Env = Rsk> {
    received: Received,
    body: Body,
    headers: OnceLock<HeaderMap>,
    envelope: Env,
}

impl<Env> std::fmt::Debug for KinesisMessage<Env> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("KinesisMessage");
        match &self.received.settlement {
            Settle::Checkpoint(settlement) => {
                debug.field("stream", &settlement.lease.stream());
            }
            #[cfg(feature = "testing")]
            Settle::InProcess(_) => {}
        }
        debug
            .field("shard", self.received.settlement.shard())
            .field("record_len", &self.received.data.len())
            .finish_non_exhaustive()
    }
}

impl<Env: Envelope> KinesisMessage<Env> {
    /// The delivery of a received record, its payload found by `envelope`.
    pub(crate) fn open(received: Received, envelope: Env) -> Self {
        let body = envelope.body(&received.data).fit(received.data.len());
        Self {
            received,
            body,
            headers: OnceLock::new(),
            envelope,
        }
    }

    /// The record's headers: what the envelope carries, then the three the record carries
    /// natively, which win over a header of the same name in the envelope.
    fn read_headers(&self) -> HeaderMap {
        let received = &self.received;
        let mut headers = HeaderMap::new();
        if self
            .envelope
            .read_headers(&received.data, &self.body, &mut headers)
            .is_err()
        {
            // A header block that does not parse costs the delivery its envelope headers, never
            // its payload.
            headers = HeaderMap::new();
            tracing::warn!(
                stream = received.settlement.stream(),
                shard = %received.settlement.shard(),
                envelope = Env::NAME,
                "the record's header block does not parse; it is delivered without its headers"
            );
        }
        headers.insert(
            Str::from_static(PARTITION_KEY_HEADER),
            received.partition_key.clone(),
        );
        headers.insert(
            Str::from_static(SEQUENCE_HEADER),
            received.sequence.to_string(),
        );
        headers.insert(
            Str::from_static(SHARD_HEADER),
            received.settlement.shard().to_string(),
        );
        headers
    }
}

impl<Env> KinesisMessage<Env> {
    /// The shard this record arrived on; the per-delivery context borrows it.
    pub(crate) fn shard(&self) -> &Arc<str> {
        self.received.settlement.shard()
    }

    /// This record's sequence number; the per-delivery context borrows it.
    pub(crate) fn sequence(&self) -> &Arc<str> {
        &self.received.sequence
    }

    /// The subscription's reposition handle, minted once when the subscription opened.
    pub(crate) fn seeker(&self) -> &KinesisSeeker {
        &self.received.seeker
    }

    async fn settle(self) -> Result<(), AckError> {
        let settlement = match self.received.settlement {
            Settle::Checkpoint(settlement) => settlement,
            // Nothing is checkpointed in process: a handled record is simply consumed.
            #[cfg(feature = "testing")]
            Settle::InProcess(_) => return Ok(()),
        };
        let Settlement {
            tracker,
            index,
            store,
            lease,
            owner,
            epoch,
            gate,
        } = settlement;
        if gate.load(std::sync::atomic::Ordering::Acquire) != epoch {
            // The subscription repositioned after this delivery: its watermark was reset,
            // and a stale checkpoint would move the cursor somewhere the seek just left.
            return Ok(());
        }
        let Some(sequence) = tracker.settle(index) else {
            return Ok(()); // handled, but the watermark waits on an earlier record
        };
        match store.checkpoint(&lease, &owner, &sequence).await {
            // A fenced checkpoint (another owner took the shard) is fine: the record was
            // handled, and the new owner replays from its checkpoint, which at-least-once
            // permits.
            Ok(_) => Ok(()),
            Err(err) => Err(AckError::Broker(err)),
        }
    }
}

impl<Env: Envelope> Positioned for KinesisMessage<Env> {
    type Position = KinesisPosition;

    fn position(&self) -> KinesisPosition {
        KinesisPosition::sequence(&**self.shard(), &*self.received.sequence)
    }
}

impl<Env: Envelope> Partitioned for KinesisMessage<Env> {
    fn partition_key(&self) -> Option<&[u8]> {
        Some(self.received.partition_key.as_bytes())
    }
}

impl<Env: Envelope> IncomingMessage for KinesisMessage<Env> {
    fn payload(&self) -> &[u8] {
        self.body.payload(&self.received.data)
    }

    fn headers(&self) -> &HeaderMap {
        self.headers.get_or_init(|| self.read_headers())
    }

    async fn ack(self) -> Result<(), AckError> {
        self.settle().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        if requeue {
            // A shard is read in order and the service holds no redelivery of its own, so there
            // is nothing to requeue one record into. The record stays unhandled: the watermark
            // stops at it, and the shard is read again from it when its lease is next taken.
            Err(AckError::Unsupported)
        } else {
            self.settle().await
        }
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}

/// The deliveries of a subscription, each record read through the subscription's envelope as it
/// leaves the transport. This is the one place the envelope meets a record, so the readers and
/// the in-process log stay the same for every envelope.
pub(crate) struct Opened<Env> {
    pub(crate) deliveries: Deliveries,
    pub(crate) envelope: Env,
}

impl<Env> std::fmt::Debug for Opened<Env> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Opened").finish_non_exhaustive()
    }
}

impl<Env: Envelope> Seekable for Opened<Env> {
    type Seeker = KinesisSeeker;

    fn seeker(&self) -> KinesisSeeker {
        self.deliveries.seeker()
    }
}

impl<Env: Envelope> Subscriber for Opened<Env> {
    type Message = KinesisMessage<Env>;
    type Error = KinesisError;

    fn stream(
        &mut self,
    ) -> impl Stream<Item = Result<KinesisMessage<Env>, KinesisError>> + Send + '_ {
        let envelope = &self.envelope;
        self.deliveries.stream().map(move |received| {
            received.map(|received| KinesisMessage::open(received, envelope.clone()))
        })
    }
}
