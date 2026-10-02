//! The delivery context this broker publishes, and the keys that read it.
//!
//! Kinesis is a repositionable log, so a handler may want two things the payload does not carry:
//! where this record sits, and the handle that moves the subscription. Both ride the framework's
//! typed context - [`KinesisContext`] per delivery, [`KinesisBatchContext`] per batch - and are
//! read by the compile-time keys [`Position`] and [`SeekHandle`], so a key this broker does not
//! carry is a compile error rather than a lookup that finds nothing.

use std::sync::Arc;

use ruststream::{BuildBatchContext, BuildContext, ContextField, Field};

use crate::message::{KinesisMessage, KinesisPosition};
use crate::subscriber::KinesisSeeker;

/// This broker's per-delivery context: where the record sits in the log, and the handle that
/// repositions the subscription it arrived on.
///
/// The runtime builds one per dispatched record, so a handler names it as the context type of
/// its body and reads the fields by key - [`Position`] and [`SeekHandle`] - through the
/// `Ctx` extractor or `ctx.context(..)`. Building it costs three reference-count bumps: the
/// shard id and the sequence number are shared with the delivery, and the seeker is the one the
/// subscription minted when it opened.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_kinesis::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// /// Logs where each job sits, and follows the tip from the marker job on.
/// #[subscriber(KinesisStream::new("jobs"))]
/// async fn follow(job: &Job, ctx: &mut Context<'_, KinesisContext>) -> HandlerOutcome {
///     println!("job {} sits at {:?}", job.id, ctx.context(Position));
///     if job.id == u64::MAX
///         && ctx
///             .context(SeekHandle)
///             .seek(KinesisPosition::latest())
///             .await
///             .is_err()
///     {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
///         b.include(follow);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone)]
pub struct KinesisContext {
    shard: Arc<str>,
    sequence: Arc<str>,
    seeker: KinesisSeeker,
}

impl BuildContext<KinesisMessage> for KinesisContext {
    fn build(msg: &KinesisMessage) -> Self {
        Self {
            shard: Arc::clone(msg.shard()),
            sequence: Arc::clone(msg.sequence()),
            seeker: msg.seeker().clone(),
        }
    }
}

/// This broker's batch context: the subscription's reposition handle, shared by every record of
/// the batch.
///
/// A batch spans many records, so there is no single position to report here - a body that reacts
/// to one reads it off the elements (the `kinesis-sequence-number` and `kinesis-shard-id`
/// headers each record carries). Keeping this a type of its own is what makes that hold at
/// compile time: [`KinesisContext`] does not build per batch, so a batch body cannot ask for
/// per-delivery fields.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_kinesis::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// /// A batch carrying the rewind marker moves the whole subscription once it settles.
/// #[subscriber(KinesisStream::new("jobs"))]
/// async fn rewind(batch: &[Job], ctx: &mut Context<'_, KinesisBatchContext>) -> HandlerOutcome {
///     if batch.iter().any(|job| job.id == u64::MAX)
///         && ctx
///             .context(SeekHandle)
///             .seek(KinesisPosition::horizon())
///             .await
///             .is_err()
///     {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
///         b.include(rewind.batch(nonzero!(100)));
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone)]
pub struct KinesisBatchContext {
    seeker: KinesisSeeker,
}

impl BuildBatchContext<KinesisMessage> for KinesisBatchContext {
    fn build(first: &KinesisMessage) -> Self {
        Self {
            seeker: first.seeker().clone(),
        }
    }
}

/// The key reading this record's [`KinesisPosition`] out of [`KinesisContext`].
///
/// The value is the pinned shard-scoped form: seeking back to it redelivers exactly this record.
///
/// # Examples
///
/// ```
/// use ruststream::prelude::*;
/// use ruststream::subscriber;
/// use ruststream_kinesis::{KinesisStream, Position};
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64 }
///
/// #[subscriber(KinesisStream::new("jobs"))]
/// async fn audit(job: &Job, Ctx(at): Ctx<Position>) -> HandlerOutcome {
///     println!("job {} sits at {at:?}", job.id);
///     HandlerOutcome::ack()
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Position;

impl ContextField for Position {
    type Context = KinesisContext;
    type Value = KinesisPosition;

    fn read(self, src: &KinesisContext) -> KinesisPosition {
        KinesisPosition::sequence(&*src.shard, &*src.sequence)
    }
}

impl Field<KinesisContext> for Position {
    // Owned rather than borrowed: the position is assembled from the two shared strings, so
    // there is nothing in the context to hand back a reference to.
    type Value<'a> = KinesisPosition;

    fn get(self, src: &KinesisContext) -> KinesisPosition {
        self.read(src)
    }
}

/// The key reading the subscription's [`KinesisSeeker`] out of either context.
///
/// The handle is the subscription's own, minted when it opened, so a stream-wide position moves
/// every shard it owns and a captured one moves the shard it names.
///
/// # Examples
///
/// ```
/// use ruststream::prelude::*;
/// use ruststream::{Seeker, subscriber};
/// use ruststream_kinesis::{KinesisPosition, KinesisStream, SeekHandle};
/// # #[derive(serde::Deserialize)]
/// # struct Job { id: u64, replay_from_horizon: bool }
///
/// /// Rewinds the whole subscription when the producer asks for a replay.
/// #[subscriber(KinesisStream::new("jobs"))]
/// async fn work(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
///     if job.replay_from_horizon && seeker.seek(KinesisPosition::horizon()).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct SeekHandle;

impl ContextField for SeekHandle {
    type Context = KinesisContext;
    type Value = KinesisSeeker;

    fn read(self, src: &KinesisContext) -> KinesisSeeker {
        src.seeker.clone()
    }
}

impl Field<KinesisContext> for SeekHandle {
    type Value<'a> = &'a KinesisSeeker;

    fn get(self, src: &KinesisContext) -> &KinesisSeeker {
        &src.seeker
    }
}

impl Field<KinesisBatchContext> for SeekHandle {
    type Value<'a> = &'a KinesisSeeker;

    fn get(self, src: &KinesisBatchContext) -> &KinesisSeeker {
        &src.seeker
    }
}
