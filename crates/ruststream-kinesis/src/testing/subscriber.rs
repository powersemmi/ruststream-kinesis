//! [`KinesisTestSubscriber`] and [`KinesisTestMessage`].
//!
//! The subscription reads its stream's retained log. It opens at the tip, the way a shard
//! without a checkpoint does, and a reposition - from `start_at(..)` at startup or from a
//! handler's [`SeekHandle`](crate::SeekHandle) while it runs - swaps its queue for the log
//! suffix at the target. Like the real reader, the swap happens inside the subscriber's own
//! poll, where the mutable borrow of the receiver is available, so no state is buffered between
//! polls and the stream stays cancel-safe.
//!
//! Batches come from the framework's own adapter over that stream, so a batch mount honours the
//! size it names here exactly as it does against the service.

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;

use ruststream::testing::Coordinator;
use ruststream::{
    AckError, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage, Partitioned,
    Positioned, Seekable, Subscriber,
};

use crate::error::KinesisError;
use crate::message::{
    KinesisPosition, PARTITION_KEY_HEADER, SEQUENCE_HEADER, SHARD_HEADER, decode_envelope,
};
use crate::subscriber::KinesisSeeker;
use crate::testing::broker::TestState;
use crate::testing::router::{Delivery, DeliveryReceiver, DeliverySender, SubscriptionId};
use crate::testing::seek::{IN_PROCESS_SHARD, LogSeeker, SeekControl, sequence_of};

/// How long a partial batch waits for more deliveries before it goes out. Short, because nothing
/// here leaves the process: it only has to outlast the fanout of a replay.
const BATCH_FILL_WINDOW: Duration = Duration::from_millis(50);

/// Subscriber returned by [`ConnectedKinesisTestBroker`](crate::testing::ConnectedKinesisTestBroker).
///
/// Dropping it unregisters the subscription, so handlers stop receiving as soon as their task
/// finishes.
pub struct KinesisTestSubscriber {
    address: Arc<str>,
    deliveries: BufferedSubscriber<LogDeliveries>,
}

/// One subscription's delivery queue over the retained log: what the batch adapter above groups,
/// and what a single-message mount reads directly.
struct LogDeliveries {
    state: Arc<TestState>,
    id: SubscriptionId,
    address: Arc<str>,
    rx: DeliveryReceiver,
    requeue: DeliverySender,
    control: Arc<SeekControl>,
    /// Minted once, before the first delivery: every record carries a clone, so a handler can
    /// reposition this subscription from its delivery context.
    seeker: KinesisSeeker,
    /// A clone of the broker's harness coordinator, threaded into each yielded message so a
    /// requeue re-counts and a consumed delivery decrements. `None` outside a harness run.
    coordinator: Option<Coordinator>,
}

impl std::fmt::Debug for KinesisTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisTestSubscriber")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for LogDeliveries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogDeliveries")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl KinesisTestSubscriber {
    pub(crate) fn new(state: Arc<TestState>, address: &str) -> Self {
        Self {
            address: Arc::from(address),
            deliveries: BufferedSubscriber::new(LogDeliveries::new(state, address))
                .max_wait(BATCH_FILL_WINDOW),
        }
    }
}

impl LogDeliveries {
    fn new(state: Arc<TestState>, address: &str) -> Self {
        let registration = state.router.subscribe(address.to_owned());
        let address: Arc<str> = Arc::from(address);
        let seeker = KinesisSeeker::in_process(LogSeeker::new(
            Arc::clone(&state),
            Arc::clone(&address),
            Arc::clone(&registration.control),
        ));
        let coordinator = state.coordinator().cloned();
        Self {
            state,
            id: registration.id,
            address,
            rx: registration.deliveries,
            requeue: registration.requeue,
            control: registration.control,
            seeker,
            coordinator,
        }
    }

    /// Applies a reposition requested through this subscription's seek handle, if one is
    /// pending: everything queued before the seek is dropped, and the retained suffix from the
    /// target on is enqueued in its place.
    ///
    /// The swap itself belongs to the router, because that is where fanout is serialised: a
    /// record published beside a reposition has to land wholly on one side of it, and only the
    /// owner of the log can promise that.
    fn apply_pending_seek(&mut self) {
        let Some(plan) = self.control.take_pending() else {
            return;
        };
        self.state.router.reposition(
            &self.address,
            plan,
            &mut self.rx,
            &self.requeue,
            self.coordinator.as_ref(),
        );
    }
}

impl Drop for LogDeliveries {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
    }
}

/// Seeking is native to the stand-in: the router keeps an append-only log per stream, so a
/// subscription can re-read any suffix of it.
impl Seekable for LogDeliveries {
    type Seeker = KinesisSeeker;

    fn seeker(&self) -> KinesisSeeker {
        self.seeker.clone()
    }
}

impl Subscriber for LogDeliveries {
    type Message = KinesisTestMessage;
    type Error = KinesisError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let requeue = self.requeue.clone();
        let coordinator = self.coordinator.clone();
        let seeker = self.seeker.clone();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            // Register, then apply a pending seek: a reposition installed between the two still
            // arrives, because installing it wakes this task again.
            self.control.waker.register(cx.waker());
            self.apply_pending_seek();
            loop {
                match self.rx.poll_recv(cx) {
                    // A stale pre-seek copy (a requeue that raced the seek): drop it, the
                    // replay already covers everything from the watermark on.
                    Poll::Ready(Some(delivery)) if delivery.sequence < self.control.watermark() => {
                        if let Some(coordinator) = &coordinator {
                            coordinator.consumed();
                        }
                    }
                    Poll::Ready(Some(delivery)) => {
                        return Poll::Ready(Some(Ok(KinesisTestMessage::new(
                            delivery,
                            requeue.clone(),
                            seeker.clone(),
                            coordinator.clone(),
                        ))));
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                }
            }
        })
    }
}

/// Repositioning reaches through the batch buffer, so a batch mount seeks exactly as a
/// single-message one does.
impl Seekable for KinesisTestSubscriber {
    type Seeker = KinesisSeeker;

    fn seeker(&self) -> KinesisSeeker {
        Seekable::seeker(&self.deliveries)
    }
}

impl Subscriber for KinesisTestSubscriber {
    type Message = KinesisTestMessage;
    type Error = KinesisError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.deliveries.stream()
    }
}

/// The stand-in groups the way a broker without batches of its own does: through the framework's
/// adapter, which honours the size the mount site named and closes a partial batch 50 ms after
/// its first delivery. Nothing about a mount says which of the two a broker did, which is why a
/// batch handler runs here and against the service unchanged.
impl BatchSubscriber for KinesisTestSubscriber {
    type Batch = Vec<KinesisTestMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, KinesisError>> + Send + '_ {
        self.deliveries.batches(size)
    }
}

/// Message handed to handlers from a [`KinesisTestSubscriber`].
///
/// It carries the delivery surface a record from the service carries: the payload out of the
/// crate's header envelope, the `partition-key`, `kinesis-sequence-number` and `kinesis-shard-id`
/// headers, its own [`KinesisPosition`], and the subscription's seek handle - so a handler that
/// reads its delivery context runs here unchanged.
///
/// `ack` consumes the handle; `nack(requeue = true)` re-queues the delivery on the owning
/// subscription's channel so the next handler invocation sees it again; `nack(requeue = false)`
/// drops it, matching the real subscriber's reject path in effect.
pub struct KinesisTestMessage {
    delivery: Option<Delivery>,
    payload: Bytes,
    headers: HeaderMap,
    shard: Arc<str>,
    sequence: Arc<str>,
    seeker: KinesisSeeker,
    requeue: DeliverySender,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in
    /// flight and is decremented exactly once when the message is consumed or dropped.
    coordinator: Option<Coordinator>,
}

impl Drop for KinesisTestMessage {
    /// Counts this delivery consumed exactly once: on ack, nack, or an unsettled drop. A
    /// requeue re-enqueues a fresh delivery first, so the in-flight count stays balanced.
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}

impl std::fmt::Debug for KinesisTestMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisTestMessage")
            .field("shard", &self.shard)
            .field("sequence", &self.sequence)
            .finish_non_exhaustive()
    }
}

impl KinesisTestMessage {
    pub(crate) fn new(
        delivery: Delivery,
        requeue: DeliverySender,
        seeker: KinesisSeeker,
        coordinator: Option<Coordinator>,
    ) -> Self {
        let sequence: Arc<str> = Arc::from(sequence_of(delivery.sequence));
        let shard: Arc<str> = Arc::from(IN_PROCESS_SHARD);
        // The same delivery contract the reader gives a record: the envelope is unwrapped, the
        // publish headers ride on top of whatever it carried, and the position is surfaced as
        // headers so a batch body can read it off the elements.
        let (mut headers, payload) = decode_envelope(&delivery.payload);
        for (name, value) in delivery.headers.iter() {
            headers.insert(name, value.to_vec());
        }
        headers.insert(SEQUENCE_HEADER, sequence.to_string());
        headers.insert(SHARD_HEADER, shard.to_string());
        Self {
            delivery: Some(delivery),
            payload,
            headers,
            shard,
            sequence,
            seeker,
            requeue,
            coordinator,
        }
    }

    /// The shard this record arrived on; the per-delivery context borrows it.
    pub(crate) fn shard(&self) -> &Arc<str> {
        &self.shard
    }

    /// This record's sequence number; the per-delivery context borrows it.
    pub(crate) fn sequence(&self) -> &Arc<str> {
        &self.sequence
    }

    /// The subscription's reposition handle, minted once when the subscription opened.
    pub(crate) fn seeker(&self) -> &KinesisSeeker {
        &self.seeker
    }
}

/// The position is the record's stable index in its stream's retained log, assigned at publish
/// and preserved across requeues, so a redelivered record reports the same position.
impl Positioned for KinesisTestMessage {
    type Position = KinesisPosition;

    fn position(&self) -> KinesisPosition {
        KinesisPosition::sequence(&*self.shard, &*self.sequence)
    }
}

impl Partitioned for KinesisTestMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers.get(PARTITION_KEY_HEADER)
    }
}

impl IncomingMessage for KinesisTestMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.delivery.take();
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self
            .delivery
            .take()
            .expect("KinesisTestMessage ack/nack invoked twice");
        if requeue {
            let sent = self.requeue.send(delivery);
            // The requeue bypasses fanout, so count the re-enqueue here to balance this
            // message's `Drop` decrement. The redelivered copy is consumed in turn.
            if sent.is_ok()
                && let Some(coordinator) = &self.coordinator
            {
                coordinator.enqueued();
            }
        }
        ready(Ok(()))
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}
