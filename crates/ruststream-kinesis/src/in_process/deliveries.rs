//! One in-process subscription's deliveries, and how each of them settles.
//!
//! The subscription reads its stream's retained log. It opens at the tip, the way a shard without
//! a checkpoint does, and a reposition swaps its queue for the log suffix at the target. Like the
//! service's reader, the swap happens inside the subscription's own poll, where the mutable borrow
//! of the receiver is available, so no state is buffered between polls and the stream stays
//! cancel-safe.

use std::sync::Arc;
use std::task::Poll;

use futures::Stream;
use ruststream::testing::Coordinator;

use super::Bus;
use super::log::{Delivery, DeliveryReceiver, DeliverySender, SubscriptionId};
use super::seek::{IN_PROCESS_SHARD, LogSeeker, SeekControl, sequence_of};
use crate::error::KinesisError;
use crate::message::{KPL_MAGIC, KinesisMessage, Settle};
use crate::subscriber::KinesisSeeker;

/// One subscription's queue over its stream's retained log.
pub(crate) struct LogDeliveries {
    bus: Arc<Bus>,
    id: SubscriptionId,
    stream: Arc<str>,
    shard: Arc<str>,
    rx: DeliveryReceiver,
    replay: DeliverySender,
    log: LogSeeker,
    /// Minted once, before the first delivery: every record carries a clone, so a handler can
    /// reposition this subscription from its delivery context.
    seeker: KinesisSeeker,
    /// The harness coordinator, carried by every delivery so its settlement is counted. `None`
    /// outside a harness run.
    coordinator: Option<Coordinator>,
}

impl std::fmt::Debug for LogDeliveries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogDeliveries")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl LogDeliveries {
    pub(crate) fn open(bus: &Arc<Bus>, stream: &str) -> Self {
        let stream: Arc<str> = Arc::from(stream);
        let registration = bus.log().subscribe(Arc::clone(&stream));
        let log = LogSeeker::new(
            Arc::clone(bus),
            Arc::clone(&stream),
            Arc::clone(&registration.control),
        );
        Self {
            bus: Arc::clone(bus),
            id: registration.id,
            stream,
            shard: Arc::from(IN_PROCESS_SHARD),
            rx: registration.deliveries,
            replay: registration.replay,
            seeker: KinesisSeeker::in_process(log.clone()),
            log,
            coordinator: bus.coordinator().cloned(),
        }
    }

    /// The handle that repositions this subscription.
    pub(crate) fn seeker(&self) -> KinesisSeeker {
        self.seeker.clone()
    }

    fn control(&self) -> &SeekControl {
        self.log.control()
    }

    /// Applies a reposition left for this subscription, if one is pending: everything queued
    /// before it is dropped, and the retained suffix from the target on is enqueued in its place.
    /// Answers the generation of what the subscription hands out next.
    fn apply_pending(&mut self) -> u64 {
        let (plan, generation) = self.control().take_pending();
        let Some(plan) = plan else {
            return generation;
        };
        self.bus.log().reposition(
            &self.stream,
            plan,
            &mut self.rx,
            &self.replay,
            self.coordinator.as_ref(),
        );
        generation
    }

    /// The delivery a queued record makes, decoded the way the service's reader decodes one.
    fn deliver(
        &self,
        delivery: &Delivery,
        generation: u64,
    ) -> Result<KinesisMessage, KinesisError> {
        let data = delivery.record.data.as_ref();
        if data.len() > 4 && data[0..4] == KPL_MAGIC {
            // The service's reader refuses such a record and reads on, so nothing settles it.
            if let Some(coordinator) = &self.coordinator {
                coordinator.consumed();
            }
            return Err(KinesisError::AggregatedRecord {
                shard: self.shard.to_string(),
            });
        }
        let replay = Replay {
            shard: Arc::clone(&self.shard),
            index: delivery.index,
            generation,
            log: self.log.clone(),
            coordinator: self.coordinator.clone(),
        };
        Ok(KinesisMessage::new(
            data,
            &delivery.record.partition_key,
            &sequence_of(delivery.index),
            self.seeker.clone(),
            Settle::InProcess(replay),
        ))
    }

    pub(crate) fn stream(
        &mut self,
    ) -> impl Stream<Item = Result<KinesisMessage, KinesisError>> + Send + '_ {
        // Polls the receiver in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            // Register, then apply a pending reposition: one installed between the two still
            // arrives, because installing it wakes this task again.
            self.control().waker.register(cx.waker());
            let generation = self.apply_pending();
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(delivery)) => {
                    Poll::Ready(Some(self.deliver(&delivery, generation)))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        })
    }
}

impl Drop for LogDeliveries {
    fn drop(&mut self) {
        self.bus.log().unsubscribe(self.id);
    }
}

/// How a delivery of the in-process transport settles.
///
/// Acknowledging it or skipping it consumes it. Leaving it unhandled reads the stream again from
/// it on, which is what the service does with a shard whose watermark stopped at a record: the
/// record and everything after it are delivered again. Either way the delivery is released to the
/// harness once, when this is dropped.
pub(crate) struct Replay {
    shard: Arc<str>,
    index: usize,
    generation: u64,
    log: LogSeeker,
    coordinator: Option<Coordinator>,
}

impl Replay {
    pub(crate) fn shard(&self) -> &Arc<str> {
        &self.shard
    }

    /// Reads the stream again from this record on.
    pub(crate) fn rewind(&self) {
        self.log.replay_from(self.index, self.generation);
    }
}

impl Drop for Replay {
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}
