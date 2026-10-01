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
    fn apply_pending(&mut self) {
        let Some(plan) = self.control().take_pending() else {
            return;
        };
        self.bus.log().reposition(
            &self.stream,
            plan,
            &mut self.rx,
            &self.replay,
            self.coordinator.as_ref(),
        );
    }

    /// The delivery a queued record makes, decoded the way the service's reader decodes one.
    fn deliver(&self, delivery: &Delivery) -> Result<KinesisMessage, KinesisError> {
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
        let release = Release {
            shard: Arc::clone(&self.shard),
            coordinator: self.coordinator.clone(),
        };
        Ok(KinesisMessage::new(
            data,
            &delivery.record.partition_key,
            &sequence_of(delivery.index),
            self.seeker.clone(),
            Settle::InProcess(release),
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
            self.apply_pending();
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(delivery)) => Poll::Ready(Some(self.deliver(&delivery))),
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
/// Acknowledging it or skipping it consumes it, and so does leaving it unhandled: the service reads
/// an unhandled record again only when the shard's lease is next taken, and the in-process mode
/// has no leases. Either way the delivery is released to the harness once, when this is dropped.
pub(crate) struct Release {
    shard: Arc<str>,
    coordinator: Option<Coordinator>,
}

impl Release {
    pub(crate) fn shard(&self) -> &Arc<str> {
        &self.shard
    }
}

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}
