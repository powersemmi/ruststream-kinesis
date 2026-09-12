//! Subscription registry, retained log, and fanout for the in-process Kinesis stand-in.
//!
//! Routing is an exact-name match: a published message fans out to every live subscription on
//! that name. What the name keeps is a **retained log** - every message in publish order, with
//! the instant it was published - because that is the one product property a Kinesis handler can
//! observe without a server: a subscription can be repositioned in it and read the same records
//! again. Shards, leases, checkpoints and retention limits are transport behaviour and are still
//! not simulated; the log is one shard's worth of ordering (see
//! [`IN_PROCESS_SHARD`](super::IN_PROCESS_SHARD)).

use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use ruststream::{HeaderMap, RawMessage, testing::Coordinator};
use tokio::sync::mpsc;

use std::sync::Arc;

use crate::error::KinesisError;
use crate::message::KinesisPosition;

use super::seek::{IN_PROCESS_SHARD, SeekControl};

/// Opaque handle identifying one subscription inside an [`AddressRouter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SubscriptionId(u64);

/// One retained record: what was published, and when.
#[derive(Debug, Clone)]
struct LogEntry {
    payload: Bytes,
    headers: HeaderMap,
    at_millis: u64,
}

impl LogEntry {
    /// The delivery this record makes at `sequence`, its index in the retained log.
    fn delivery(&self, sequence: usize) -> Delivery {
        Delivery {
            payload: self.payload.clone(),
            headers: self.headers.clone(),
            sequence,
        }
    }
}

/// Single delivery handed to a matching subscriber, stamped with its index in the retained log.
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) payload: Bytes,
    pub(crate) headers: HeaderMap,
    /// The record's index in its name's retained log, preserved across requeues so a
    /// redelivered record reports the same position.
    pub(crate) sequence: usize,
}

pub(crate) type DeliverySender = mpsc::UnboundedSender<Delivery>;
pub(crate) type DeliveryReceiver = mpsc::UnboundedReceiver<Delivery>;

struct Subscription {
    address: String,
    sender: DeliverySender,
}

#[derive(Default)]
struct RouterState {
    subscriptions: HashMap<SubscriptionId, Subscription>,
    log: HashMap<String, Vec<LogEntry>>,
}

/// In-memory exact-address router over a retained per-name log.
#[derive(Default)]
pub(crate) struct AddressRouter {
    state: Mutex<RouterState>,
    next_id: AtomicU64,
}

/// Where a reposition lands: the log index to resume from, and how many retained records that
/// covered at the instant it was resolved.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Plan {
    pub(crate) target: usize,
    pub(crate) count: usize,
}

/// What [`AddressRouter::subscribe`] hands back: the channel pair the subscriber polls, its
/// registry id, and the seek surface it shares with the seekers minted off it.
pub(crate) struct Registration {
    pub(crate) id: SubscriptionId,
    pub(crate) requeue: DeliverySender,
    pub(crate) deliveries: DeliveryReceiver,
    pub(crate) control: Arc<SeekControl>,
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

impl AddressRouter {
    /// Registers a subscription on `address`.
    ///
    /// The returned [`DeliverySender`] is the same one fanout uses, so subscribers can re-send a
    /// delivery into their own queue to implement `nack(requeue = true)`, and the replay of a
    /// seek can enqueue the log suffix through it.
    pub(crate) fn subscribe(&self, address: String) -> Registration {
        let (tx, rx) = mpsc::unbounded_channel();
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.state
            .lock()
            .expect("kinesis test router mutex poisoned")
            .subscriptions
            .insert(
                id,
                Subscription {
                    address,
                    sender: tx.clone(),
                },
            );
        Registration {
            id,
            requeue: tx,
            deliveries: rx,
            control: Arc::default(),
        }
    }

    /// Removes a subscription. No-op if the id is unknown (double-drop of the subscriber).
    pub(crate) fn unsubscribe(&self, id: SubscriptionId) {
        self.state
            .lock()
            .expect("kinesis test router mutex poisoned")
            .subscriptions
            .remove(&id);
    }

    /// Appends to the name's retained log and fans the record out to every live subscription on
    /// it. Under a harness run every live enqueue is counted with [`Coordinator::enqueued`].
    ///
    /// The append and the fanout happen under one lock, so the log and a subscription's queue
    /// never disagree about a record: a [`reposition`](Self::reposition) running beside this one
    /// sees the record in both or in neither, and so cannot drop one that neither its replay nor
    /// its queue then carries.
    pub(crate) fn publish(
        &self,
        address: &str,
        payload: Bytes,
        headers: HeaderMap,
        coordinator: Option<&Coordinator>,
    ) {
        let entry = LogEntry {
            payload,
            headers,
            at_millis: now_millis(),
        };
        let mut state = self
            .state
            .lock()
            .expect("kinesis test router mutex poisoned");
        let log = state.log.entry(address.to_owned()).or_default();
        let delivery = entry.delivery(log.len());
        log.push(entry);

        for sub in state.subscriptions.values() {
            if sub.address == address
                && sub.sender.send(delivery.clone()).is_ok()
                && let Some(coordinator) = coordinator
            {
                coordinator.enqueued();
            }
        }
    }

    /// Repositions one subscription over the name's retained log: what it had queued is dropped,
    /// and the retained suffix from `plan.target` on is enqueued in its place.
    ///
    /// The swap holds the lock [`publish`](Self::publish) takes, which is what makes it safe: a
    /// record published beside it is either already in the suffix this enqueues, or arrives after
    /// the queue has been swapped. Split across two lock acquisitions, a publish landing between
    /// the snapshot and the drain would be carried by neither - dropped as though it were a
    /// pre-seek copy, with the harness's in-flight count still balanced, so a test would observe
    /// quiescence and a missing delivery.
    ///
    /// Under a harness run the swap is counted: the seek already counted the records the log held
    /// when it resolved ([`Plan::count`]), so only the growth since is added here, and every
    /// drained delivery is balanced with [`Coordinator::consumed`].
    // The guard is held across the whole swap on purpose - that is the invariant documented
    // above - so the lint's advice to narrow its scope is what the fix had to undo.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn reposition(
        &self,
        address: &str,
        plan: Plan,
        queue: &mut DeliveryReceiver,
        requeue: &DeliverySender,
        coordinator: Option<&Coordinator>,
    ) {
        let state = self
            .state
            .lock()
            .expect("kinesis test router mutex poisoned");
        let replay: Vec<Delivery> = state.log.get(address).map_or_else(Vec::new, |log| {
            log.iter()
                .enumerate()
                .skip(plan.target)
                .map(|(sequence, entry)| entry.delivery(sequence))
                .collect()
        });

        // Counted before the drain, so the in-flight total never touches zero while a replay is
        // still pending: a harness waiting for quiescence would otherwise call the reaction
        // finished mid-swap.
        if let Some(coordinator) = coordinator {
            for _ in plan.count..replay.len() {
                coordinator.enqueued();
            }
        }
        while queue.try_recv().is_ok() {
            // Every drained delivery was counted in flight when it was enqueued.
            if let Some(coordinator) = coordinator {
                coordinator.consumed();
            }
        }
        for delivery in replay {
            // The send cannot fail: the subscription holds both ends of its own channel.
            let _ = requeue.send(delivery);
        }
    }

    /// Resolves `to` against the name's retained log.
    ///
    /// The target and the size of the replay it covers are read under one lock, so the count a
    /// seeker reports to the harness matches the log it resolved against.
    ///
    /// # Errors
    ///
    /// Returns [`KinesisError::Read`] for a [`KinesisPosition::Sequence`] naming another shard
    /// or a sequence number this transport never issued.
    pub(crate) fn plan(&self, address: &str, to: &KinesisPosition) -> Result<Plan, KinesisError> {
        let state = self
            .state
            .lock()
            .expect("kinesis test router mutex poisoned");
        let plan = Self::plan_over(
            state.log.get(address).map_or(&[][..], Vec::as_slice),
            address,
            to,
        );
        drop(state);
        plan
    }

    /// Resolves a position over one name's retained records.
    fn plan_over(
        log: &[LogEntry],
        address: &str,
        to: &KinesisPosition,
    ) -> Result<Plan, KinesisError> {
        let target = match to {
            KinesisPosition::Horizon => 0,
            KinesisPosition::Latest => log.len(),
            // The log is append-only, so its timestamps are non-decreasing and the first record
            // at or after an instant is a partition point.
            KinesisPosition::Timestamp(millis) => {
                log.partition_point(|entry| entry.at_millis < *millis)
            }
            KinesisPosition::Sequence { shard, sequence } => {
                if shard != IN_PROCESS_SHARD {
                    return Err(KinesisError::Read {
                        stream: address.to_owned(),
                        shard: shard.clone(),
                        source: Box::from(
                            "no live reader for this shard (the stand-in routes one)",
                        ),
                    });
                }
                let index = sequence.parse::<usize>().map_err(|_| KinesisError::Read {
                    stream: address.to_owned(),
                    shard: shard.clone(),
                    source: Box::from("the sequence number is not one this transport issued"),
                })?;
                // Clamped the way the service clamps a position past the tip: the subscription
                // resumes with the next publish instead of failing.
                index.min(log.len())
            }
        };
        Ok(Plan {
            target,
            count: log.len() - target,
        })
    }

    /// Returns every message retained for `address`, in publish order.
    pub(crate) fn published(&self, address: &str) -> Vec<RawMessage> {
        self.state
            .lock()
            .expect("kinesis test router mutex poisoned")
            .log
            .get(address)
            .map_or_else(Vec::new, |log| {
                log.iter()
                    .map(|entry| {
                        RawMessage::new(address, entry.payload.clone())
                            .with_headers(entry.headers.clone())
                    })
                    .collect()
            })
    }

    /// Drops every subscription and clears the retained log. Used by broker shutdown.
    pub(crate) fn clear(&self) {
        let mut state = self
            .state
            .lock()
            .expect("kinesis test router mutex poisoned");
        state.subscriptions.clear();
        state.log.clear();
    }
}

impl std::fmt::Debug for AddressRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self
            .state
            .lock()
            .expect("kinesis test router mutex poisoned");
        f.debug_struct("AddressRouter")
            .field("subscriptions", &state.subscriptions.len())
            .field("logged_addresses", &state.log.len())
            .finish_non_exhaustive()
    }
}
