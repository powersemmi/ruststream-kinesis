//! The in-process transport's streams: a retained log of records per stream, and the
//! subscriptions reading it.
//!
//! A record is kept in the form it has on the wire - the partition key and the data blob the
//! publisher built, the header envelope included - so a delivery is decoded by the same code a
//! record from the service is. A publish reaches every subscription on its stream, which is what
//! the service does for consumers that each read every shard. The log is kept because a Kinesis
//! handler can observe it without a server: a subscription can be repositioned in it and read the
//! same records again.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use ruststream::testing::Coordinator;
use tokio::sync::mpsc;

use super::seek::{IN_PROCESS_SHARD, SeekControl, index_of};
use crate::error::KinesisError;
use crate::message::KinesisPosition;

/// Opaque handle identifying one subscription inside a [`StreamLog`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SubscriptionId(u64);

/// One retained record: what went out on the wire, and when it arrived.
#[derive(Debug, Clone)]
pub(crate) struct Record {
    pub(crate) partition_key: Arc<str>,
    pub(crate) data: Bytes,
    at_millis: u64,
}

impl Record {
    /// The delivery this record makes at `index`, its place in the retained log.
    fn delivery(&self, index: usize) -> Delivery {
        Delivery {
            record: self.clone(),
            index,
        }
    }
}

/// One queued delivery: the record and its index in its stream's retained log.
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) record: Record,
    /// The record's place in the log, which is its sequence number and survives a replay, so a
    /// record delivered again reports the same position.
    pub(crate) index: usize,
}

pub(crate) type DeliverySender = mpsc::UnboundedSender<Delivery>;
pub(crate) type DeliveryReceiver = mpsc::UnboundedReceiver<Delivery>;

struct Subscription {
    stream: Arc<str>,
    sender: DeliverySender,
}

#[derive(Default)]
struct LogState {
    subscriptions: HashMap<SubscriptionId, Subscription>,
    records: HashMap<String, Vec<Record>>,
}

/// The retained logs of every stream, and the subscriptions reading them.
#[derive(Default)]
pub(crate) struct StreamLog {
    state: Mutex<LogState>,
    next_id: AtomicU64,
}

/// Where a reposition lands: the log index to resume from, and how many retained records that
/// covered at the instant it was resolved.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Plan {
    pub(crate) target: usize,
    pub(crate) count: usize,
}

/// What [`StreamLog::subscribe`] hands back: the channel pair the subscription polls, its
/// registry id, and the seek surface it shares with the seekers minted off it.
pub(crate) struct Registration {
    pub(crate) id: SubscriptionId,
    pub(crate) replay: DeliverySender,
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

impl StreamLog {
    fn lock(&self) -> MutexGuard<'_, LogState> {
        self.state
            .lock()
            .expect("kinesis in-process log mutex poisoned")
    }

    /// Registers a subscription on `stream`. It starts at the tip: records published from now on
    /// reach it, the retained ones do not until a reposition asks for them.
    ///
    /// The returned sender is the one fanout uses, so the replay of a reposition enqueues the log
    /// suffix through it.
    pub(crate) fn subscribe(&self, stream: Arc<str>) -> Registration {
        let (tx, rx) = mpsc::unbounded_channel();
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.lock().subscriptions.insert(
            id,
            Subscription {
                stream,
                sender: tx.clone(),
            },
        );
        Registration {
            id,
            replay: tx,
            deliveries: rx,
            control: Arc::default(),
        }
    }

    /// Removes a subscription. No-op if the id is unknown.
    pub(crate) fn unsubscribe(&self, id: SubscriptionId) {
        self.lock().subscriptions.remove(&id);
    }

    /// Appends a record to its stream's log and fans it out to every subscription on that stream.
    /// Under a harness run every enqueue is counted with [`Coordinator::enqueued`].
    ///
    /// The append and the fanout happen under one lock, so the log and a subscription's queue
    /// never disagree about a record: a [`reposition`](Self::reposition) running beside this one
    /// sees the record in both or in neither, and so cannot drop one that neither its replay nor
    /// its queue then carries.
    pub(crate) fn append(
        &self,
        stream: &str,
        partition_key: Arc<str>,
        data: Bytes,
        coordinator: Option<&Coordinator>,
    ) {
        let record = Record {
            partition_key,
            data,
            at_millis: now_millis(),
        };
        let mut state = self.lock();
        let log = state.records.entry(stream.to_owned()).or_default();
        let delivery = record.delivery(log.len());
        log.push(record);

        for subscription in state.subscriptions.values() {
            if *subscription.stream == *stream
                && subscription.sender.send(delivery.clone()).is_ok()
                && let Some(coordinator) = coordinator
            {
                coordinator.enqueued();
            }
        }
    }

    /// Repositions one subscription over its stream's log: what it had queued is dropped, and the
    /// retained suffix from `plan.target` on is enqueued in its place.
    ///
    /// The swap holds the lock [`append`](Self::append) takes, which is what makes it safe: a
    /// record published beside it is either already in the suffix this enqueues, or arrives after
    /// the queue has been swapped.
    ///
    /// Under a harness run the swap is counted: the reposition already counted the records the
    /// log held when it resolved ([`Plan::count`]), so only the growth since is added here, and
    /// every drained delivery is balanced with [`Coordinator::consumed`].
    // The guard is held across the whole swap on purpose - that is the invariant documented
    // above - so the lint's advice to narrow its scope is what the swap cannot take.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn reposition(
        &self,
        stream: &str,
        plan: Plan,
        queue: &mut DeliveryReceiver,
        replay: &DeliverySender,
        coordinator: Option<&Coordinator>,
    ) {
        let state = self.lock();
        let suffix: Vec<Delivery> = state.records.get(stream).map_or_else(Vec::new, |log| {
            log.iter()
                .enumerate()
                .skip(plan.target)
                .map(|(index, record)| record.delivery(index))
                .collect()
        });

        // Counted before the drain, so the in-flight total never touches zero while a replay is
        // still pending: a harness waiting for quiescence would otherwise call the reaction
        // finished mid-swap.
        if let Some(coordinator) = coordinator {
            for _ in plan.count..suffix.len() {
                coordinator.enqueued();
            }
        }
        while queue.try_recv().is_ok() {
            // Every drained delivery was counted in flight when it was enqueued.
            if let Some(coordinator) = coordinator {
                coordinator.consumed();
            }
        }
        for delivery in suffix {
            // The send cannot fail: the subscription holds both ends of its own channel.
            let _ = replay.send(delivery);
        }
    }

    /// Resolves `to` against the stream's log.
    ///
    /// The target and the size of the replay it covers are read under one lock, so the count a
    /// reposition reports to the harness matches the log it resolved against.
    ///
    /// # Errors
    ///
    /// Returns [`KinesisError::Read`] for a [`KinesisPosition::Sequence`] naming another shard
    /// or a sequence number this transport never issued.
    pub(crate) fn plan(&self, stream: &str, to: &KinesisPosition) -> Result<Plan, KinesisError> {
        let state = self.lock();
        let plan = Self::plan_over(
            state.records.get(stream).map_or(&[][..], Vec::as_slice),
            stream,
            to,
        );
        drop(state);
        plan
    }

    /// The plan that replays from the record at `index` on: what a shard does when it is read
    /// again from its first unhandled record.
    pub(crate) fn plan_from(&self, stream: &str, index: usize) -> Plan {
        let len = self.lock().records.get(stream).map_or(0, Vec::len);
        let target = index.min(len);
        Plan {
            target,
            count: len - target,
        }
    }

    /// Resolves a position over one stream's retained records.
    fn plan_over(log: &[Record], stream: &str, to: &KinesisPosition) -> Result<Plan, KinesisError> {
        let target = match to {
            KinesisPosition::Horizon => 0,
            KinesisPosition::Latest => log.len(),
            // The log is append-only, so its arrival stamps are non-decreasing and the first
            // record at or after an instant is a partition point.
            KinesisPosition::Timestamp(millis) => {
                log.partition_point(|record| record.at_millis < *millis)
            }
            KinesisPosition::Sequence { shard, sequence } => {
                if shard != IN_PROCESS_SHARD {
                    return Err(KinesisError::Read {
                        stream: stream.to_owned(),
                        shard: shard.clone(),
                        source: Box::from("no live reader for this shard (not owned, or finished)"),
                    });
                }
                let index = index_of(sequence).ok_or_else(|| KinesisError::Read {
                    stream: stream.to_owned(),
                    shard: shard.clone(),
                    source: Box::from("the sequence number is not one this stream issued"),
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

    /// Every record retained for `stream`, in publish order.
    pub(crate) fn records(&self, stream: &str) -> Vec<Record> {
        self.lock().records.get(stream).cloned().unwrap_or_default()
    }

    /// Drops every subscription and clears the retained logs. Used by broker shutdown.
    pub(crate) fn clear(&self) {
        let mut state = self.lock();
        state.subscriptions.clear();
        state.records.clear();
    }
}

impl std::fmt::Debug for StreamLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("StreamLog")
            .field("subscriptions", &state.subscriptions.len())
            .field("streams", &state.records.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_process::seek::sequence_of;

    /// One retained record that arrived at `at_millis`; what it carries plays no part in
    /// resolving a position.
    fn record(at_millis: u64) -> Record {
        Record {
            partition_key: Arc::from("key"),
            data: Bytes::from_static(b"record"),
            at_millis,
        }
    }

    fn target_of(log: &[Record], to: &KinesisPosition) -> Result<usize, KinesisError> {
        StreamLog::plan_over(log, "jobs", to).map(|plan| plan.target)
    }

    /// A timestamp is stream-wide and opens at the first record from that instant on: an instant
    /// before the log opens the whole of it, one between two records opens at the later one, and
    /// one past the last record opens at the tip.
    #[test]
    fn a_timestamp_opens_at_the_first_record_from_that_instant() {
        let log = [record(10), record(20), record(30)];
        let at = |millis| {
            target_of(&log, &KinesisPosition::timestamp(millis)).expect("a timestamp resolves")
        };
        assert_eq!(at(0), 0);
        assert_eq!(at(20), 1);
        assert_eq!(at(25), 2);
        assert_eq!(at(40), 3);
    }

    /// A stream here has one shard, so a captured position from another one is refused rather
    /// than applied to the one it has, the way the service refuses a position on a shard the
    /// subscription has no reader for.
    #[test]
    fn a_position_this_stream_never_issued_is_refused() {
        let log = [record(10)];
        let elsewhere = KinesisPosition::sequence("shardId-000000000007", sequence_of(0));
        assert!(target_of(&log, &elsewhere).is_err());

        let unissued = KinesisPosition::sequence(IN_PROCESS_SHARD, "not-a-sequence-number");
        assert!(target_of(&log, &unissued).is_err());
    }

    /// A sequence past the tip clamps to it, the way the service clamps one: the subscription
    /// resumes with the next publish instead of failing.
    #[test]
    fn a_sequence_past_the_tip_clamps_to_it() {
        let log = [record(10)];
        let ahead = KinesisPosition::sequence(IN_PROCESS_SHARD, sequence_of(9));
        assert_eq!(target_of(&log, &ahead).expect("a sequence resolves"), 1);
    }
}
