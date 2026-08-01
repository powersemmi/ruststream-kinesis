//! [`KinesisSubscriber`]: shard discovery, leasing, and per-shard readers.
//!
//! The substantial work of this crate: a coordinator task lists the stream's shards, gates
//! children on their parents being fully consumed, takes leases through the store, and runs
//! one reader task per owned shard; readers poll `GetRecords`, feed the shared delivery
//! channel, renew their lease (stopping immediately when fenced), and close their shard at
//! `SHARD_END` once every delivery has settled.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_kinesis::operation::get_records::GetRecordsError;
use aws_sdk_kinesis::types::{Shard, ShardIteratorType};
use futures::Stream;
use ruststream::Subscriber;
use tokio::sync::mpsc;

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::broker::Core;
use crate::error::{KinesisError, sdk_err};
use crate::lease::{LeaseStore, SHARD_END};
use crate::message::{KPL_MAGIC, KinesisMessage, KinesisPosition, Settlement};
use crate::stream::{KinesisStream, StartPosition};
use crate::track::Watermark;

/// How often the coordinator re-lists shards (splits and merges change the set over time).
const SHARD_SYNC: Duration = Duration::from_secs(10);
/// How long a lease is valid without renewal, and the renewal cadence derived from it.
const LEASE_TTL: Duration = Duration::from_secs(10);
const RENEW_EVERY: Duration = Duration::from_secs(3);
/// How many deliveries may sit between the readers and the consumer.
const CHANNEL_CAPACITY: usize = 64;

/// A repositioning request delivered to one shard's reader.
pub(crate) struct ShardSeek {
    pub(crate) sequence: String,
    pub(crate) done: tokio::sync::oneshot::Sender<Result<(), KinesisError>>,
}

/// One shard's live seek surface: the delivery-generation gate (bumped at enqueue, so
/// in-flight batches stamp stale) and the reader's command channel.
#[derive(Clone)]
pub(crate) struct ShardHandle {
    pub(crate) gate: Arc<AtomicU64>,
    pub(crate) tx: mpsc::UnboundedSender<ShardSeek>,
}

pub(crate) type SeekBus = Arc<Mutex<HashMap<String, ShardHandle>>>;

/// One channel item: the delivery (or error) plus its generation stamp. A seek bumps the
/// shard's gate, and items stamped under an older generation are discarded on the way out.
pub(crate) struct Stamped {
    stamp: Option<(u64, Arc<AtomicU64>)>,
    item: Result<KinesisMessage, KinesisError>,
}

impl Stamped {
    fn live(epoch: u64, gate: &Arc<AtomicU64>, item: Result<KinesisMessage, KinesisError>) -> Self {
        Self {
            stamp: Some((epoch, Arc::clone(gate))),
            item,
        }
    }

    fn unstamped(item: Result<KinesisMessage, KinesisError>) -> Self {
        Self { stamp: None, item }
    }

    fn current(&self) -> bool {
        self.stamp
            .as_ref()
            .is_none_or(|(epoch, gate)| *epoch == gate.load(Ordering::Acquire))
    }
}

/// A subscription to one Kinesis stream; yields [`KinesisMessage`]s from every owned shard.
///
/// Dropping the subscriber stops the coordinator and every reader; unsettled records
/// redeliver from the last checkpoint when the leases are next taken.
pub struct KinesisSubscriber {
    stream: String,
    rx: mpsc::Receiver<Stamped>,
    bus: SeekBus,
}

impl std::fmt::Debug for KinesisSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisSubscriber")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

impl KinesisSubscriber {
    /// The stream this subscription consumes from.
    #[must_use]
    pub fn stream_name(&self) -> &str {
        &self.stream
    }

    pub(crate) fn open(core: &Core, descriptor: KinesisStream) -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let stream = descriptor.stream().to_owned();
        let bus: SeekBus = Arc::new(Mutex::new(HashMap::new()));
        tokio::spawn(coordinate(
            core.client.clone(),
            Arc::clone(&core.store),
            core.owner.clone(),
            descriptor,
            tx,
            Arc::clone(&bus),
        ));
        Self { stream, rx, bus }
    }
}

/// Repositions one shard of a [`KinesisSubscriber`] while its stream runs; minted by
/// [`Seekable::seeker`](ruststream::Seekable::seeker).
///
/// A seek addresses the shard carried by the position and resets that shard's watermark
/// bookkeeping: acknowledgements of records delivered before the seek no longer checkpoint.
#[derive(Clone)]
pub struct KinesisSeeker {
    bus: SeekBus,
}

impl std::fmt::Debug for KinesisSeeker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisSeeker").finish_non_exhaustive()
    }
}

impl ruststream::Seeker for KinesisSeeker {
    type Position = KinesisPosition;
    type Error = KinesisError;

    async fn seek(&self, to: KinesisPosition) -> Result<(), KinesisError> {
        let handle = {
            let bus = self.bus.lock().expect("seek bus mutex poisoned");
            bus.get(&to.shard).cloned()
        };
        let Some(handle) = handle else {
            return Err(KinesisError::Read {
                stream: String::new(),
                shard: to.shard,
                source: Box::from("no live reader for this shard (not owned, or finished)"),
            });
        };
        // Bump the generation first: deliveries stamped before this instant are discarded,
        // including an in-flight batch the reader has not finished forwarding.
        handle.gate.fetch_add(1, Ordering::Release);
        let (done, wait) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(ShardSeek {
                sequence: to.sequence,
                done,
            })
            .map_err(|_| KinesisError::Read {
                stream: String::new(),
                shard: to.shard.clone(),
                source: Box::from("the shard reader has shut down"),
            })?;
        wait.await.map_err(|_| KinesisError::Read {
            stream: String::new(),
            shard: to.shard,
            source: Box::from("the shard reader has shut down"),
        })?
    }
}

impl ruststream::Seekable for KinesisSubscriber {
    type Seeker = KinesisSeeker;

    fn seeker(&self) -> KinesisSeeker {
        KinesisSeeker {
            bus: Arc::clone(&self.bus),
        }
    }
}

impl Subscriber for KinesisSubscriber {
    type Message = KinesisMessage;
    type Error = KinesisError;

    fn stream(&mut self) -> impl Stream<Item = Result<KinesisMessage, KinesisError>> + Send + '_ {
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call). Items stamped under an older generation
        // (before a seek) are discarded here.
        futures::stream::poll_fn(move |cx| {
            loop {
                match self.rx.poll_recv(cx) {
                    std::task::Poll::Ready(Some(stamped)) => {
                        if stamped.current() {
                            return std::task::Poll::Ready(Some(stamped.item));
                        }
                    }
                    std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                }
            }
        })
    }
}

async fn list_all_shards(
    client: &aws_sdk_kinesis::Client,
    stream: &str,
) -> Result<Vec<Shard>, KinesisError> {
    // No paginator exists for ListShards, and a continuation call may carry ONLY the token.
    let mut shards = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let request = token.take().map_or_else(
            || client.list_shards().stream_name(stream),
            |t| client.list_shards().next_token(t),
        );
        let output = request.send().await.map_err(|e| KinesisError::Stream {
            stream: stream.to_owned(),
            source: sdk_err(&e),
        })?;
        shards.extend(output.shards().iter().cloned());
        match output.next_token() {
            Some(t) => token = Some(t.to_owned()),
            None => return Ok(shards),
        }
    }
}

fn is_closed(shard: &Shard) -> bool {
    shard
        .sequence_number_range()
        .and_then(|r| r.ending_sequence_number())
        .is_some()
}

async fn coordinate(
    client: aws_sdk_kinesis::Client,
    store: Arc<dyn LeaseStore>,
    owner: String,
    descriptor: KinesisStream,
    out: mpsc::Sender<Stamped>,
    bus: SeekBus,
) {
    let stream = descriptor.stream().to_owned();
    let mut readers: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    loop {
        readers.retain(|_, handle| !handle.is_finished());

        match list_all_shards(&client, &stream).await {
            Ok(shards) => {
                let by_id: HashMap<&str, &Shard> =
                    shards.iter().map(|s| (s.shard_id(), s)).collect();
                for shard in &shards {
                    let id = shard.shard_id().to_owned();
                    if readers.contains_key(&id) {
                        continue;
                    }
                    if !parents_done(&descriptor, shard, &by_id, store.as_ref()).await {
                        continue;
                    }
                    match store.read(&id).await {
                        Ok(state) if state.checkpoint.as_deref() == Some(SHARD_END) => continue,
                        Ok(_) => {}
                        Err(err) => {
                            let _ = out
                                .send(Stamped::unstamped(Err(KinesisError::Lease {
                                    shard: id.clone(),
                                    source: err,
                                })))
                                .await;
                            continue;
                        }
                    }
                    match store.acquire(&id, &owner, LEASE_TTL).await {
                        Ok(true) => {
                            let (seek_tx, seek_rx) = mpsc::unbounded_channel();
                            let handle = ShardHandle {
                                gate: Arc::new(AtomicU64::new(0)),
                                tx: seek_tx,
                            };
                            bus.lock()
                                .expect("seek bus mutex poisoned")
                                .insert(id.clone(), handle.clone());
                            readers.insert(
                                id.clone(),
                                tokio::spawn(read_shard(
                                    client.clone(),
                                    Arc::clone(&store),
                                    owner.clone(),
                                    descriptor.clone(),
                                    id,
                                    out.clone(),
                                    handle.gate,
                                    seek_rx,
                                    Arc::clone(&bus),
                                )),
                            );
                        }
                        Ok(false) => {} // another instance owns it
                        Err(err) => {
                            let _ = out
                                .send(Stamped::unstamped(Err(KinesisError::Lease {
                                    shard: id.clone(),
                                    source: err,
                                })))
                                .await;
                        }
                    }
                }
            }
            Err(err) => {
                if out.send(Stamped::unstamped(Err(err))).await.is_err() {
                    break;
                }
            }
        }

        tokio::select! {
            () = out.closed() => break,
            () = tokio::time::sleep(SHARD_SYNC) => {}
        }
    }
    // Readers watch the same channel and stop on their own.
}

/// A child shard may start only when every parent is fully consumed - that is what keeps
/// per-key ordering across a split. A parent counts as done when it reached `SHARD_END`, was
/// trimmed out of the listing, or is closed with no checkpoint under a `Latest` start (its
/// history is being skipped by request).
async fn parents_done(
    descriptor: &KinesisStream,
    shard: &Shard,
    by_id: &HashMap<&str, &Shard>,
    store: &dyn LeaseStore,
) -> bool {
    let parents = [shard.parent_shard_id(), shard.adjacent_parent_shard_id()];
    for parent in parents.into_iter().flatten() {
        let Some(parent_shard) = by_id.get(parent) else {
            continue; // trimmed past retention
        };
        let Ok(state) = store.read(parent).await else {
            return false;
        };
        match state.checkpoint.as_deref() {
            Some(SHARD_END) => {}
            None if is_closed(parent_shard)
                && matches!(descriptor.start_value(), StartPosition::Latest) => {}
            _ => return false,
        }
    }
    true
}

async fn initial_iterator(
    client: &aws_sdk_kinesis::Client,
    stream: &str,
    shard: &str,
    descriptor: &KinesisStream,
    store: &dyn LeaseStore,
) -> Result<Option<String>, KinesisError> {
    let checkpoint = store
        .read(shard)
        .await
        .map_err(|e| KinesisError::Lease {
            shard: shard.to_owned(),
            source: e,
        })?
        .checkpoint;
    let mut request = client
        .get_shard_iterator()
        .stream_name(stream)
        .shard_id(shard);
    request = match checkpoint.as_deref() {
        Some(SHARD_END) => return Ok(None),
        Some(sequence) => request
            .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
            .starting_sequence_number(sequence),
        None => match descriptor.start_value() {
            StartPosition::Latest => request.shard_iterator_type(ShardIteratorType::Latest),
            StartPosition::Horizon => request.shard_iterator_type(ShardIteratorType::TrimHorizon),
            StartPosition::Timestamp(millis) => request
                .shard_iterator_type(ShardIteratorType::AtTimestamp)
                .timestamp(aws_sdk_kinesis::primitives::DateTime::from_millis(
                    i64::try_from(*millis).unwrap_or(i64::MAX),
                )),
            StartPosition::Sequence(sequence) => request
                .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
                .starting_sequence_number(sequence),
        },
    };
    let output = request.send().await.map_err(|e| KinesisError::Read {
        stream: stream.to_owned(),
        shard: shard.to_owned(),
        source: sdk_err(&e),
    })?;
    Ok(output.shard_iterator().map(str::to_owned))
}

/// Applies one reposition: a fresh iterator at the requested sequence and a reset watermark.
async fn apply_shard_seek(
    client: &aws_sdk_kinesis::Client,
    stream: &str,
    shard: &str,
    seek: ShardSeek,
    iterator: &mut String,
    tracker: &mut Arc<Watermark>,
) {
    let fresh = client
        .get_shard_iterator()
        .stream_name(stream)
        .shard_id(shard)
        .shard_iterator_type(ShardIteratorType::AtSequenceNumber)
        .starting_sequence_number(&seek.sequence)
        .send()
        .await;
    let outcome = match fresh {
        Ok(output) => output.shard_iterator().map_or_else(
            || {
                Err(KinesisError::Read {
                    stream: stream.to_owned(),
                    shard: shard.to_owned(),
                    source: Box::from("the service returned no iterator for the position"),
                })
            },
            |new_iterator| {
                new_iterator.clone_into(iterator);
                *tracker = Arc::new(Watermark::default());
                Ok(())
            },
        ),
        Err(err) => Err(KinesisError::Read {
            stream: stream.to_owned(),
            shard: shard.to_owned(),
            source: sdk_err(&err),
        }),
    };
    let _ = seek.done.send(outcome);
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn read_shard(
    client: aws_sdk_kinesis::Client,
    store: Arc<dyn LeaseStore>,
    owner: String,
    descriptor: KinesisStream,
    shard: String,
    out: mpsc::Sender<Stamped>,
    gate: Arc<AtomicU64>,
    mut seek_rx: mpsc::UnboundedReceiver<ShardSeek>,
    bus: SeekBus,
) {
    // Every exit path must deregister this shard's seek surface.
    struct BusGuard {
        bus: SeekBus,
        shard: String,
    }
    impl Drop for BusGuard {
        fn drop(&mut self) {
            self.bus
                .lock()
                .expect("seek bus mutex poisoned")
                .remove(&self.shard);
        }
    }
    let _bus_guard = BusGuard {
        bus,
        shard: shard.clone(),
    };
    let stream = descriptor.stream().to_owned();
    let mut tracker = Arc::new(Watermark::default());
    let mut iterator =
        match initial_iterator(&client, &stream, &shard, &descriptor, store.as_ref()).await {
            Ok(Some(iterator)) => iterator,
            Ok(None) => return, // already at SHARD_END
            Err(err) => {
                let _ = out.send(Stamped::unstamped(Err(err))).await;
                return;
            }
        };
    let mut last_renew = tokio::time::Instant::now();
    let mut failures: u32 = 0;

    loop {
        if out.is_closed() {
            let _ = store.release(&shard, &owner).await;
            return;
        }
        // A reposition replaces the iterator and resets the watermark; deliveries stamped
        // under the previous generation are discarded by their settlements and were already
        // filtered from checkpointing by the gate bump at enqueue.
        while let Ok(seek) = seek_rx.try_recv() {
            apply_shard_seek(&client, &stream, &shard, seek, &mut iterator, &mut tracker).await;
        }
        if last_renew.elapsed() >= RENEW_EVERY {
            match store.renew(&shard, &owner, LEASE_TTL).await {
                Ok(true) => last_renew = tokio::time::Instant::now(),
                // Fenced: stop immediately, without checkpointing - the new owner replays
                // from the last checkpoint, which at-least-once permits.
                Ok(false) | Err(_) => return,
            }
        }

        // Stamped before the read: a seek that lands while the batch is in flight bumps the
        // gate, so these deliveries are discarded rather than leaking pre-seek records.
        let epoch = gate.load(Ordering::Acquire);
        let response = client
            .get_records()
            .shard_iterator(&iterator)
            .limit(descriptor.batch_value())
            .send()
            .await;
        match response {
            Ok(output) => {
                failures = 0;
                for record in output.records() {
                    let data = record.data().as_ref();
                    if data.len() > 4 && data[0..4] == KPL_MAGIC {
                        if out
                            .send(Stamped::live(
                                epoch,
                                &gate,
                                Err(KinesisError::AggregatedRecord {
                                    shard: shard.clone(),
                                }),
                            ))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        continue;
                    }
                    let settlement = Settlement {
                        tracker: Arc::clone(&tracker),
                        index: tracker.deliver(record.sequence_number()),
                        store: Arc::clone(&store),
                        shard: shard.clone(),
                        owner: owner.clone(),
                        epoch,
                        gate: Arc::clone(&gate),
                    };
                    let message = KinesisMessage::new(
                        data,
                        record.partition_key(),
                        record.sequence_number(),
                        settlement,
                    );
                    if out
                        .send(Stamped::live(epoch, &gate, Ok(message)))
                        .await
                        .is_err()
                    {
                        let _ = store.release(&shard, &owner).await;
                        return;
                    }
                }

                let Some(next) = output.next_shard_iterator() else {
                    // SHARD_END. Wait for every delivery to settle, then mark the shard
                    // finished so the coordinator may start its children.
                    while !tracker.drained() && !out.is_closed() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    if tracker.drained() {
                        let _ = store.checkpoint(&shard, &owner, SHARD_END).await;
                    }
                    let _ = store.release(&shard, &owner).await;
                    return;
                };
                iterator = next.to_owned();
                if output.records().is_empty() {
                    tokio::select! {
                        () = out.closed() => {}
                        seek = seek_rx.recv() => {
                            if let Some(seek) = seek {
                                apply_shard_seek(
                                    &client, &stream, &shard, seek, &mut iterator, &mut tracker,
                                )
                                .await;
                            }
                        }
                        () = tokio::time::sleep(descriptor.poll_value()) => {}
                    }
                }
            }
            Err(err) => {
                let expired = err
                    .as_service_error()
                    .is_some_and(GetRecordsError::is_expired_iterator_exception);
                let fatal = err
                    .as_service_error()
                    .is_some_and(GetRecordsError::is_resource_not_found_exception);
                if !expired
                    && out
                        .send(Stamped::unstamped(Err(KinesisError::Read {
                            stream: stream.clone(),
                            shard: shard.clone(),
                            source: sdk_err(&err),
                        })))
                        .await
                        .is_err()
                {
                    return;
                }
                if fatal {
                    let _ = store.release(&shard, &owner).await;
                    return;
                }
                failures += 1;
                if failures > 10 {
                    let _ = store.release(&shard, &owner).await;
                    return;
                }
                // Expired iterators are refetched from the checkpoint (never Latest, which
                // would silently skip data); everything else backs off and retries.
                tokio::time::sleep(Duration::from_secs(1)).await;
                match initial_iterator(&client, &stream, &shard, &descriptor, store.as_ref()).await
                {
                    Ok(Some(fresh)) => iterator = fresh,
                    Ok(None) => return,
                    Err(_) => {}
                }
            }
        }
    }
}
