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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use aws_sdk_kinesis::operation::get_records::GetRecordsError;
use aws_sdk_kinesis::operation::get_shard_iterator::builders::GetShardIteratorFluentBuilder;
use aws_sdk_kinesis::primitives::DateTime;
use aws_sdk_kinesis::types::{Shard, ShardIteratorType};
use futures::Stream;
use ruststream::Subscriber;
use tokio::sync::{mpsc, oneshot};

use crate::broker::Core;
use crate::error::{KinesisError, sdk_err};
use crate::lease::{LeaseStore, SHARD_END};
use crate::message::{KPL_MAGIC, KinesisMessage, KinesisPosition, Settlement};
use crate::stream::KinesisStream;
use crate::track::Watermark;

/// How often the coordinator re-lists shards (splits and merges change the set over time).
const SHARD_SYNC: Duration = Duration::from_secs(10);
/// How long a lease is valid without renewal, and the renewal cadence derived from it.
const LEASE_TTL: Duration = Duration::from_secs(10);
const RENEW_EVERY: Duration = Duration::from_secs(3);
/// How many deliveries may sit between the readers and the consumer.
const CHANNEL_CAPACITY: usize = 64;

/// A position every shard of the subscription can open at: the stream-wide half of
/// [`KinesisPosition`]. Kept apart from the shard-scoped forms so that "install this for the
/// whole subscription" cannot be handed a position that only one shard understands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamStart {
    Horizon,
    Latest,
    Timestamp(u64),
}

/// The cursor one shard's reader opens with, in the shapes the service's iterator types take.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShardStart {
    /// A position shared by the whole subscription.
    Stream(StreamStart),
    /// Redelivers exactly this record: a captured, pinned position.
    At(String),
    /// Resumes after this record: a stored checkpoint.
    After(String),
}

/// A repositioning request delivered to one shard's reader.
pub(crate) struct ShardSeek {
    pub(crate) start: ShardStart,
    pub(crate) done: oneshot::Sender<Result<(), KinesisError>>,
}

/// One shard's live seek surface: the delivery-generation gate (bumped at enqueue, so
/// in-flight batches stamp stale) and the reader's command channel.
#[derive(Clone)]
pub(crate) struct ShardHandle {
    pub(crate) gate: Arc<AtomicU64>,
    pub(crate) tx: mpsc::UnboundedSender<ShardSeek>,
}

/// The subscription's seek surface, shared by the seeker, the coordinator, and every reader.
#[derive(Default)]
pub(crate) struct SeekState {
    /// The readers that can be repositioned right now, by shard id.
    shards: Mutex<HashMap<String, ShardHandle>>,
    /// The stream-wide position a seek installed, if any. Readers consult it when they fetch
    /// their first iterator, which is what makes a stream-wide seek reach shards that have no
    /// reader yet: the subscription opened microseconds ago (the `start_at(..)` case), or the
    /// shard is a child that only appears after a split.
    start: Mutex<Option<StreamStart>>,
}

pub(crate) type SeekBus = Arc<SeekState>;

impl SeekState {
    fn register(&self, shard: String, handle: ShardHandle) {
        self.lock_shards().insert(shard, handle);
    }

    fn deregister(&self, shard: &str) {
        self.lock_shards().remove(shard);
    }

    fn handle(&self, shard: &str) -> Option<ShardHandle> {
        self.lock_shards().get(shard).cloned()
    }

    fn live(&self) -> Vec<ShardHandle> {
        self.lock_shards().values().cloned().collect()
    }

    fn install(&self, start: StreamStart) {
        *self.start.lock().expect("seek state mutex poisoned") = Some(start);
    }

    pub(crate) fn installed(&self) -> Option<StreamStart> {
        *self.start.lock().expect("seek state mutex poisoned")
    }

    fn lock_shards(&self) -> MutexGuard<'_, HashMap<String, ShardHandle>> {
        self.shards.lock().expect("seek state mutex poisoned")
    }
}

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
        let bus: SeekBus = Arc::new(SeekState::default());
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

/// Repositions a [`KinesisSubscriber`] while its stream runs; minted by
/// [`Seekable::seeker`](ruststream::Seekable::seeker).
///
/// A stream-wide position ([`KinesisPosition::Horizon`], [`Latest`](KinesisPosition::Latest),
/// [`Timestamp`](KinesisPosition::Timestamp)) moves every shard of the subscription and is
/// remembered, so shards whose readers start later open there too. A captured
/// [`Sequence`](KinesisPosition::Sequence) position moves the one shard it names. Either way
/// the affected shards drop their watermark bookkeeping: acknowledgements of records delivered
/// before the seek no longer checkpoint.
#[derive(Clone)]
pub struct KinesisSeeker {
    bus: SeekBus,
}

impl std::fmt::Debug for KinesisSeeker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisSeeker").finish_non_exhaustive()
    }
}

impl KinesisSeeker {
    /// Repositions the one shard the captured position names.
    async fn seek_shard(&self, shard: String, start: ShardStart) -> Result<(), KinesisError> {
        let Some(handle) = self.bus.handle(&shard) else {
            return Err(KinesisError::Read {
                stream: String::new(),
                shard,
                source: Box::from("no live reader for this shard (not owned, or finished)"),
            });
        };
        // Bump the generation first: deliveries stamped before this instant are discarded,
        // including an in-flight batch the reader has not finished forwarding.
        handle.gate.fetch_add(1, Ordering::Release);
        let (done, wait) = oneshot::channel();
        handle
            .tx
            .send(ShardSeek { start, done })
            .map_err(|_| KinesisError::Read {
                stream: String::new(),
                shard: shard.clone(),
                source: Box::from("the shard reader has shut down"),
            })?;
        wait.await.map_err(|_| KinesisError::Read {
            stream: String::new(),
            shard,
            source: Box::from("the shard reader has shut down"),
        })?
    }

    /// Repositions every shard, present and future.
    async fn seek_stream(&self, start: StreamStart) -> Result<(), KinesisError> {
        // Installed before the broadcast, so a shard whose reader has not fetched its first
        // iterator yet lands on the position instead of racing past it. This is the whole
        // reason a `start_at(..)` clause works: it seeks the instant the subscription is
        // created, when no reader has started.
        self.bus.install(start);
        let handles = self.bus.live();
        // Every gate bumps before the first await, so a batch already in flight anywhere in
        // the subscription stamps stale.
        for handle in &handles {
            handle.gate.fetch_add(1, Ordering::Release);
        }
        let mut pending = Vec::with_capacity(handles.len());
        for handle in handles {
            let (done, wait) = oneshot::channel();
            // A reader that shut down between the snapshot and the send needs no reposition:
            // its shard is finished, or its successor will open at the installed position.
            if handle
                .tx
                .send(ShardSeek {
                    start: ShardStart::Stream(start),
                    done,
                })
                .is_ok()
            {
                pending.push(wait);
            }
        }
        for wait in pending {
            if let Ok(outcome) = wait.await {
                outcome?;
            }
        }
        Ok(())
    }
}

impl ruststream::Seeker for KinesisSeeker {
    type Position = KinesisPosition;
    type Error = KinesisError;

    async fn seek(&self, to: KinesisPosition) -> Result<(), KinesisError> {
        match to {
            KinesisPosition::Horizon => self.seek_stream(StreamStart::Horizon).await,
            KinesisPosition::Latest => self.seek_stream(StreamStart::Latest).await,
            KinesisPosition::Timestamp(millis) => {
                self.seek_stream(StreamStart::Timestamp(millis)).await
            }
            KinesisPosition::Sequence { shard, sequence } => {
                self.seek_shard(shard, ShardStart::At(sequence)).await
            }
        }
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
                    if !parents_done(&bus, shard, &by_id, store.as_ref()).await {
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
                            bus.register(id.clone(), handle.clone());
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
/// trimmed out of the listing, or is closed with no checkpoint while the subscription starts
/// at the tip (its history is being skipped by request).
async fn parents_done(
    bus: &SeekState,
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
                && matches!(bus.installed(), None | Some(StreamStart::Latest)) => {}
            _ => return false,
        }
    }
    true
}

/// Points a `GetShardIterator` request at a cursor.
fn iterator_at(
    request: GetShardIteratorFluentBuilder,
    start: &ShardStart,
) -> GetShardIteratorFluentBuilder {
    match start {
        ShardStart::Stream(StreamStart::Latest) => {
            request.shard_iterator_type(ShardIteratorType::Latest)
        }
        ShardStart::Stream(StreamStart::Horizon) => {
            request.shard_iterator_type(ShardIteratorType::TrimHorizon)
        }
        ShardStart::Stream(StreamStart::Timestamp(millis)) => request
            .shard_iterator_type(ShardIteratorType::AtTimestamp)
            .timestamp(DateTime::from_millis(
                i64::try_from(*millis).unwrap_or(i64::MAX),
            )),
        ShardStart::At(sequence) => request
            .shard_iterator_type(ShardIteratorType::AtSequenceNumber)
            .starting_sequence_number(sequence),
        ShardStart::After(sequence) => request
            .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
            .starting_sequence_number(sequence),
    }
}

/// Why a reader is fetching an iterator from scratch, which decides whether a sought position
/// or the stored checkpoint wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reopen {
    /// The reader is starting. A position installed by a seek is forced here, ahead of the
    /// checkpoint: that is what the capability promises, and what `start_at(..)` means.
    Start,
    /// The reader lost its iterator mid-flight (expiry, a transient failure). The checkpoint
    /// is the truth now - re-applying the installed position would replay everything this
    /// reader has already handled - and the position only serves a shard that never
    /// checkpointed.
    Recover,
}

async fn initial_iterator(
    client: &aws_sdk_kinesis::Client,
    stream: &str,
    shard: &str,
    bus: &SeekState,
    store: &dyn LeaseStore,
    why: Reopen,
) -> Result<Option<String>, KinesisError> {
    let checkpoint = store
        .read(shard)
        .await
        .map_err(|e| KinesisError::Lease {
            shard: shard.to_owned(),
            source: e,
        })?
        .checkpoint;
    if checkpoint.as_deref() == Some(SHARD_END) {
        return Ok(None);
    }
    let start = match (why, bus.installed(), checkpoint) {
        (Reopen::Start, Some(installed), _) | (Reopen::Recover, Some(installed), None) => {
            ShardStart::Stream(installed)
        }
        (_, _, Some(sequence)) => ShardStart::After(sequence),
        (_, None, None) => ShardStart::Stream(StreamStart::Latest),
    };
    let request = iterator_at(
        client
            .get_shard_iterator()
            .stream_name(stream)
            .shard_id(shard),
        &start,
    );
    let output = request.send().await.map_err(|e| KinesisError::Read {
        stream: stream.to_owned(),
        shard: shard.to_owned(),
        source: sdk_err(&e),
    })?;
    Ok(output.shard_iterator().map(str::to_owned))
}

/// Applies one reposition: a fresh iterator at the requested cursor and a reset watermark.
async fn apply_shard_seek(
    client: &aws_sdk_kinesis::Client,
    stream: &str,
    shard: &str,
    seek: ShardSeek,
    iterator: &mut String,
    tracker: &mut Arc<Watermark>,
) {
    let fresh = iterator_at(
        client
            .get_shard_iterator()
            .stream_name(stream)
            .shard_id(shard),
        &seek.start,
    )
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
            self.bus.deregister(&self.shard);
        }
    }
    let _bus_guard = BusGuard {
        bus: Arc::clone(&bus),
        shard: shard.clone(),
    };
    let stream = descriptor.stream().to_owned();
    let mut tracker = Arc::new(Watermark::default());
    let mut iterator = match initial_iterator(
        &client,
        &stream,
        &shard,
        &bus,
        store.as_ref(),
        Reopen::Start,
    )
    .await
    {
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
                match initial_iterator(
                    &client,
                    &stream,
                    &shard,
                    &bus,
                    store.as_ref(),
                    Reopen::Recover,
                )
                .await
                {
                    Ok(Some(fresh)) => iterator = fresh,
                    Ok(None) => return,
                    Err(_) => {}
                }
            }
        }
    }
}
