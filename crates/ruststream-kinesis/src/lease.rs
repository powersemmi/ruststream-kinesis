//! [`LeaseStore`]: pluggable shard-lease and checkpoint coordination.
//!
//! Leasing needs durable state, and whether the crate owns that choice or hands it to the
//! user is a real design decision - so it is a trait. The built-in
//! [`MemoryLeaseStore`] coordinates within one process (a single service instance); the
//! `DynamoDB` store behind the `dynamodb-lease` feature lets multiple instances share the
//! shards with conditional-write fencing.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;

/// The checkpoint value marking a shard fully consumed; its children may start.
pub const SHARD_END: &str = "SHARD_END";

/// The name one shard's lease and checkpoint live under: the stream and the shard id.
///
/// Shard ids repeat across streams (the first shard of every stream is `shardId-000000000000`),
/// so a store keys by both, and one store serves every stream a broker consumes. The stream is the
/// name the subscription's [`KinesisStream`](crate::KinesisStream) was given.
///
/// # Examples
///
/// ```
/// use ruststream_kinesis::LeaseKey;
///
/// let key = LeaseKey::new("orders", "shardId-000000000000");
/// assert_eq!(key.stream(), "orders");
/// assert_eq!(key.shard(), "shardId-000000000000");
/// assert_eq!(key.to_string(), "orders:shardId-000000000000");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LeaseKey {
    // Shared, not owned: a reader stamps its key onto every record it delivers, and the
    // per-delivery context reads the shard back, so both stay reference-count bumps.
    stream: Arc<str>,
    shard: Arc<str>,
}

impl LeaseKey {
    /// The lease of `shard` on `stream`.
    #[must_use]
    pub fn new(stream: impl Into<Arc<str>>, shard: impl Into<Arc<str>>) -> Self {
        Self {
            stream: stream.into(),
            shard: shard.into(),
        }
    }

    /// The stream the shard belongs to.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// The shard id, unique within its stream only.
    #[must_use]
    pub fn shard(&self) -> &str {
        &self.shard
    }

    pub(crate) const fn shard_shared(&self) -> &Arc<str> {
        &self.shard
    }
}

/// `stream:shard`, the form a store that keeps one string key writes. A stream name holds no
/// colon, so the form is unambiguous.
impl fmt::Display for LeaseKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.stream, self.shard)
    }
}

/// The persisted state of one shard's lease.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LeaseState {
    /// The last checkpointed sequence number, [`SHARD_END`] when the shard is finished, or
    /// `None` when never checkpointed.
    pub checkpoint: Option<String>,
}

/// The boxed error lease stores report; the crate wraps it with the stream and the shard for
/// diagnostics.
pub type LeaseError = Box<dyn StdError + Send + Sync>;

/// Durable coordination for shard leases and checkpoints.
///
/// The contract mirrors the vendor's own consumer library: `acquire` takes a shard for an
/// owner (stealing expired leases), `renew` heartbeats it - a failed renew means the owner
/// has been fenced and must stop processing immediately - and `checkpoint` records progress
/// conditionally on still holding the lease. Every call names the shard by its [`LeaseKey`]: a
/// store keeps one lease per stream and shard.
pub trait LeaseStore: Send + Sync + 'static {
    /// Attempts to take the shard's lease for `owner`, valid for `ttl`. Returns `false` when
    /// another live owner holds it.
    fn acquire<'a>(
        &'a self,
        key: &'a LeaseKey,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>>;

    /// Heartbeats the lease. Returns `false` when the lease is no longer held (fenced).
    fn renew<'a>(
        &'a self,
        key: &'a LeaseKey,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>>;

    /// Records `sequence` as the shard's checkpoint, conditional on `owner` holding the
    /// lease. Returns `false` when fenced.
    fn checkpoint<'a>(
        &'a self,
        key: &'a LeaseKey,
        owner: &'a str,
        sequence: &'a str,
    ) -> BoxFuture<'a, Result<bool, LeaseError>>;

    /// Reads the shard's persisted state (regardless of ownership).
    fn read<'a>(&'a self, key: &'a LeaseKey) -> BoxFuture<'a, Result<LeaseState, LeaseError>>;

    /// Releases the lease so another owner can take it without waiting for expiry.
    fn release<'a>(
        &'a self,
        key: &'a LeaseKey,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<(), LeaseError>>;
}

#[derive(Debug, Default)]
struct MemoryLease {
    owner: Option<String>,
    expires: Option<Instant>,
    checkpoint: Option<String>,
}

/// In-process lease coordination, correct for a single service instance.
///
/// Workers within the process still gate children on parents and checkpoint, but nothing
/// survives a restart. Multiple instances need a shared store such as the `DynamoDB` one.
#[derive(Debug, Default)]
pub struct MemoryLeaseStore {
    leases: Mutex<HashMap<LeaseKey, MemoryLease>>,
}

impl MemoryLeaseStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn with_lease<R>(&self, key: &LeaseKey, f: impl FnOnce(&mut MemoryLease) -> R) -> R {
        let mut leases = self.leases.lock().expect("lease store mutex poisoned");
        // A checkpoint lands on every acknowledgement that advances the watermark, and by then
        // the lease exists: look it up first, so that path clones no key.
        if let Some(lease) = leases.get_mut(key) {
            return f(lease);
        }
        f(leases.entry(key.clone()).or_default())
    }
}

impl LeaseStore for MemoryLeaseStore {
    fn acquire<'a>(
        &'a self,
        key: &'a LeaseKey,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async move {
            Ok(self.with_lease(key, |lease| {
                let now = Instant::now();
                let held = lease.owner.as_deref().is_some_and(|current| {
                    current != owner && lease.expires.is_some_and(|expiry| expiry > now)
                });
                if held {
                    false
                } else {
                    lease.owner = Some(owner.to_owned());
                    lease.expires = Some(now + ttl);
                    true
                }
            }))
        })
    }

    fn renew<'a>(
        &'a self,
        key: &'a LeaseKey,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async move {
            Ok(self.with_lease(key, |lease| {
                if lease.owner.as_deref() == Some(owner) {
                    lease.expires = Some(Instant::now() + ttl);
                    true
                } else {
                    false
                }
            }))
        })
    }

    fn checkpoint<'a>(
        &'a self,
        key: &'a LeaseKey,
        owner: &'a str,
        sequence: &'a str,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async move {
            Ok(self.with_lease(key, |lease| {
                if lease.owner.as_deref() == Some(owner) {
                    lease.checkpoint = Some(sequence.to_owned());
                    true
                } else {
                    false
                }
            }))
        })
    }

    fn read<'a>(&'a self, key: &'a LeaseKey) -> BoxFuture<'a, Result<LeaseState, LeaseError>> {
        Box::pin(async move {
            Ok(self.with_lease(key, |lease| LeaseState {
                checkpoint: lease.checkpoint.clone(),
            }))
        })
    }

    fn release<'a>(
        &'a self,
        key: &'a LeaseKey,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<(), LeaseError>> {
        Box::pin(async move {
            self.with_lease(key, |lease| {
                if lease.owner.as_deref() == Some(owner) {
                    lease.owner = None;
                    lease.expires = None;
                }
            });
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_mins(1);

    fn key(shard: &str) -> LeaseKey {
        LeaseKey::new("orders", shard)
    }

    #[tokio::test]
    async fn a_live_lease_is_exclusive_and_an_expired_one_is_stealable() {
        let store = MemoryLeaseStore::new();
        let s1 = key("s1");
        assert!(store.acquire(&s1, "a", TTL).await.expect("acquire"));
        assert!(!store.acquire(&s1, "b", TTL).await.expect("acquire"));

        // Expired: b may steal.
        let store = MemoryLeaseStore::new();
        assert!(
            store
                .acquire(&s1, "a", Duration::ZERO)
                .await
                .expect("acquire")
        );
        assert!(store.acquire(&s1, "b", TTL).await.expect("steal"));
        // a has been fenced.
        assert!(!store.renew(&s1, "a", TTL).await.expect("renew"));
        assert!(!store.checkpoint(&s1, "a", "42").await.expect("checkpoint"));
    }

    #[tokio::test]
    async fn checkpoints_survive_release() {
        let store = MemoryLeaseStore::new();
        let s1 = key("s1");
        assert!(store.acquire(&s1, "a", TTL).await.expect("acquire"));
        assert!(store.checkpoint(&s1, "a", "41").await.expect("checkpoint"));
        store.release(&s1, "a").await.expect("release");
        assert_eq!(
            store.read(&s1).await.expect("read").checkpoint.as_deref(),
            Some("41")
        );
    }

    /// Every stream's first shard has the same id, so one store serving two streams must keep
    /// their leases and their progress apart: neither the lease, nor the checkpoint, nor the
    /// finished mark of one stream's shard reaches the other's.
    #[tokio::test]
    async fn the_same_shard_id_on_two_streams_is_two_leases() {
        let store = MemoryLeaseStore::new();
        let orders = LeaseKey::new("orders", "shardId-000000000000");
        let refunds = LeaseKey::new("refunds", "shardId-000000000000");

        assert!(store.acquire(&orders, "a", TTL).await.expect("acquire"));
        assert!(
            store.acquire(&refunds, "b", TTL).await.expect("acquire"),
            "a lease on one stream must not hold the same shard id on another",
        );
        assert!(
            store
                .checkpoint(&orders, "a", "41")
                .await
                .expect("checkpoint")
        );
        assert_eq!(
            store.read(&refunds).await.expect("read").checkpoint,
            None,
            "one stream's progress must not become another's",
        );
        assert!(
            store
                .checkpoint(&refunds, "b", SHARD_END)
                .await
                .expect("checkpoint")
        );
        assert_eq!(
            store
                .read(&orders)
                .await
                .expect("read")
                .checkpoint
                .as_deref(),
            Some("41"),
            "a shard finished on one stream must stay open on another",
        );
    }
}
