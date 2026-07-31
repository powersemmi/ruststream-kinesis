//! [`LeaseStore`]: pluggable shard-lease and checkpoint coordination.
//!
//! Leasing needs durable state, and whether the crate owns that choice or hands it to the
//! user is a real design decision - so it is a trait. The built-in
//! [`MemoryLeaseStore`] coordinates within one process (a single service instance); the
//! `DynamoDB` store behind the `dynamodb-lease` feature lets multiple instances share the
//! shards with conditional-write fencing.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::sync::Mutex;
use std::time::Duration;

use futures::future::BoxFuture;

/// The checkpoint value marking a shard fully consumed; its children may start.
pub const SHARD_END: &str = "SHARD_END";

/// The persisted state of one shard's lease.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LeaseState {
    /// The last checkpointed sequence number, [`SHARD_END`] when the shard is finished, or
    /// `None` when never checkpointed.
    pub checkpoint: Option<String>,
}

/// The boxed error lease stores report; the crate wraps it with the shard for diagnostics.
pub type LeaseError = Box<dyn StdError + Send + Sync>;

/// Durable coordination for shard leases and checkpoints.
///
/// The contract mirrors the vendor's own consumer library: `acquire` takes a shard for an
/// owner (stealing expired leases), `renew` heartbeats it - a failed renew means the owner
/// has been fenced and must stop processing immediately - and `checkpoint` records progress
/// conditionally on still holding the lease.
pub trait LeaseStore: Send + Sync + 'static {
    /// Attempts to take the shard's lease for `owner`, valid for `ttl`. Returns `false` when
    /// another live owner holds it.
    fn acquire<'a>(
        &'a self,
        shard: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>>;

    /// Heartbeats the lease. Returns `false` when the lease is no longer held (fenced).
    fn renew<'a>(
        &'a self,
        shard: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>>;

    /// Records `sequence` as the shard's checkpoint, conditional on `owner` holding the
    /// lease. Returns `false` when fenced.
    fn checkpoint<'a>(
        &'a self,
        shard: &'a str,
        owner: &'a str,
        sequence: &'a str,
    ) -> BoxFuture<'a, Result<bool, LeaseError>>;

    /// Reads the shard's persisted state (regardless of ownership).
    fn read<'a>(&'a self, shard: &'a str) -> BoxFuture<'a, Result<LeaseState, LeaseError>>;

    /// Releases the lease so another owner can take it without waiting for expiry.
    fn release<'a>(
        &'a self,
        shard: &'a str,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<(), LeaseError>>;
}

#[derive(Debug, Default)]
struct MemoryLease {
    owner: Option<String>,
    expires: Option<std::time::Instant>,
    checkpoint: Option<String>,
}

/// In-process lease coordination, correct for a single service instance.
///
/// Workers within the process still gate children on parents and checkpoint, but nothing
/// survives a restart. Multiple instances need a shared store such as the `DynamoDB` one.
#[derive(Debug, Default)]
pub struct MemoryLeaseStore {
    leases: Mutex<HashMap<String, MemoryLease>>,
}

impl MemoryLeaseStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn with_lease<R>(&self, shard: &str, f: impl FnOnce(&mut MemoryLease) -> R) -> R {
        let mut leases = self.leases.lock().expect("lease store mutex poisoned");
        f(leases.entry(shard.to_owned()).or_default())
    }
}

impl LeaseStore for MemoryLeaseStore {
    fn acquire<'a>(
        &'a self,
        shard: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async move {
            Ok(self.with_lease(shard, |lease| {
                let now = std::time::Instant::now();
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
        shard: &'a str,
        owner: &'a str,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async move {
            Ok(self.with_lease(shard, |lease| {
                if lease.owner.as_deref() == Some(owner) {
                    lease.expires = Some(std::time::Instant::now() + ttl);
                    true
                } else {
                    false
                }
            }))
        })
    }

    fn checkpoint<'a>(
        &'a self,
        shard: &'a str,
        owner: &'a str,
        sequence: &'a str,
    ) -> BoxFuture<'a, Result<bool, LeaseError>> {
        Box::pin(async move {
            Ok(self.with_lease(shard, |lease| {
                if lease.owner.as_deref() == Some(owner) {
                    lease.checkpoint = Some(sequence.to_owned());
                    true
                } else {
                    false
                }
            }))
        })
    }

    fn read<'a>(&'a self, shard: &'a str) -> BoxFuture<'a, Result<LeaseState, LeaseError>> {
        Box::pin(async move {
            Ok(self.with_lease(shard, |lease| LeaseState {
                checkpoint: lease.checkpoint.clone(),
            }))
        })
    }

    fn release<'a>(
        &'a self,
        shard: &'a str,
        owner: &'a str,
    ) -> BoxFuture<'a, Result<(), LeaseError>> {
        Box::pin(async move {
            self.with_lease(shard, |lease| {
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

    #[tokio::test]
    async fn a_live_lease_is_exclusive_and_an_expired_one_is_stealable() {
        let store = MemoryLeaseStore::new();
        let ttl = Duration::from_mins(1);
        assert!(store.acquire("s1", "a", ttl).await.expect("acquire"));
        assert!(!store.acquire("s1", "b", ttl).await.expect("acquire"));

        // Expired: b may steal.
        let store = MemoryLeaseStore::new();
        assert!(
            store
                .acquire("s1", "a", Duration::ZERO)
                .await
                .expect("acquire")
        );
        assert!(store.acquire("s1", "b", ttl).await.expect("steal"));
        // a has been fenced.
        assert!(!store.renew("s1", "a", ttl).await.expect("renew"));
        assert!(!store.checkpoint("s1", "a", "42").await.expect("checkpoint"));
    }

    #[tokio::test]
    async fn checkpoints_survive_release() {
        let store = MemoryLeaseStore::new();
        let ttl = Duration::from_mins(1);
        assert!(store.acquire("s1", "a", ttl).await.expect("acquire"));
        assert!(store.checkpoint("s1", "a", "41").await.expect("checkpoint"));
        store.release("s1", "a").await.expect("release");
        assert_eq!(
            store.read("s1").await.expect("read").checkpoint.as_deref(),
            Some("41")
        );
    }
}
