//! The streams one broker reads, so that it reads each of them once.
//!
//! A broker leases and checkpoints a stream's shards under its one owner id, keyed by stream and
//! shard. Two subscriptions of one broker to one stream would take the same leases: each would
//! overwrite the other's checkpoints, and one finishing a shard would stop the other mid-shard.
//! The broker therefore refuses the second subscription when it opens, on either transport.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::error::KinesisError;

/// The streams a connected broker has an open subscription to.
#[derive(Debug, Default)]
pub(crate) struct ReadStreams {
    streams: Mutex<HashSet<String>>,
}

impl ReadStreams {
    /// Records that a subscription to `stream` opens, or refuses it when one is already open.
    pub(crate) fn claim(self: &Arc<Self>, stream: &str) -> Result<StreamClaim, KinesisError> {
        if !self.lock().insert(stream.to_owned()) {
            return Err(KinesisError::AlreadySubscribed {
                stream: stream.to_owned(),
            });
        }
        Ok(StreamClaim {
            streams: Arc::clone(self),
            stream: stream.to_owned(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, HashSet<String>> {
        // A panic while the set is held leaves it consistent: every operation is one insert or
        // one remove.
        self.streams.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One subscription's hold on its stream, given back when the subscription is dropped, so the
/// stream can be opened again on the same broker.
#[derive(Debug)]
pub(crate) struct StreamClaim {
    streams: Arc<ReadStreams>,
    stream: String,
}

impl Drop for StreamClaim {
    fn drop(&mut self) {
        self.streams.lock().remove(&self.stream);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_is_claimed_once_until_its_claim_is_dropped() {
        let streams = Arc::new(ReadStreams::default());
        let first = streams.claim("orders").expect("the first claim holds");
        assert!(matches!(
            streams.claim("orders"),
            Err(KinesisError::AlreadySubscribed { stream }) if stream == "orders"
        ));
        let other = streams
            .claim("audit")
            .expect("another stream is claimed on its own");
        drop(first);
        let again = streams
            .claim("orders")
            .expect("a dropped claim frees the stream");
        drop((other, again));
    }
}
