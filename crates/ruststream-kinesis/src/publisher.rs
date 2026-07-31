//! [`KinesisPublisher`] and its [`KinesisPublish`] policy.

use aws_sdk_kinesis::primitives::Blob;
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher};

use crate::broker::{ConnectedKinesisBroker, Core, CoreCell};
use crate::error::{KinesisError, sdk_err};
use crate::message::{PARTITION_KEY_HEADER, encode_envelope};

/// Publishes records to Kinesis streams (the destination is the stream name or ARN).
///
/// The `partition-key` header becomes the record's partition key - the unit of shard routing
/// and per-key ordering; without one, a process-unique key spreads records across shards.
/// User headers beyond the partition key travel in a small conditional envelope (Kinesis
/// records carry only a data blob and a partition key); plain payloads stay unenveloped.
/// Buildable before `connect` and usable until `shutdown`; afterwards every publish reports
/// [`KinesisError::NotConnected`].
#[derive(Clone)]
pub struct KinesisPublisher {
    cell: CoreCell,
}

impl std::fmt::Debug for KinesisPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisPublisher").finish_non_exhaustive()
    }
}

impl KinesisPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self { cell }
    }

    fn core(&self) -> Result<&Core, KinesisError> {
        let core = self.cell.get().ok_or(KinesisError::NotConnected)?;
        core.ensure_open()?;
        Ok(core)
    }
}

fn spread_key() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "rs-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

impl Publisher for KinesisPublisher {
    type Error = KinesisError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let core = self.core()?;
        let partition_key = msg
            .headers()
            .get(PARTITION_KEY_HEADER)
            .map_or_else(spread_key, |key| String::from_utf8_lossy(key).into_owned());
        let data = encode_envelope(msg.headers(), msg.payload());
        core.client
            .put_record()
            .stream_name(msg.name())
            .partition_key(partition_key)
            .data(Blob::new(data))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| KinesisError::Publish {
                stream: msg.name().to_owned(),
                source: sdk_err(&e),
            })
    }
}

/// The publish policy for [`KinesisPublisher`]: pure declaration, constructible anywhere,
/// paired with the connected broker by the runtime after `connect`.
///
/// # Examples
///
/// ```
/// use ruststream_kinesis::KinesisPublish;
///
/// let policy = KinesisPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct KinesisPublish;

impl PublishPolicy<ConnectedKinesisBroker> for KinesisPublish {
    type Live = KinesisPublisher;

    async fn pair(self, connected: &ConnectedKinesisBroker) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
    }
}
