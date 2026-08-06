//! [`KinesisMessage`]: a delivered record whose acknowledgement is a checkpoint.

use std::sync::Arc;

use bytes::Bytes;
use ruststream::{AckError, Headers, IncomingMessage, Partitioned, Positioned};

use crate::lease::LeaseStore;
use crate::track::Watermark;

/// Header carrying the partition key, mapped onto the record's own partition key.
///
/// Mirrors the in-memory broker's convention, so services can switch brokers without changing
/// their headers.
pub const PARTITION_KEY_HEADER: &str = "partition-key";

/// Header exposing the record's sequence number on received messages.
pub const SEQUENCE_HEADER: &str = "kinesis-sequence-number";

/// Header exposing the shard a record arrived on.
pub const SHARD_HEADER: &str = "kinesis-shard-id";

/// The KPL aggregation magic prefix; such records are refused loudly (deaggregation is a
/// follow-up), never handed to a handler as opaque protobuf.
pub(crate) const KPL_MAGIC: [u8; 4] = [0xF3, 0x89, 0x9A, 0xC2];

/// The conditional header-envelope magic: Kinesis records carry only a data blob and a
/// partition key, so user headers (beyond the partition key, which travels natively) ride a
/// small prefix - applied only when such headers are present, so plain payloads stay readable
/// by any consumer.
pub(crate) const ENVELOPE_MAGIC: [u8; 4] = *b"RSK1";

/// Encodes a payload with its user headers (partition key excluded - it travels natively).
pub(crate) fn encode_envelope(headers: &Headers, payload: &[u8]) -> Vec<u8> {
    let mut lines = String::new();
    for (name, value) in headers.iter() {
        if name == PARTITION_KEY_HEADER {
            continue;
        }
        lines.push_str(name);
        lines.push_str(": ");
        lines.push_str(&String::from_utf8_lossy(value));
        lines.push('\n');
    }
    if lines.is_empty() {
        return payload.to_vec();
    }
    let header_bytes = lines.as_bytes();
    let mut out = Vec::with_capacity(8 + header_bytes.len() + payload.len());
    out.extend_from_slice(&ENVELOPE_MAGIC);
    out.extend_from_slice(&u32::try_from(header_bytes.len()).unwrap_or(0).to_be_bytes());
    out.extend_from_slice(header_bytes);
    out.extend_from_slice(payload);
    out
}

/// Splits an enveloped payload back into headers and raw payload; a payload without the
/// magic reads as headerless.
pub(crate) fn decode_envelope(data: &[u8]) -> (Headers, Bytes) {
    if data.len() >= 8 && data[0..4] == ENVELOPE_MAGIC {
        let len = u32::from_be_bytes([data[4], data[5], data[6], data[7]]) as usize;
        if data.len() >= 8 + len {
            let mut headers = Headers::new();
            let text = String::from_utf8_lossy(&data[8..8 + len]);
            for line in text.lines() {
                if let Some((name, value)) = line.split_once(':') {
                    headers.insert(name.trim().to_owned(), value.trim().to_owned());
                }
            }
            return (headers, Bytes::copy_from_slice(&data[8 + len..]));
        }
    }
    (Headers::new(), Bytes::copy_from_slice(data))
}

/// A position in the stream's retained log: the whole start vocabulary of this broker,
/// accepted by [`Seeker::seek`](ruststream::Seeker::seek) and by the `start_at(..)` clause of
/// `#[subscriber(..)]`.
///
/// Repositioning resets the checkpoint bookkeeping of every shard it moves: acknowledgements
/// of records delivered before the seek stop advancing the watermark, so a stale checkpoint
/// cannot drag the cursor back over the position just taken. Records from the new position
/// onward are delivered again, which at-least-once permits.
///
/// Without a position a subscription resumes from the stored checkpoint of each shard, and
/// starts at the tip on a shard that has none.
///
/// # Examples
///
/// ```
/// use ruststream_kinesis::KinesisPosition;
///
/// // Every retained record on every shard, replayed from the trim horizon.
/// let backlog = KinesisPosition::horizon();
/// # let _ = backlog;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KinesisPosition {
    /// The trim horizon: everything the stream still retains.
    ///
    /// Stream-wide - it applies to every shard of the subscription, including shards
    /// discovered later (the children of a split or merge).
    Horizon,
    /// The tip: only records published after the reposition.
    ///
    /// Stream-wide, and the position a shard without a checkpoint starts at by default.
    Latest,
    /// The first record at or after this timestamp, in milliseconds since the Unix epoch.
    ///
    /// Stream-wide; each shard opens at its own first record from that instant.
    Timestamp(u64),
    /// Exactly one record on one shard.
    ///
    /// This is the pinned form the framework defines for captured positions
    /// ([`Positioned::position`]): seeking to one redelivers that very record. It addresses a
    /// single shard, so it moves that shard's reader only, the way a partitioned log seeks
    /// per partition, and the shard must be owned and live.
    Sequence {
        /// The shard the position lives on.
        shard: String,
        /// The record's sequence number.
        sequence: String,
    },
}

impl KinesisPosition {
    /// The trim horizon, for every shard: see [`KinesisPosition::Horizon`].
    #[must_use]
    pub const fn horizon() -> Self {
        Self::Horizon
    }

    /// The tip, for every shard: see [`KinesisPosition::Latest`].
    #[must_use]
    pub const fn latest() -> Self {
        Self::Latest
    }

    /// A wall-clock instant (milliseconds since the Unix epoch), for every shard: see
    /// [`KinesisPosition::Timestamp`].
    #[must_use]
    pub const fn timestamp(millis: u64) -> Self {
        Self::Timestamp(millis)
    }

    /// One record on one shard: see [`KinesisPosition::Sequence`].
    ///
    /// Captured positions come from [`Positioned::position`]; this constructor is for a
    /// sequence number carried in from elsewhere (an operator's replay request, say).
    #[must_use]
    pub fn sequence(shard: impl Into<String>, sequence: impl Into<String>) -> Self {
        Self::Sequence {
            shard: shard.into(),
            sequence: sequence.into(),
        }
    }
}

pub(crate) struct Settlement {
    pub(crate) tracker: Arc<Watermark>,
    pub(crate) index: u64,
    pub(crate) store: Arc<dyn LeaseStore>,
    pub(crate) shard: String,
    pub(crate) owner: String,
    /// The reader's delivery generation at delivery time; a seek bumps the shared gate, and
    /// stale settlements skip checkpointing (the watermark was reset).
    pub(crate) epoch: u64,
    pub(crate) gate: Arc<std::sync::atomic::AtomicU64>,
}

/// A record delivered by a [`KinesisSubscriber`](crate::KinesisSubscriber).
///
/// Acknowledgement is a per-shard checkpoint, not per-message settlement: `ack` marks this
/// record handled, and when every earlier record on the shard is handled too, the watermark
/// advances and is persisted to the lease store. `nack(requeue = true)` leaves the record
/// unhandled - the watermark stops advancing, and the records from it onward redeliver when
/// the shard's lease is next taken (a sharded log repositions; it cannot requeue one
/// message). `nack(requeue = false)` skips the record (checkpoints past it).
pub struct KinesisMessage {
    payload: Bytes,
    headers: Headers,
    sequence: String,
    settlement: Settlement,
}

impl std::fmt::Debug for KinesisMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KinesisMessage")
            .field("shard", &self.settlement.shard)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl KinesisMessage {
    pub(crate) fn new(
        data: &[u8],
        partition_key: &str,
        sequence: &str,
        settlement: Settlement,
    ) -> Self {
        let (mut headers, payload) = decode_envelope(data);
        headers.insert(PARTITION_KEY_HEADER, partition_key.to_owned());
        headers.insert(SEQUENCE_HEADER, sequence.to_owned());
        headers.insert(SHARD_HEADER, settlement.shard.clone());
        Self {
            payload,
            headers,
            sequence: sequence.to_owned(),
            settlement,
        }
    }

    async fn settle(self) -> Result<(), AckError> {
        let Settlement {
            tracker,
            index,
            store,
            shard,
            owner,
            epoch,
            gate,
        } = self.settlement;
        if gate.load(std::sync::atomic::Ordering::Acquire) != epoch {
            // The subscription repositioned after this delivery: its watermark was reset,
            // and a stale checkpoint would move the cursor somewhere the seek just left.
            return Ok(());
        }
        let Some(sequence) = tracker.settle(index) else {
            return Ok(()); // handled, but the watermark waits on an earlier record
        };
        match store.checkpoint(&shard, &owner, &sequence).await {
            // A fenced checkpoint (another owner took the shard) is fine: the record was
            // handled, and the new owner replays from its checkpoint, which at-least-once
            // permits.
            Ok(_) => Ok(()),
            Err(err) => Err(AckError::Broker(err)),
        }
    }
}

impl Positioned for KinesisMessage {
    type Position = KinesisPosition;

    fn position(&self) -> KinesisPosition {
        KinesisPosition::sequence(self.settlement.shard.clone(), self.sequence.clone())
    }
}

impl Partitioned for KinesisMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers.get(PARTITION_KEY_HEADER)
    }
}

impl IncomingMessage for KinesisMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &Headers {
        &self.headers
    }

    async fn ack(self) -> Result<(), AckError> {
        self.settle().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        if requeue {
            // Leaving the record unhandled wedges the watermark: no later checkpoint can
            // pass it, so the shard replays from here when its lease is next taken.
            Ok(())
        } else {
            self.settle().await
        }
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_envelope_applies_only_when_user_headers_exist() {
        let mut headers = Headers::new();
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        // Only the partition key: no envelope, the payload stays plain.
        assert_eq!(encode_envelope(&headers, b"raw"), b"raw");

        headers.insert("x-tenant", "acme");
        let enveloped = encode_envelope(&headers, b"raw");
        assert_eq!(enveloped[0..4], ENVELOPE_MAGIC);
        let (decoded, payload) = decode_envelope(&enveloped);
        assert_eq!(decoded.get_str("x-tenant"), Some("acme"));
        assert!(decoded.get(PARTITION_KEY_HEADER).is_none());
        assert_eq!(payload.as_ref(), b"raw");
    }

    #[test]
    fn plain_payloads_read_as_headerless() {
        let (headers, payload) = decode_envelope(b"{\"id\":1}");
        assert!(headers.is_empty());
        assert_eq!(payload.as_ref(), b"{\"id\":1}");
    }
}
