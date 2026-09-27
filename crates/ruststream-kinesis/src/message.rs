//! [`KinesisMessage`]: a delivered record whose acknowledgement is a checkpoint.

// Without the `testing` feature a settlement has one variant, so a `match` on it has a single arm;
// the match stays so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use std::sync::Arc;

use bytes::Bytes;
use ruststream::{AckError, BytesMut, HeaderMap, IncomingMessage, Partitioned, Positioned, Str};

#[cfg(feature = "testing")]
use crate::in_process::Replay;
use crate::lease::{LeaseKey, LeaseStore};
use crate::subscriber::KinesisSeeker;
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
/// by any consumer. Each header is a length-prefixed name and a length-prefixed value, so a
/// value is carried byte for byte whatever it holds.
pub(crate) const ENVELOPE_MAGIC: [u8; 4] = *b"RSK2";

/// The magic of the envelope earlier releases wrote, one `name: value` text line per header.
/// Records keep their bytes for as long as the stream retains them, so it is still read.
const TEXT_ENVELOPE_MAGIC: [u8; 4] = *b"RSK1";

/// The magic and the header-block length in front of the header block.
const ENVELOPE_PREFIX: usize = 8;

/// The length prefix of a header name or value.
const FIELD_LEN: usize = 4;

/// Encodes a payload with its user headers (partition key excluded - it travels natively).
pub(crate) fn encode_envelope(headers: &HeaderMap, payload: BytesMut) -> Vec<u8> {
    let user = || {
        headers
            .iter()
            .filter(|(name, _)| *name != PARTITION_KEY_HEADER)
    };
    let block: usize = user()
        .map(|(name, value)| 2 * FIELD_LEN + name.len() + value.len())
        .sum();
    if block == 0 {
        // `Vec::from` reclaims the buffer the framework wrote: a record with nothing in front of
        // its payload is that buffer.
        return Vec::from(payload);
    }
    let mut out = Vec::with_capacity(ENVELOPE_PREFIX + block + payload.len());
    out.extend_from_slice(&ENVELOPE_MAGIC);
    out.extend_from_slice(&field_len(block));
    for (name, value) in user() {
        out.extend_from_slice(&field_len(name.len()));
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&field_len(value.len()));
        out.extend_from_slice(value);
    }
    out.extend_from_slice(&payload);
    out
}

/// A length as the envelope writes it. A record is at most 1 MiB, so every length fits; one
/// that does not writes zero, and the service refuses the record for its size anyway.
fn field_len(len: usize) -> [u8; FIELD_LEN] {
    u32::try_from(len).unwrap_or(0).to_be_bytes()
}

/// Splits an enveloped payload back into headers and raw payload; a payload without the
/// magic, or whose header block does not parse, reads as headerless.
pub(crate) fn decode_envelope(data: &[u8]) -> (HeaderMap, Bytes) {
    let parsed = match data.get(..4) {
        Some(magic) if magic == ENVELOPE_MAGIC => envelope_parts(data, decode_fields),
        Some(magic) if magic == TEXT_ENVELOPE_MAGIC => {
            envelope_parts(data, |block| Some(decode_text(block)))
        }
        _ => None,
    };
    parsed.map_or_else(
        || (HeaderMap::new(), Bytes::copy_from_slice(data)),
        |(headers, payload)| (headers, Bytes::copy_from_slice(payload)),
    )
}

/// The headers and the payload of an enveloped record, the header block read by `decode`.
fn envelope_parts(
    data: &[u8],
    decode: fn(&[u8]) -> Option<HeaderMap>,
) -> Option<(HeaderMap, &[u8])> {
    let (len, rest) = read_len(data.get(4..)?)?;
    let block = rest.get(..len)?;
    Some((decode(block)?, &rest[len..]))
}

/// Reads one length prefix off the front of `data`.
fn read_len(data: &[u8]) -> Option<(usize, &[u8])> {
    let (len, rest) = data.split_first_chunk::<FIELD_LEN>()?;
    Some((usize::try_from(u32::from_be_bytes(*len)).ok()?, rest))
}

/// Reads one length-prefixed field off the front of `data`.
fn read_field(data: &[u8]) -> Option<(&[u8], &[u8])> {
    let (len, rest) = read_len(data)?;
    rest.split_at_checked(len)
}

/// The header block of an envelope: length-prefixed names and values.
fn decode_fields(mut block: &[u8]) -> Option<HeaderMap> {
    let mut headers = HeaderMap::new();
    while !block.is_empty() {
        let (name, rest) = read_field(block)?;
        let (value, rest) = read_field(rest)?;
        headers.insert(
            String::from_utf8(name.to_vec()).ok()?,
            Bytes::copy_from_slice(value),
        );
        block = rest;
    }
    Some(headers)
}

/// The header block of the text envelope earlier releases wrote.
fn decode_text(block: &[u8]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for line in String::from_utf8_lossy(block).lines() {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_owned(), value.trim().to_owned());
        }
    }
    headers
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
    /// Stream-wide; each shard opens at its own first record from that instant. The instant is
    /// matched against the arrival timestamp the service stamped on the record, and the service
    /// calls that stamp approximate: records that arrived around the same moment are not
    /// separated exactly.
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
    // The lease key and the owner are per-reader constants stamped onto every record it
    // forwards, and the per-delivery context reads the shard back: sharing them keeps both paths
    // to reference-count bumps instead of a string copy per record.
    pub(crate) lease: Arc<LeaseKey>,
    pub(crate) owner: Arc<str>,
    /// The reader's delivery generation at delivery time; a seek bumps the shared gate, and
    /// stale settlements skip checkpointing (the watermark was reset).
    pub(crate) epoch: u64,
    pub(crate) gate: Arc<std::sync::atomic::AtomicU64>,
}

/// How a delivery settles: against its shard's watermark and the lease store, or, under the
/// `testing` feature, against the in-process transport the test harness connected instead.
///
/// Without the feature there is one variant, so the type is the checkpoint settlement itself and
/// every `match` on it resolves at compile time: a production build carries no second settlement
/// and no branch to it.
pub(crate) enum Settle {
    Checkpoint(Settlement),
    #[cfg(feature = "testing")]
    InProcess(Replay),
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives the
// settlement exactly the size of the checkpoint it wraps.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Settle>() == size_of::<Settlement>());

impl Settle {
    fn shard(&self) -> &Arc<str> {
        match self {
            Self::Checkpoint(settlement) => settlement.lease.shard_shared(),
            #[cfg(feature = "testing")]
            Self::InProcess(replay) => replay.shard(),
        }
    }
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
    headers: HeaderMap,
    sequence: Arc<str>,
    seeker: KinesisSeeker,
    settlement: Settle,
}

impl std::fmt::Debug for KinesisMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("KinesisMessage");
        match &self.settlement {
            Settle::Checkpoint(settlement) => {
                debug.field("stream", &settlement.lease.stream());
            }
            #[cfg(feature = "testing")]
            Settle::InProcess(_) => {}
        }
        debug
            .field("shard", self.settlement.shard())
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

impl KinesisMessage {
    pub(crate) fn new(
        data: &[u8],
        partition_key: &str,
        sequence: &str,
        seeker: KinesisSeeker,
        settlement: Settle,
    ) -> Self {
        let (mut headers, payload) = decode_envelope(data);
        headers.insert(
            Str::from_static(PARTITION_KEY_HEADER),
            partition_key.to_owned(),
        );
        headers.insert(Str::from_static(SEQUENCE_HEADER), sequence.to_owned());
        headers.insert(
            Str::from_static(SHARD_HEADER),
            settlement.shard().to_string(),
        );
        Self {
            payload,
            headers,
            sequence: Arc::from(sequence),
            seeker,
            settlement,
        }
    }

    /// The shard this record arrived on; the per-delivery context borrows it.
    pub(crate) fn shard(&self) -> &Arc<str> {
        self.settlement.shard()
    }

    /// This record's sequence number; the per-delivery context borrows it.
    pub(crate) fn sequence(&self) -> &Arc<str> {
        &self.sequence
    }

    /// The subscription's reposition handle, minted once when the subscription opened.
    pub(crate) fn seeker(&self) -> &KinesisSeeker {
        &self.seeker
    }

    async fn settle(self) -> Result<(), AckError> {
        let settlement = match self.settlement {
            Settle::Checkpoint(settlement) => settlement,
            // Nothing is checkpointed in process: a handled record is simply consumed.
            #[cfg(feature = "testing")]
            Settle::InProcess(_) => return Ok(()),
        };
        let Settlement {
            tracker,
            index,
            store,
            lease,
            owner,
            epoch,
            gate,
        } = settlement;
        if gate.load(std::sync::atomic::Ordering::Acquire) != epoch {
            // The subscription repositioned after this delivery: its watermark was reset,
            // and a stale checkpoint would move the cursor somewhere the seek just left.
            return Ok(());
        }
        let Some(sequence) = tracker.settle(index) else {
            return Ok(()); // handled, but the watermark waits on an earlier record
        };
        match store.checkpoint(&lease, &owner, &sequence).await {
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
        KinesisPosition::sequence(&**self.settlement.shard(), &*self.sequence)
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

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    async fn ack(self) -> Result<(), AckError> {
        self.settle().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        if requeue {
            // Leaving the record unhandled wedges the watermark: no later checkpoint can
            // pass it, so the shard replays from here when its lease is next taken.
            #[cfg(feature = "testing")]
            if let Settle::InProcess(replay) = &self.settlement {
                replay.rewind();
            }
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
    use ruststream::BytesMut;

    use super::*;

    /// Content equality cannot tell a hand-over from a copy, so the buffer the framework wrote is
    /// identified by its address.
    #[test]
    fn a_record_without_an_envelope_keeps_the_buffer_the_framework_wrote() {
        let payload = BytesMut::from(&br#"{"id":1}"#[..]);
        let written = payload.as_ptr();

        let data = encode_envelope(&HeaderMap::new(), payload);

        assert_eq!(
            data.as_ptr(),
            written,
            "a publish carrying no header is the record's data as it stands, so the buffer is \
             handed over rather than copied"
        );
    }

    #[test]
    fn the_envelope_applies_only_when_user_headers_exist() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        // Only the partition key: no envelope, the payload stays plain.
        assert_eq!(
            encode_envelope(&headers, BytesMut::from(&b"raw"[..])),
            b"raw"
        );

        headers.insert("x-tenant", "acme");
        let enveloped = encode_envelope(&headers, BytesMut::from(&b"raw"[..]));
        assert_eq!(enveloped[0..4], ENVELOPE_MAGIC);
        let (decoded, payload) = decode_envelope(&enveloped);
        assert_eq!(decoded.get_str("x-tenant"), Some("acme"));
        assert!(decoded.get(PARTITION_KEY_HEADER).is_none());
        assert_eq!(payload.as_ref(), b"raw");
    }

    /// A header value is bytes, not text: whatever it holds - a byte that is not UTF-8, a line
    /// break, a colon, surrounding spaces - comes back exactly as it was written.
    #[test]
    fn a_header_value_travels_byte_for_byte() {
        let binary: &[u8] = &[0x00, 0x80, b'\r', b'\n', 0xfe, 0xff, 0xc3];
        let mut headers = HeaderMap::new();
        headers.insert("x-binary", Bytes::from_static(binary));
        headers.insert("x-text", " a: b\nc ");
        headers.insert("x-empty", "");

        let enveloped = encode_envelope(&headers, BytesMut::from(&b"raw"[..]));
        let (decoded, payload) = decode_envelope(&enveloped);

        assert_eq!(decoded.get("x-binary"), Some(binary));
        assert_eq!(decoded.get_str("x-text"), Some(" a: b\nc "));
        assert_eq!(decoded.get("x-empty"), Some(&b""[..]));
        assert_eq!(decoded.iter().count(), 3);
        assert_eq!(payload.as_ref(), b"raw");
    }

    /// A record an earlier release wrote keeps its text envelope for as long as the stream
    /// retains it, and still reads back with its headers.
    #[test]
    fn a_record_in_the_earlier_text_envelope_reads_back() {
        let block = b"x-tenant: acme\n";
        let mut data = b"RSK1".to_vec();
        data.extend_from_slice(&u32::try_from(block.len()).unwrap().to_be_bytes());
        data.extend_from_slice(block);
        data.extend_from_slice(b"raw");

        let (decoded, payload) = decode_envelope(&data);

        assert_eq!(decoded.get_str("x-tenant"), Some("acme"));
        assert_eq!(payload.as_ref(), b"raw");
    }

    /// A blob that starts like an envelope and does not parse as one is somebody else's payload,
    /// handed over whole rather than cut at a guessed boundary.
    #[test]
    fn a_blob_that_does_not_parse_as_an_envelope_reads_as_headerless() {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        let enveloped = encode_envelope(&headers, BytesMut::new());
        for cut in [6, enveloped.len() - 1] {
            let (decoded, payload) = decode_envelope(&enveloped[..cut]);
            assert!(decoded.is_empty(), "cut at {cut}");
            assert_eq!(payload.as_ref(), &enveloped[..cut]);
        }
        let mut not_utf8_name = enveloped;
        not_utf8_name[12] = 0xff;
        assert!(decode_envelope(&not_utf8_name).0.is_empty());
    }

    #[test]
    fn plain_payloads_read_as_headerless() {
        let (headers, payload) = decode_envelope(b"{\"id\":1}");
        assert!(headers.is_empty());
        assert_eq!(payload.as_ref(), b"{\"id\":1}");
    }
}
