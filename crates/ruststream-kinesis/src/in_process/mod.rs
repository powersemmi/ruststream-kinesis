//! The broker's in-process mode, behind the `testing` feature: the transport a connected broker
//! carries when the test harness connects it through `InProcess::connect_in_process` rather than
//! through `connect`.
//!
//! The connected broker, its subscriber, its publisher and its delivery type each carry this
//! transport as a variant of their own, so a service's routes, descriptors and publish policies
//! run against it unchanged. It has no configuration of its own. A record is written the way the
//! publisher writes it for the service - the partition key resolved by the same ladder, the
//! headers in the same envelope - and read back by the same decoding, so what a handler sees here
//! is what it sees from a shard. It never succeeds where the service fails: a stream name, a
//! partition key or a record size the service refuses is refused here with the same error, and so
//! is every handle used after `shutdown`.
//!
//! What it models: one retained log per stream, read by every subscription on that stream; a
//! subscription opening at the tip; the stream-wide and shard-scoped positions of `start_at(..)`
//! and the seek handle; and the replay of a record left unhandled, which reads the stream again
//! from that record on. What belongs to the service and is left to the live mode: a stream's
//! shards (a stream here has one, so every record is ordered against every other), leases and
//! checkpoints, retention, resharding, and the existence of a stream (every stream a service names
//! is there). A record left unhandled is read again at once here; the service reads it again when
//! the shard's lease is next taken.

mod deliveries;
mod log;
mod seek;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::Coordinator;
use ruststream::{BytesMut, OutgoingFor, OutgoingMessage, RawMessage, Str, Take};

pub(crate) use deliveries::{LogDeliveries, Replay};
pub(crate) use seek::LogSeeker;

use crate::error::KinesisError;
use crate::message::{PARTITION_KEY_HEADER, decode_envelope, encode_envelope};
use crate::publisher::{KinesisPublishOptions, resolve_partition_key};
use crate::stream::KinesisStream;
use crate::subscriber::KinesisSubscriber;
use log::StreamLog;

/// The longest stream name the service accepts.
const MAX_STREAM_NAME: usize = 128;

/// The longest partition key the service accepts, in characters.
const MAX_PARTITION_KEY: usize = 256;

/// The most one record may carry: its data blob and its partition key together.
const MAX_RECORD: usize = 1024 * 1024;

/// The in-process transport of one connected broker: the streams, the harness coordinator, and
/// whether the broker has shut down.
#[derive(Debug, Default)]
pub(crate) struct Bus {
    log: StreamLog,
    coordinator: OnceLock<Coordinator>,
    /// Set by `shutdown`. A publisher or a seek handle is an aliasing handle, and the service's
    /// aliasing handles report `NotConnected` after shutdown rather than succeeding against a
    /// dead connection; the in-process ones report the same.
    closed: AtomicBool,
}

impl Bus {
    pub(crate) fn new() -> Arc<Self> {
        Arc::default()
    }

    pub(crate) fn log(&self) -> &StreamLog {
        &self.log
    }

    /// Installs the harness coordinator; a second install is ignored.
    pub(crate) fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    pub(crate) fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    pub(crate) fn ensure_open(&self) -> Result<(), KinesisError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(KinesisError::NotConnected);
        }
        Ok(())
    }

    /// Shuts the transport down: every handle reports `NotConnected` from now on, and every
    /// subscription's stream ends.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.log.clear();
    }

    /// Opens a subscription on the stream `descriptor` names, refusing what the service refuses.
    ///
    /// # Errors
    ///
    /// Returns [`KinesisError::NotConnected`] after `shutdown`, and [`KinesisError::Stream`] for a
    /// stream name the service does not accept.
    pub(crate) fn subscribe(
        self: &Arc<Self>,
        descriptor: &KinesisStream,
    ) -> Result<KinesisSubscriber, KinesisError> {
        self.ensure_open()?;
        let stream = descriptor.stream();
        check_stream(stream).map_err(|reason| KinesisError::Stream {
            stream: stream.to_owned(),
            source: Box::from(reason),
        })?;
        Ok(KinesisSubscriber::in_process(
            stream.to_owned(),
            LogDeliveries::open(self, stream),
        ))
    }

    /// Publishes a record the way the service's publisher does: the partition key resolved by the
    /// same ladder, the headers written into the same envelope.
    ///
    /// # Errors
    ///
    /// Returns [`KinesisError::NotConnected`] after `shutdown`, and [`KinesisError::Publish`] for
    /// a record the service refuses.
    pub(crate) fn put(
        &self,
        msg: OutgoingFor<'_, Take>,
        options: Option<&KinesisPublishOptions>,
        from_policy: Option<&str>,
    ) -> Result<(), KinesisError> {
        self.ensure_open()?;
        let partition_key = resolve_partition_key(options, msg.headers(), from_policy);
        let (stream, payload, headers) = msg.into_parts();
        let data = encode_envelope(&headers, payload);
        self.append(stream, partition_key, data)
    }

    /// Takes a record from outside the service, as another producer writes one to the stream.
    ///
    /// # Errors
    ///
    /// Returns what [`put`](Self::put) returns for the same record.
    pub(crate) fn inject(&self, message: OutgoingMessage<'_>) -> Result<(), KinesisError> {
        self.ensure_open()?;
        let partition_key = resolve_partition_key(None, message.headers(), None);
        let (stream, payload, headers) = message.into_parts();
        let data = encode_envelope(&headers, BytesMut::from(payload));
        self.append(stream, partition_key, data)
    }

    fn append(
        &self,
        stream: &str,
        partition_key: String,
        data: Vec<u8>,
    ) -> Result<(), KinesisError> {
        check_record(stream, &partition_key, data.len()).map_err(|reason| {
            KinesisError::Publish {
                stream: stream.to_owned(),
                source: Box::from(reason),
            }
        })?;
        self.log.append(
            stream,
            Arc::from(partition_key),
            Bytes::from(data),
            self.coordinator(),
        );
        Ok(())
    }

    /// Every record published to `stream`, as a consumer reads it: the payload out of the header
    /// envelope, the headers it carried, and the record's partition key under
    /// [`PARTITION_KEY_HEADER`].
    pub(crate) fn published(&self, stream: &str) -> Vec<RawMessage> {
        self.log
            .records(stream)
            .into_iter()
            .map(|record| {
                let (mut headers, payload) = decode_envelope(&record.data);
                headers.insert(
                    Str::from_static(PARTITION_KEY_HEADER),
                    record.partition_key.to_string(),
                );
                RawMessage::new(stream.to_owned(), payload).with_headers(headers)
            })
            .collect()
    }
}

/// Refuses a stream name the service refuses: one to 128 characters, each a letter, a digit,
/// `_`, `.` or `-`.
fn check_stream(stream: &str) -> Result<(), String> {
    if stream.is_empty() || stream.len() > MAX_STREAM_NAME {
        return Err(format!(
            "a stream name is 1 to {MAX_STREAM_NAME} characters long, this one is {}",
            stream.len()
        ));
    }
    if let Some(refused) = stream
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-')))
    {
        return Err(format!(
            "a stream name holds letters, digits, '_', '.' and '-', and {refused:?} is none of \
             them"
        ));
    }
    Ok(())
}

/// Refuses a record the service refuses: a stream name it does not accept, a partition key that
/// is empty or longer than 256 characters, or a record whose data and partition key together
/// exceed 1 MiB.
fn check_record(stream: &str, partition_key: &str, data: usize) -> Result<(), String> {
    check_stream(stream)?;
    let key_chars = partition_key.chars().count();
    if key_chars == 0 || key_chars > MAX_PARTITION_KEY {
        return Err(format!(
            "a partition key is 1 to {MAX_PARTITION_KEY} characters long, this one is \
             {key_chars}"
        ));
    }
    let size = data + partition_key.len();
    if size > MAX_RECORD {
        return Err(format!(
            "a record carries at most {MAX_RECORD} bytes of data and partition key, this one \
             carries {size}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The characters the service's stream-name pattern admits, and the ones it does not: an ARN
    /// is not a stream name, and neither is a name with a slash.
    #[test]
    fn a_stream_name_takes_what_the_service_takes() {
        assert!(check_stream("orders.dead-letter_v2").is_ok());
        assert!(check_stream("").is_err());
        assert!(check_stream(&"s".repeat(129)).is_err());
        assert!(check_stream("orders/dead").is_err());
        assert!(check_stream("arn:aws:kinesis:us-east-1:123456789012:stream/orders").is_err());
    }

    /// The partition key and the size limits the service applies to one record.
    #[test]
    fn a_record_takes_what_the_service_takes() {
        assert!(check_record("orders", "k", MAX_RECORD - 1).is_ok());
        assert!(check_record("orders", "", 1).is_err());
        assert!(check_record("orders", &"k".repeat(257), 1).is_err());
        assert!(check_record("orders", &"é".repeat(256), 1).is_ok());
        assert!(check_record("orders", "k", MAX_RECORD).is_err());
    }
}
