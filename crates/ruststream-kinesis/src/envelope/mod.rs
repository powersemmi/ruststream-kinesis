//! The envelope: how a record's data carries the headers beside the payload.
//!
//! A Kinesis record holds a data blob and a partition key, nothing else. The partition key rides
//! the record's own field in every format; the other headers ride the data, in the layout an
//! [`Envelope`] writes and reads:
//!
//! - [`Rsk`], the default: the payload as it stands when there is no header, a small binary
//!   prefix in front of it when there is one. Every release of this crate reads it.
//! - [`Framed`]: headers and payload as one document of a `RustStream` codec, which a consumer in
//!   any language reads with its own JSON, CBOR or msgpack library.
//! - [`NoOp`]: the record's data is the payload and nothing else.
//!
//! The envelope is a type, chosen the way a codec is: the broker's default
//! ([`KinesisBroker::envelope`](crate::KinesisBroker::envelope)), overridden by a subscription
//! descriptor ([`KinesisStream::envelope`](crate::KinesisStream::envelope)) or a publish policy
//! ([`KinesisPublish::envelope`](crate::KinesisPublish::envelope)). A delivery finds its payload
//! without reading the headers and reads them on the first call that asks for them.

mod framed;
mod noop;
mod rsk;

use std::error::Error as StdError;
use std::ops::Range;

use ruststream::codec::CodecError;
use ruststream::{BytesMut, HeaderMap};

pub use framed::{FrameCodec, Framed};
pub use noop::NoOp;
pub use rsk::Rsk;

use crate::message::{PARTITION_KEY_HEADER, SEQUENCE_HEADER, SHARD_HEADER};

/// A layout of a record's data: where the headers go and where the payload sits.
///
/// The crate ships three, [`Rsk`] (the default), [`Framed`] and [`NoOp`]; a service implements
/// its own for a layout another system already reads. An envelope is a value of its own type, so
/// choosing one costs nothing per record: no dynamic call and no shared pointer. It is cloned
/// into every delivery, so it is a unit struct or a value as cheap to clone as one.
///
/// A record carries three headers natively: the partition key, which is the record's own field,
/// and the sequence number and the shard, which the service stamps on every read. An envelope
/// never sees them; [`EnvelopeHeaders`] hands it the others.
///
/// Reading is split in two. [`body`](Self::body) finds the payload and runs for every delivery;
/// [`read_headers`](Self::read_headers) runs once, when something reads the delivery's headers.
/// A record the envelope does not recognise is somebody else's payload: `body` answers the whole
/// record and `read_headers` adds nothing.
///
/// # Examples
///
/// A layout an existing producer writes: one header line `tenant=<name>`, a line break, then the
/// payload. Records without the line are read as payload alone.
///
/// ```
/// # mod demo {
/// use ruststream::BytesMut;
/// use ruststream::HeaderMap;
/// use ruststream_kinesis::envelope::{Body, Envelope, EnvelopeError, EnvelopeHeaders, MalformedHeaders};
/// use ruststream_kinesis::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Debug, Clone, Copy, Default)]
/// pub struct TenantLine;
///
/// const PREFIX: &[u8] = b"tenant=";
///
/// impl TenantLine {
///     /// The tenant and where the payload starts, when the record begins with the line.
///     fn split(record: &[u8]) -> Option<(&[u8], usize)> {
///         let line = record.strip_prefix(PREFIX)?;
///         let end = line.iter().position(|byte| *byte == b'\n')?;
///         Some((&line[..end], PREFIX.len() + end + 1))
///     }
/// }
///
/// impl Envelope for TenantLine {
///     const NAME: &'static str = "tenant-line";
///
///     fn seal(
///         &self,
///         headers: EnvelopeHeaders<'_>,
///         payload: BytesMut,
///     ) -> Result<Vec<u8>, EnvelopeError> {
///         let mut record = Vec::with_capacity(payload.len() + 32);
///         for (name, value) in headers.iter() {
///             if name != "tenant" {
///                 return Err(EnvelopeError::unsupported_header(name));
///             }
///             record.extend_from_slice(PREFIX);
///             record.extend_from_slice(value);
///             record.push(b'\n');
///         }
///         record.extend_from_slice(&payload);
///         Ok(record)
///     }
///
///     fn body(&self, record: &[u8]) -> Body {
///         Self::split(record).map_or(Body::Whole, |(_, start)| Body::Within(start..record.len()))
///     }
///
///     fn read_headers(
///         &self,
///         record: &[u8],
///         _body: &Body,
///         headers: &mut HeaderMap,
///     ) -> Result<(), MalformedHeaders> {
///         if let Some((tenant, _)) = Self::split(record) {
///             headers.insert("tenant", tenant.to_vec());
///         }
///         Ok(())
///     }
/// }
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(KinesisStream::new("orders"))]
/// async fn on_order(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
///     tracing::info!(order.id, tenant = ?ctx.headers().get_str("tenant"), "order");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
///         b.include(on_order.envelope(TenantLine));
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a Kinesis envelope",
    note = "an envelope is `Rsk` (the default), `Framed::new(codec)`, `NoOp`, or a type of the \
            service that implements `ruststream_kinesis::envelope::Envelope`"
)]
pub trait Envelope: Clone + Send + Sync + 'static {
    /// The name the generated `AsyncAPI` document reports for this layout.
    const NAME: &'static str;

    /// The media type of the record's data when the envelope frames the payload in a document
    /// of its own, reported by the generated `AsyncAPI` document. `None` when the data is the
    /// payload, in the payload's own media type.
    const CONTENT_TYPE: Option<&'static str> = None;

    /// Writes a record's data from the headers and the payload of a publish.
    ///
    /// The payload is the buffer the framework encoded the message into; an envelope that puts
    /// nothing in front of it returns it as it stands (`Vec::from(payload)`) without a copy.
    ///
    /// # Errors
    ///
    /// [`EnvelopeError`] when the layout has no place for a header the publish carries, or the
    /// envelope cannot encode it. The publish fails with it; nothing is written.
    fn seal(
        &self,
        headers: EnvelopeHeaders<'_>,
        payload: BytesMut,
    ) -> Result<Vec<u8>, EnvelopeError>;

    /// Where the payload sits in a record's data, found without reading the headers.
    fn body(&self, record: &[u8]) -> Body;

    /// Adds the headers a record's data carries to `headers`. Runs at most once per delivery,
    /// on the first read of its headers, and is handed the [`Body`] that [`body`](Self::body)
    /// found, so what that pass learned about the record is not learned again.
    ///
    /// # Errors
    ///
    /// [`MalformedHeaders`] when the record is an envelope whose header block does not parse.
    /// The delivery then has no envelope headers, and the subscription logs it.
    fn read_headers(
        &self,
        record: &[u8],
        body: &Body,
        headers: &mut HeaderMap,
    ) -> Result<(), MalformedHeaders>;
}

/// Where a record's payload sits, as an [`Envelope`] finds it.
///
/// # Examples
///
/// An envelope that puts a fixed four-byte tag in front of every payload it writes:
///
/// ```
/// # mod demo {
/// use ruststream::{BytesMut, HeaderMap};
/// use ruststream_kinesis::envelope::{Body, Envelope, EnvelopeError, EnvelopeHeaders, MalformedHeaders};
/// use ruststream_kinesis::prelude::*;
///
/// const TAG: &[u8; 4] = b"ORD1";
///
/// #[derive(Debug, Clone, Copy, Default)]
/// pub struct Tagged;
///
/// impl Envelope for Tagged {
///     const NAME: &'static str = "tagged";
///
///     fn seal(
///         &self,
///         headers: EnvelopeHeaders<'_>,
///         payload: BytesMut,
///     ) -> Result<Vec<u8>, EnvelopeError> {
///         if let Some((name, _)) = headers.iter().next() {
///             return Err(EnvelopeError::unsupported_header(name));
///         }
///         let mut record = TAG.to_vec();
///         record.extend_from_slice(&payload);
///         Ok(record)
///     }
///
///     fn body(&self, record: &[u8]) -> Body {
///         if record.starts_with(TAG) {
///             Body::Within(TAG.len()..record.len())
///         } else {
///             Body::Whole
///         }
///     }
///
///     fn read_headers(
///         &self,
///         _record: &[u8],
///         _body: &Body,
///         _headers: &mut HeaderMap,
///     ) -> Result<(), MalformedHeaders> {
///         Ok(())
///     }
/// }
///
/// #[derive(serde::Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(KinesisStream::new("orders"))]
/// async fn on_order(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "order");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
///         b.include(on_order.envelope(Tagged));
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    /// The whole record is the payload: it carries no envelope, or one this layout does not
    /// recognise.
    Whole,
    /// The payload is this range of the record. A range that does not fit the record reads as
    /// [`Body::Whole`].
    Within(Range<usize>),
    /// The payload is not a slice of the record (a framed document that encodes it), so the
    /// envelope hands over the decoded bytes.
    Decoded(Vec<u8>),
}

impl Body {
    /// Settles a range against the record it points into: one that does not fit is the whole
    /// record, so a delivery never panics on what an envelope answered.
    pub(crate) fn fit(self, record_len: usize) -> Self {
        match self {
            Self::Within(range) if range.start > range.end || range.end > record_len => Self::Whole,
            body => body,
        }
    }

    /// The payload this body names in `record`.
    pub(crate) fn payload<'a>(&'a self, record: &'a [u8]) -> &'a [u8] {
        match self {
            Self::Whole => record,
            Self::Within(range) => record.get(range.clone()).unwrap_or(record),
            Self::Decoded(bytes) => bytes,
        }
    }
}

/// The headers of a publish that an envelope writes: every header except the three a record
/// carries natively ([`PARTITION_KEY_HEADER`], [`SEQUENCE_HEADER`] and [`SHARD_HEADER`]).
///
/// # Examples
///
/// An envelope that refuses a record with more than one header, as a consumer that reads a single
/// one would:
///
/// ```
/// # mod demo {
/// use ruststream::{BytesMut, HeaderMap};
/// use ruststream_kinesis::envelope::{
///     Body, Envelope, EnvelopeError, EnvelopeHeaders, MalformedHeaders, Rsk,
/// };
/// use ruststream_kinesis::prelude::*;
///
/// #[derive(Debug, Clone, Copy, Default)]
/// pub struct OneHeader;
///
/// impl Envelope for OneHeader {
///     const NAME: &'static str = "one-header";
///
///     fn seal(
///         &self,
///         headers: EnvelopeHeaders<'_>,
///         payload: BytesMut,
///     ) -> Result<Vec<u8>, EnvelopeError> {
///         if let Some((name, _)) = headers.iter().nth(1) {
///             return Err(EnvelopeError::unsupported_header(name));
///         }
///         Rsk.seal(headers, payload)
///     }
///
///     fn body(&self, record: &[u8]) -> Body {
///         Rsk.body(record)
///     }
///
///     fn read_headers(
///         &self,
///         record: &[u8],
///         body: &Body,
///         headers: &mut HeaderMap,
///     ) -> Result<(), MalformedHeaders> {
///         Rsk.read_headers(record, body, headers)
///     }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(KinesisBroker::new().envelope(OneHeader), |_| {})
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy)]
pub struct EnvelopeHeaders<'a> {
    headers: &'a HeaderMap,
}

impl<'a> EnvelopeHeaders<'a> {
    pub(crate) const fn new(headers: &'a HeaderMap) -> Self {
        Self { headers }
    }

    /// The headers the envelope writes, as `(name, value)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&'a str, &'a [u8])> + 'a {
        self.headers.iter().filter(|(name, _)| {
            !matches!(*name, PARTITION_KEY_HEADER | SEQUENCE_HEADER | SHARD_HEADER)
        })
    }

    /// Whether the publish carries no header for the envelope to write.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.iter().next().is_none()
    }
}

/// A record is an envelope whose header block does not parse.
///
/// [`Envelope::read_headers`] answers it; the delivery then carries no envelope headers, and the
/// subscription logs it with the stream and the envelope.
///
/// # Examples
///
/// See [`Envelope`]: an envelope whose header block always parses answers `Ok(())`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, thiserror::Error)]
#[error("the record's header block does not parse")]
pub struct MalformedHeaders;

/// Why an [`Envelope`] could not write a record.
///
/// A publish that meets it fails with
/// [`KinesisError::Envelope`](crate::KinesisError::Envelope), which names the stream.
///
/// # Examples
///
/// A publish of a header through [`NoOp`] fails, naming the header and the stream:
///
/// ```
/// # #[cfg(feature = "testing")]
/// # mod demo {
/// use ruststream::testing::InProcess;
/// use ruststream::{HeaderMap, OutgoingMessage};
/// use ruststream_kinesis::envelope::EnvelopeError;
/// use ruststream_kinesis::prelude::*;
/// use ruststream_kinesis::KinesisError;
///
/// pub async fn refuse() -> Result<(), KinesisError> {
///     let connected = KinesisBroker::new().envelope(NoOp).connect_in_process().await?;
///     let mut headers = HeaderMap::new();
///     headers.insert("x-tenant", "acme");
///     let refused = connected
///         .publisher()
///         .publish(OutgoingMessage::new("orders", b"{}").with_headers(headers), None)
///         .await;
///     match refused {
///         Err(KinesisError::Envelope { stream, source: EnvelopeError::UnsupportedHeader { header } }) => {
///             tracing::warn!(stream, header, "the raw stream carries no headers");
///             Ok(())
///         }
///         other => other,
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EnvelopeError {
    /// The layout has no place for this header.
    #[error("the envelope has no place for header '{header}'")]
    UnsupportedHeader {
        /// The header the publish carried.
        header: String,
    },
    /// A framed document carries a header value as text, and this one is not UTF-8.
    #[error(
        "header '{header}' is not UTF-8, and the framed document carries header values as text"
    )]
    HeaderNotText {
        /// The header whose value is not text.
        header: String,
    },
    /// The codec of a framed document could not encode the record.
    #[error("the framed document does not encode: {0}")]
    Codec(#[source] CodecError),
    /// An envelope of the service refused the record for a reason of its own.
    #[error("{0}")]
    Other(#[source] Box<dyn StdError + Send + Sync>),
}

impl EnvelopeError {
    /// The layout has no place for header `name`.
    ///
    /// # Examples
    ///
    /// See [`Envelope`]: its example refuses every header but the one its layout carries.
    #[must_use]
    pub fn unsupported_header(name: &str) -> Self {
        Self::UnsupportedHeader {
            header: name.to_owned(),
        }
    }
}

/// The envelope of a descriptor or a publish policy that names none: the broker's own.
///
/// It is the default of [`KinesisStream`](crate::KinesisStream)'s and
/// [`KinesisPublish`](crate::KinesisPublish)'s envelope parameter, so a mount site that says
/// nothing reads and writes in the layout the broker was given.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::codec::JsonCodec;
/// use ruststream_kinesis::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// /// Reads in the broker's framed JSON: the descriptor names no envelope of its own.
/// #[subscriber(KinesisStream::new("orders"))]
/// async fn on_order(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "order");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let broker = KinesisBroker::new().envelope(Framed::new(JsonCodec));
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
///         b.include(on_order);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Inherit;

/// Resolves the envelope a mount site names against the broker's default: [`Inherit`] takes the
/// broker's, an [`Envelope`] keeps itself.
///
/// It is what the descriptor's and the policy's envelope parameter is bounded on, so the choice
/// resolves at compile time.
///
/// # Examples
///
/// See [`Inherit`] and [`KinesisStream::envelope`](crate::KinesisStream::envelope).
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a Kinesis envelope",
    note = "a mount site names `Rsk`, `Framed::new(codec)`, `NoOp` or an envelope of the \
            service, or names none and reads in the broker's"
)]
pub trait ChooseEnvelope<Default: Envelope>: Clone + Send + Sync + 'static {
    /// The envelope that applies.
    type Chosen: Envelope;

    /// The envelope that applies, given the broker's.
    fn choose(self, default: &Default) -> Self::Chosen;

    /// The envelope named here, or `None` where the broker's applies. The generated `AsyncAPI`
    /// document reports what a mount site names; the broker's own is on the server.
    fn declared() -> Option<(&'static str, Option<&'static str>)>;
}

impl<Default: Envelope> ChooseEnvelope<Default> for Inherit {
    type Chosen = Default;

    fn choose(self, default: &Default) -> Default {
        default.clone()
    }

    fn declared() -> Option<(&'static str, Option<&'static str>)> {
        None
    }
}

impl<Named: Envelope, Default: Envelope> ChooseEnvelope<Default> for Named {
    type Chosen = Named;

    fn choose(self, _default: &Default) -> Named {
        self
    }

    fn declared() -> Option<(&'static str, Option<&'static str>)> {
        Some((Named::NAME, Named::CONTENT_TYPE))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_headers_a_record_carries_natively_are_not_the_envelopes() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert(SEQUENCE_HEADER, "1");
        headers.insert(SHARD_HEADER, "shardId-0");
        assert!(EnvelopeHeaders::new(&headers).is_empty());

        headers.insert("x-tenant", "acme");
        let written: Vec<_> = EnvelopeHeaders::new(&headers).iter().collect();
        assert_eq!(written, vec![("x-tenant", &b"acme"[..])]);
    }

    #[test]
    fn a_range_that_does_not_fit_the_record_reads_as_the_whole_record() {
        let record = b"payload";
        for range in [0..8, Range { start: 5, end: 3 }] {
            let body = Body::Within(range).fit(record.len());
            assert_eq!(body, Body::Whole);
            assert_eq!(body.payload(record), record);
        }
        assert_eq!(Body::Within(3..7).fit(7).payload(record), b"load");
    }
}
