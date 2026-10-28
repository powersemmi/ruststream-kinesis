//! [`Framed`]: headers and payload as one document of a `RustStream` codec.

mod binary;
#[cfg(feature = "json")]
mod json;

#[cfg(feature = "cbor")]
use ruststream::codec::CborCodec;
use ruststream::codec::Codec;
#[cfg(feature = "msgpack")]
use ruststream::codec::MsgpackCodec;
use ruststream::{BytesMut, HeaderMap};

use super::{Body, Envelope, EnvelopeError, EnvelopeHeaders, MalformedHeaders};

/// Headers and payload as one document of a `RustStream` codec, which a consumer in any language
/// reads with its own JSON, CBOR or msgpack library.
///
/// Every record is framed, with or without headers. The document holds `headers`, a map of
/// header names to text values, and the payload in one of two fields, following the convention
/// of the `CloudEvents` JSON format:
///
/// - in JSON, a payload that is a JSON text rides `payload` as that JSON value, byte for byte:
///   `{"headers":{"event":"created"},"payload":{"id":7}}`; any other payload rides
///   `payload_base64` as a standard base64 string: `{"headers":{"event":"created"},"payload_base64":"CAc="}`;
/// - in CBOR and msgpack, the payload is always a byte string in `payload`.
///
/// A document is a frame only in that exact shape: `headers` and exactly one of the two payload
/// fields, nothing else. A record of another shape is somebody else's payload and is delivered
/// whole, with no headers. A header value that is not UTF-8 fails the publish with
/// [`EnvelopeError::HeaderNotText`](super::EnvelopeError::HeaderNotText).
///
/// A JSON payload embedded as a value is read in place, without a copy. A base64 payload, and the
/// byte string of CBOR and msgpack, is decoded into a buffer of the delivery's own: one
/// allocation per record. The headers are decoded on the first read of them.
///
/// The codec is one that implements [`FrameCodec`]: the framework's JSON, CBOR and msgpack codecs
/// with this crate's feature of the same name, or a codec of the service.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "cbor")]
/// # mod demo {
/// use ruststream::codec::CborCodec;
/// use ruststream_kinesis::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(KinesisStream::new("orders"))]
/// async fn on_order(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
///     tracing::info!(order.id, tenant = ?ctx.headers().get_str("x-tenant"), "order");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     // A consumer in another language reads every record as one CBOR document.
///     let broker = KinesisBroker::new().envelope(Framed::new(CborCodec));
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
///         b.include(on_order);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Framed<FrameCodec> {
    codec: FrameCodec,
}

impl<Format: FrameCodec> Framed<Format> {
    /// Frames records in `codec`'s documents.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::codec::JsonCodec;
    /// use ruststream_kinesis::prelude::*;
    ///
    /// #[derive(Debug, Clone, serde::Serialize, Outgoing)]
    /// #[outgoing(name = "receipts")]
    /// struct Receipt {
    ///     order: u64,
    /// }
    ///
    /// #[derive(serde::Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// /// Answers in a JSON document a consumer in another language reads as it stands.
    /// #[subscriber(KinesisStream::new("orders"), reply)]
    /// async fn confirm(order: &Order) -> Receipt {
    ///     Receipt { order: order.id }
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
    ///         b.include(confirm)
    ///             .out_reply(Publish::default().envelope(Framed::new(JsonCodec)));
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn new(codec: Format) -> Self {
        Self { codec }
    }
}

/// A codec [`Framed`] writes its documents in: where the payload goes in one and how it is found
/// again.
///
/// The framework's JSON, CBOR and msgpack codecs implement it under this crate's features of
/// the same name. The provided methods write the payload as a byte string in `payload`, the form
/// of a binary codec, so a codec of the service implements the trait with an empty body. JSON
/// overrides them with the `payload` / `payload_base64` pair of the `CloudEvents` JSON format.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "cbor")]
/// # mod demo {
/// use ruststream::BytesMut;
/// use ruststream::codec::{CborCodec, Codec, CodecError};
/// use ruststream_kinesis::envelope::FrameCodec;
/// use ruststream_kinesis::prelude::*;
/// use serde::Serialize;
/// use serde::de::DeserializeOwned;
///
/// /// CBOR under the media type an analytics team registered for its documents.
/// #[derive(Debug, Clone, Copy, Default)]
/// pub struct AnalyticsCbor;
///
/// impl Codec for AnalyticsCbor {
///     const CONTENT_TYPE: &'static str = "application/vnd.acme.analytics+cbor";
///
///     fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
///         CborCodec.encode(value)
///     }
///
///     fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
///         CborCodec.decode(bytes)
///     }
/// }
///
/// impl FrameCodec for AnalyticsCbor {}
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let broker = KinesisBroker::new().envelope(Framed::new(AnalyticsCbor));
///     RustStream::new(AppInfo::new("analytics", "0.1.0")).with_broker(broker, |_| {})
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a codec `Framed` writes documents in",
    note = "enable this crate's `json`, `cbor` or `msgpack` feature for the framework's codec, or \
            implement `ruststream_kinesis::envelope::FrameCodec` for a codec of the service"
)]
pub trait FrameCodec: Codec + Clone + 'static {
    /// Writes the document of a record. Every header value is UTF-8 by now.
    ///
    /// # Errors
    ///
    /// [`EnvelopeError::Codec`] when the codec cannot encode the document.
    fn frame(
        &self,
        headers: EnvelopeHeaders<'_>,
        payload: &[u8],
    ) -> Result<Vec<u8>, EnvelopeError> {
        binary::frame(self, headers, payload)
    }

    /// Where the payload of a document sits, or [`Body::Whole`] for a record that is no frame.
    fn frame_body(&self, record: &[u8]) -> Body {
        binary::body(self, record)
    }

    /// Adds the headers of a document to `headers`. Called only for a record
    /// [`frame_body`](Self::frame_body) found to be a frame, so it reads the header map and
    /// skips the payload.
    ///
    /// # Errors
    ///
    /// [`MalformedHeaders`] when the document's header map does not parse.
    fn frame_headers(
        &self,
        record: &[u8],
        headers: &mut HeaderMap,
    ) -> Result<(), MalformedHeaders> {
        binary::headers(self, record, headers)
    }
}

#[cfg(feature = "cbor")]
impl FrameCodec for CborCodec {}

#[cfg(feature = "msgpack")]
impl FrameCodec for MsgpackCodec {}

impl<Format: FrameCodec> Envelope for Framed<Format> {
    const NAME: &'static str = "framed";
    const CONTENT_TYPE: Option<&'static str> = Some(Format::CONTENT_TYPE);

    fn seal(
        &self,
        headers: EnvelopeHeaders<'_>,
        payload: BytesMut,
    ) -> Result<Vec<u8>, EnvelopeError> {
        // Checked before encoding: a serializer that met the value half way through the
        // document could not say which header it was.
        if let Some((name, _)) = headers
            .iter()
            .find(|(_, value)| str::from_utf8(value).is_err())
        {
            return Err(EnvelopeError::HeaderNotText {
                header: name.to_owned(),
            });
        }
        self.codec.frame(headers, &payload)
    }

    fn body(&self, record: &[u8]) -> Body {
        self.codec.frame_body(record)
    }

    fn read_headers(
        &self,
        record: &[u8],
        body: &Body,
        headers: &mut HeaderMap,
    ) -> Result<(), MalformedHeaders> {
        // The body pass decided whether the record is a frame; a record that is none has no
        // headers to read.
        if *body == Body::Whole {
            return Ok(());
        }
        self.codec.frame_headers(record, headers)
    }
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use ruststream::codec::CborCodec;

    use super::*;
    use crate::message::PARTITION_KEY_HEADER;

    fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("event", "created");
        headers
    }

    fn seal(payload: &[u8]) -> Vec<u8> {
        Framed::new(CborCodec)
            .seal(EnvelopeHeaders::new(&headers()), BytesMut::from(payload))
            .expect("text headers frame")
    }

    /// A CBOR document carries the payload as a byte string (major type 2), whatever it holds,
    /// and reads it back with the headers.
    #[test]
    fn a_cbor_document_carries_the_payload_as_a_byte_string() {
        let payload: &[u8] = br#"{"id":7}"#;
        let record = seal(payload);

        let mut byte_string = vec![0x40 | 8];
        byte_string.extend_from_slice(payload);
        assert!(
            record
                .windows(byte_string.len())
                .any(|window| window == byte_string),
            "{record:x?}"
        );

        let framed = Framed::new(CborCodec);
        let mut read = HeaderMap::new();
        let body = framed.body(&record).fit(record.len());
        framed
            .read_headers(&record, &body, &mut read)
            .expect("the header map parses");
        assert_eq!(body.payload(&record), payload);
        assert_eq!(read.get_str("event"), Some("created"));
        assert_eq!(read.len(), 1);
    }

    /// A document with a field beside `headers` and `payload` is no frame.
    #[test]
    fn a_cbor_document_of_another_shape_is_the_payload_with_no_headers() {
        #[derive(serde::Serialize)]
        struct Extra<'a> {
            headers: (),
            payload: &'a serde_bytes::Bytes,
            payload_base64: &'a str,
        }
        let codec = CborCodec;
        let both = Vec::from(
            codec
                .encode(&Extra {
                    headers: (),
                    payload: serde_bytes::Bytes::new(b"x"),
                    payload_base64: "eA==",
                })
                .expect("the document encodes"),
        );
        let framed = Framed::new(codec);
        for record in [both, b"\x00\x01raw".to_vec()] {
            let mut read = HeaderMap::new();
            let body = framed.body(&record);
            framed
                .read_headers(&record, &body, &mut read)
                .expect("no frame, nothing malformed");
            assert_eq!(body, Body::Whole, "for {record:x?}");
            assert!(read.is_empty(), "for {record:x?}");
        }
    }

    #[test]
    fn a_header_value_that_is_not_text_is_refused_naming_the_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-binary", vec![0xff, 0xfe]);

        let refused =
            Framed::new(CborCodec).seal(EnvelopeHeaders::new(&headers), BytesMut::from(&b"hi"[..]));

        assert!(
            matches!(&refused, Err(EnvelopeError::HeaderNotText { header }) if header == "x-binary"),
            "{refused:?}"
        );
    }
}
