//! [`Framed`]: headers and payload as one document of a `RustStream` codec.

use std::fmt;

use ruststream::codec::Codec;
use ruststream::{BytesMut, HeaderMap};
use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::ByteBuf;

use super::{Body, Envelope, EnvelopeError, EnvelopeHeaders};

/// Headers and payload as one document of a `RustStream` codec, which a consumer in any language
/// reads with its own JSON, CBOR or msgpack library.
///
/// Every record is framed, with or without headers. The document is a map of exactly two
/// entries: `headers`, a map of header names to text values, and `payload`, the payload's bytes
/// (a byte string in CBOR and msgpack, an array of numbers in JSON). A record of another shape
/// is somebody else's payload and is delivered whole, with no headers. A header value that is not
/// UTF-8 fails the publish with
/// [`EnvelopeError::HeaderNotText`](super::EnvelopeError::HeaderNotText).
///
/// The payload is not a slice of the record, so a delivery decodes it into a buffer of its own:
/// one allocation per record, which the binary [`Rsk`](super::Rsk) layout does not need. The
/// headers are decoded on the first read of them.
///
/// # Examples
///
/// ```
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

impl<FrameCodec: Codec> Framed<FrameCodec> {
    /// Frames records in `codec`'s documents.
    ///
    /// # Examples
    ///
    /// ```
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
    pub const fn new(codec: FrameCodec) -> Self {
        Self { codec }
    }
}

impl<FrameCodec> Envelope for Framed<FrameCodec>
where
    FrameCodec: Codec + Clone + 'static,
{
    const NAME: &'static str = "framed";
    const CONTENT_TYPE: Option<&'static str> = Some(FrameCodec::CONTENT_TYPE);

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
        let frame = FrameOut {
            headers: TextHeaders(headers),
            payload: serde_bytes::Bytes::new(&payload),
        };
        self.codec
            .encode(&frame)
            .map(Vec::from)
            .map_err(EnvelopeError::Codec)
    }

    fn body(&self, record: &[u8]) -> Body {
        // The payload entry alone: the headers are skipped over, not built.
        self.codec
            .decode::<FramePayload>(record)
            .map_or(Body::Whole, |frame| Body::Decoded(frame.payload.into_vec()))
    }

    fn read_headers(&self, record: &[u8], headers: &mut HeaderMap) {
        if let Ok(frame) = self.codec.decode::<FrameHeaders>(record) {
            for (name, value) in frame.headers.0 {
                headers.insert(name, value);
            }
        }
    }
}

/// The document a publish writes.
#[derive(Serialize)]
struct FrameOut<'a> {
    headers: TextHeaders<'a>,
    payload: &'a serde_bytes::Bytes,
}

/// The headers of a publish as a map of names to text, every value already checked to be UTF-8.
struct TextHeaders<'a>(EnvelopeHeaders<'a>);

impl Serialize for TextHeaders<'_> {
    fn serialize<Out: Serializer>(&self, serializer: Out) -> Result<Out::Ok, Out::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (name, value) in self.0.iter() {
            // `seal` refused every value that is not UTF-8 before encoding began.
            map.serialize_entry(name, str::from_utf8(value).unwrap_or_default())?;
        }
        map.end()
    }
}

/// A document read for its payload. Exactly the two entries make a frame, so a document of
/// another shape fails here and the record is delivered whole.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FramePayload {
    #[serde(rename = "headers")]
    _headers: IgnoredAny,
    payload: ByteBuf,
}

/// A document read for its headers, the payload skipped over.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FrameHeaders {
    headers: ReadHeaders,
    #[serde(rename = "payload")]
    _payload: IgnoredAny,
}

/// The header map of a frame, in the order the document lists it.
struct ReadHeaders(Vec<(String, String)>);

impl<'de> Deserialize<'de> for ReadHeaders {
    fn deserialize<In: Deserializer<'de>>(deserializer: In) -> Result<Self, In::Error> {
        deserializer.deserialize_map(ReadHeadersVisitor)
    }
}

struct ReadHeadersVisitor;

impl<'de> Visitor<'de> for ReadHeadersVisitor {
    type Value = ReadHeaders;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a map of header names to text")
    }

    fn visit_map<Entries: MapAccess<'de>>(
        self,
        mut map: Entries,
    ) -> Result<ReadHeaders, Entries::Error> {
        let mut headers = Vec::with_capacity(map.size_hint().unwrap_or(0));
        while let Some(entry) = map.next_entry::<String, String>()? {
            headers.push(entry);
        }
        Ok(ReadHeaders(headers))
    }
}

#[cfg(test)]
mod tests {
    use ruststream::codec::{CborCodec, JsonCodec};

    use super::*;
    use crate::message::PARTITION_KEY_HEADER;

    fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("x-tenant", "acme");
        headers.insert("x-empty", "");
        headers
    }

    fn round_trip<FrameCodec: Codec + Clone + 'static>(codec: FrameCodec) {
        let framed = Framed::new(codec);
        let payload: &[u8] = &[0x00, 0xff, b'{', b'"'];
        let record = framed
            .seal(EnvelopeHeaders::new(&headers()), BytesMut::from(payload))
            .expect("text headers frame");

        let body = framed.body(&record).fit(record.len());
        let mut read = HeaderMap::new();
        framed.read_headers(&record, &mut read);

        assert_eq!(body.payload(&record), payload);
        assert_eq!(read.get_str("x-tenant"), Some("acme"));
        assert_eq!(read.get_str("x-empty"), Some(""));
        assert_eq!(read.get(PARTITION_KEY_HEADER), None);
        assert_eq!(read.len(), 2);
    }

    #[test]
    fn a_record_round_trips_through_a_json_document() {
        round_trip(JsonCodec);
    }

    #[test]
    fn a_record_round_trips_through_a_cbor_document() {
        round_trip(CborCodec);
    }

    #[test]
    fn the_json_document_reads_without_this_crate() {
        let record = Framed::new(JsonCodec)
            .seal(EnvelopeHeaders::new(&headers()), BytesMut::from(&b"hi"[..]))
            .expect("text headers frame");

        let document: serde_json::Value =
            serde_json::from_slice(&record).expect("the record is a JSON document");

        assert_eq!(
            document,
            serde_json::json!({
                "headers": {"x-tenant": "acme", "x-empty": ""},
                "payload": [104, 105],
            })
        );
    }

    #[test]
    fn a_headerless_record_is_framed_too() {
        let record = Framed::new(JsonCodec)
            .seal(
                EnvelopeHeaders::new(&HeaderMap::new()),
                BytesMut::from(&b"hi"[..]),
            )
            .expect("a headerless record frames");

        assert_eq!(record, br#"{"headers":{},"payload":[104,105]}"#);
    }

    /// A record of another shape - a plain document, a document with a third entry, bytes that
    /// are no document at all - is somebody else's payload.
    #[test]
    fn a_record_of_another_shape_is_the_payload_with_no_headers() {
        let framed = Framed::new(JsonCodec);
        for record in [
            &br#"{"id":1}"#[..],
            br#"{"headers":{},"payload":[104],"extra":1}"#,
            b"\x00\x01raw",
        ] {
            let mut read = HeaderMap::new();
            framed.read_headers(record, &mut read);
            assert_eq!(framed.body(record), Body::Whole, "for {record:?}");
            assert!(read.is_empty(), "for {record:?}");
        }
    }

    #[test]
    fn a_header_value_that_is_not_text_is_refused_naming_the_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-binary", vec![0xff, 0xfe]);

        let refused =
            Framed::new(JsonCodec).seal(EnvelopeHeaders::new(&headers), BytesMut::from(&b"hi"[..]));

        assert!(
            matches!(&refused, Err(EnvelopeError::HeaderNotText { header }) if header == "x-binary"),
            "{refused:?}"
        );
    }
}
