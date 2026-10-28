//! The JSON document: the payload a JSON value in `payload` when it is a JSON text, a base64
//! string in `payload_base64` otherwise, as the `CloudEvents` JSON format carries data.

use base64::Engine;
use base64::display::Base64Display;
use base64::engine::general_purpose::STANDARD;
use ruststream::HeaderMap;
use ruststream::codec::{CodecError, JsonCodec};
use serde::de::IgnoredAny;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;
use std::borrow::Cow;

use super::FrameCodec;
use super::binary::{ReadHeaders, TextHeaders};
use crate::envelope::{Body, EnvelopeError, EnvelopeHeaders, MalformedHeaders};

impl FrameCodec for JsonCodec {
    fn frame(
        &self,
        headers: EnvelopeHeaders<'_>,
        payload: &[u8],
    ) -> Result<Vec<u8>, EnvelopeError> {
        // A JSON text is embedded only when the value spans the whole payload: whitespace around
        // it is not part of the value and would not come back.
        let embedded = serde_json::from_slice::<&RawValue>(payload)
            .ok()
            .filter(|value| value.get().len() == payload.len());
        let headers = TextHeaders(headers);
        let mut out = Vec::with_capacity(payload.len() + payload.len() / 3 + 64);
        let written = match embedded {
            Some(payload) => serde_json::to_writer(&mut out, &Embedded { headers, payload }),
            None => serde_json::to_writer(
                &mut out,
                &Encoded {
                    headers,
                    payload_base64: Base64(payload),
                },
            ),
        };
        written
            .map(|()| out)
            .map_err(|err| EnvelopeError::Codec(CodecError::Encode(Box::new(err))))
    }

    fn frame_body(&self, record: &[u8]) -> Body {
        let Ok(frame) = serde_json::from_slice::<Frame<'_>>(record) else {
            return Body::Whole;
        };
        match (frame.payload, frame.payload_base64) {
            (Some(value), None) => {
                // The value is a slice of the record, so the payload is read where it lies.
                let text = value.get();
                let start = text.as_ptr() as usize - record.as_ptr() as usize;
                Body::Within(start..start + text.len())
            }
            (None, Some(encoded)) => decode_base64(&encoded).map_or(Body::Whole, Body::Decoded),
            _ => Body::Whole,
        }
    }

    fn frame_headers(
        &self,
        record: &[u8],
        headers: &mut HeaderMap,
    ) -> Result<(), MalformedHeaders> {
        // The body pass found a frame and decoded its payload; this pass reads the header map and
        // skips the payload fields without decoding them.
        let frame = serde_json::from_slice::<FrameHeaders>(record).map_err(|_| MalformedHeaders)?;
        frame.headers.insert_into(headers);
        Ok(())
    }
}

/// The payload of a `payload_base64` field, or `None` when it is no base64.
fn decode_base64(encoded: &str) -> Option<Vec<u8>> {
    #[cfg(test)]
    tests::DECODES.with(|decodes| decodes.set(decodes.get() + 1));
    STANDARD.decode(encoded.as_bytes()).ok()
}

/// The document of a payload that is a JSON text.
#[derive(Serialize)]
struct Embedded<'a> {
    headers: TextHeaders<'a>,
    payload: &'a RawValue,
}

/// The document of any other payload.
#[derive(Serialize)]
struct Encoded<'a> {
    headers: TextHeaders<'a>,
    payload_base64: Base64<'a>,
}

/// Bytes as a standard base64 string, written straight into the document.
struct Base64<'a>(&'a [u8]);

impl Serialize for Base64<'_> {
    fn serialize<Out: Serializer>(&self, serializer: Out) -> Result<Out::Ok, Out::Error> {
        serializer.collect_str(&Base64Display::new(self.0, &STANDARD))
    }
}

/// A document read for its payload: the header map skipped over, and the two payload fields, of
/// which a frame has exactly one. Any other field makes it no frame.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Frame<'a> {
    #[serde(rename = "headers")]
    _headers: IgnoredAny,
    #[serde(default, borrow, deserialize_with = "present")]
    payload: Option<&'a RawValue>,
    #[serde(default, borrow)]
    payload_base64: Option<Cow<'a, str>>,
}

/// A frame read for its header map, the payload fields skipped over.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FrameHeaders {
    headers: ReadHeaders,
    #[serde(default, rename = "payload")]
    _payload: Option<IgnoredAny>,
    #[serde(default, rename = "payload_base64")]
    _payload_base64: Option<IgnoredAny>,
}

/// A `payload` field that is there, whatever it holds: `null` is a payload too.
fn present<'de, In: Deserializer<'de>>(
    deserializer: In,
) -> Result<Option<&'de RawValue>, In::Error> {
    <&RawValue>::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use ruststream::{BytesMut, HeaderMap};

    use ruststream::codec::JsonCodec;

    use crate::envelope::{Body, Envelope, EnvelopeHeaders, Framed, MalformedHeaders};
    use crate::message::PARTITION_KEY_HEADER;

    thread_local! {
        /// The base64 payloads decoded on this test's thread.
        pub(super) static DECODES: Cell<usize> = const { Cell::new(0) };
    }

    fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("event", "created");
        headers
    }

    fn seal(payload: &[u8]) -> Vec<u8> {
        Framed::new(JsonCodec)
            .seal(EnvelopeHeaders::new(&headers()), BytesMut::from(payload))
            .expect("text headers frame")
    }

    /// The payload and the headers as a delivery reads them.
    fn open(record: &[u8]) -> (Body, Vec<u8>, HeaderMap) {
        let framed = Framed::new(JsonCodec);
        let body = framed.body(record).fit(record.len());
        let payload = body.payload(record).to_vec();
        let mut read = HeaderMap::new();
        framed
            .read_headers(record, &body, &mut read)
            .expect("the header map parses");
        (body, payload, read)
    }

    #[test]
    fn a_json_payload_rides_as_a_json_value() {
        assert_eq!(
            seal(br#"{"id":7}"#),
            br#"{"headers":{"event":"created"},"payload":{"id":7}}"#
        );
    }

    /// The value is the payload's own text: its spacing and number spelling come back as they
    /// were, read in place rather than copied.
    #[test]
    fn a_json_payload_reads_back_byte_for_byte_in_place() {
        for payload in [
            &br#"{ "id" : 7,  "price": 1.50e0 }"#[..],
            b"null",
            b"\"text\"",
            b"[1, 2]",
        ] {
            let record = seal(payload);
            let (body, read, headers) = open(&record);
            assert_eq!(read, payload, "for {payload:?}");
            assert!(matches!(body, Body::Within(_)), "for {payload:?}: {body:?}");
            assert_eq!(headers.get_str("event"), Some("created"));
        }
    }

    #[test]
    fn a_payload_that_is_no_json_text_rides_as_base64() {
        let payload: &[u8] = &[0x08, 0x07];
        let record = seal(payload);
        assert_eq!(
            record,
            br#"{"headers":{"event":"created"},"payload_base64":"CAc="}"#
        );
        let (_, read, headers) = open(&record);
        assert_eq!(read, payload);
        assert_eq!(headers.get_str("event"), Some("created"));
    }

    /// Whitespace around a JSON text is not part of the value the document can carry, so such a
    /// payload rides as base64 and still comes back byte for byte.
    #[test]
    fn a_json_text_with_surrounding_whitespace_rides_as_base64() {
        let payload: &[u8] = b" 7\n";
        let record = seal(payload);
        assert!(
            record
                .windows(16)
                .any(|window| window == b"\"payload_base64\""),
            "{}",
            String::from_utf8_lossy(&record)
        );
        assert_eq!(open(&record).1, payload);
    }

    /// Only `headers` with exactly one of the two payload fields is a frame.
    #[test]
    fn a_document_of_another_shape_is_the_payload_with_no_headers() {
        for record in [
            &br#"{"headers":{},"payload":1,"payload_base64":"CAc="}"#[..],
            br#"{"headers":{},"payload":1,"extra":1}"#,
            br#"{"headers":{}}"#,
            br#"{"payload":1}"#,
            br#"{"headers":{},"payload_base64":"not base64!"}"#,
            br#"{"id":1}"#,
            b"\x00\x01raw",
        ] {
            let (body, read, headers) = open(record);
            assert_eq!(body, Body::Whole, "for {}", String::from_utf8_lossy(record));
            assert_eq!(read, record);
            assert!(
                headers.is_empty(),
                "for {}",
                String::from_utf8_lossy(record)
            );
        }
    }

    /// A delivery of a base64 payload decodes it once: the body pass decides the shape and
    /// decodes the payload, and reading the headers decodes nothing again.
    #[test]
    fn a_base64_payload_is_decoded_once_per_delivery() {
        let record = seal(&[0x08, 0x07]);
        DECODES.with(|decodes| decodes.set(0));

        let (_, payload, headers) = open(&record);

        assert_eq!(payload, [0x08, 0x07]);
        assert_eq!(headers.get_str("event"), Some("created"));
        assert_eq!(
            DECODES.with(Cell::get),
            1,
            "the payload was decoded more than once"
        );
    }

    /// A frame whose header map does not parse still delivers its payload; its headers are
    /// reported malformed rather than read as nothing without a word.
    #[test]
    fn a_frame_whose_header_map_does_not_parse_reports_it() {
        let record = br#"{"headers":{"event":7},"payload":{"id":7}}"#;
        let framed = Framed::new(JsonCodec);
        let body = framed.body(record).fit(record.len());
        let mut read = HeaderMap::new();

        let headers = framed.read_headers(record, &body, &mut read);

        assert_eq!(body.payload(record), br#"{"id":7}"#);
        assert_eq!(headers, Err(MalformedHeaders));
        assert!(read.is_empty());
    }
}
