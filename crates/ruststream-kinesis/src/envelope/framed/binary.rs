//! The document of a binary codec: the payload a byte string in `payload`, and the header map
//! every codec's document shares.

use std::fmt;

use ruststream::HeaderMap;
use ruststream::codec::Codec;
use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::ByteBuf;

use crate::envelope::{Body, EnvelopeError, EnvelopeHeaders, MalformedHeaders};

/// The document of a binary codec, the payload a byte string.
pub(super) fn frame<Format: Codec>(
    codec: &Format,
    headers: EnvelopeHeaders<'_>,
    payload: &[u8],
) -> Result<Vec<u8>, EnvelopeError> {
    let frame = FrameOut {
        headers: TextHeaders(headers),
        payload: serde_bytes::Bytes::new(payload),
    };
    codec
        .encode(&frame)
        .map(Vec::from)
        .map_err(EnvelopeError::Codec)
}

/// The payload of a binary document, the headers skipped over rather than built.
pub(super) fn body<Format: Codec>(codec: &Format, record: &[u8]) -> Body {
    codec
        .decode::<FramePayload>(record)
        .map_or(Body::Whole, |frame| Body::Decoded(frame.payload.into_vec()))
}

/// The headers of a binary document, the payload skipped over.
pub(super) fn headers<Format: Codec>(
    codec: &Format,
    record: &[u8],
    headers: &mut HeaderMap,
) -> Result<(), MalformedHeaders> {
    let frame = codec
        .decode::<FrameHeaders>(record)
        .map_err(|_| MalformedHeaders)?;
    frame.headers.insert_into(headers);
    Ok(())
}

/// The document a publish writes.
#[derive(Serialize)]
struct FrameOut<'a> {
    headers: TextHeaders<'a>,
    payload: &'a serde_bytes::Bytes,
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

/// The headers of a publish as a map of names to text, every value already checked to be UTF-8.
pub(super) struct TextHeaders<'a>(pub(super) EnvelopeHeaders<'a>);

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

/// The header map of a frame, in the order the document lists it.
pub(super) struct ReadHeaders(Vec<(String, String)>);

impl ReadHeaders {
    pub(super) fn insert_into(self, headers: &mut HeaderMap) {
        for (name, value) in self.0 {
            headers.insert(name, value);
        }
    }
}

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
        let mut headers = Vec::new();
        while let Some(entry) = map.next_entry::<String, String>()? {
            headers.push(entry);
        }
        Ok(ReadHeaders(headers))
    }
}
