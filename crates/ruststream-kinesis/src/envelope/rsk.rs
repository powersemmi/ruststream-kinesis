//! [`Rsk`]: the crate's own binary envelope, written only when a record carries headers.

use bytes::Bytes;
use ruststream::{BytesMut, HeaderMap};

use super::{Body, Envelope, EnvelopeError, EnvelopeHeaders, MalformedHeaders};

/// The envelope magic: a record that starts with it carries a header block.
const MAGIC: [u8; 4] = *b"RSK2";

/// The magic of the envelope earlier releases wrote, one `name: value` text line per header.
/// Records keep their bytes for as long as the stream retains them, so it is still read.
const TEXT_MAGIC: [u8; 4] = *b"RSK1";

/// The magic and the header-block length in front of the header block.
const PREFIX: usize = 8;

/// The length prefix of a header name or value.
const FIELD_LEN: usize = 4;

/// The default envelope: the payload as it stands when the record carries no header, a binary
/// header block in front of it when it does.
///
/// The block is the magic `RSK2`, a big-endian `u32` length of the block, then each header as a
/// length-prefixed name and a length-prefixed value, so a value comes back byte for byte. Records
/// in the earlier `RSK1` text envelope still read. A headerless payload that begins with either
/// magic is written with an empty block, so it reads back whole; a record without the magic, or
/// whose block does not parse, is read as payload alone.
///
/// Every release of this crate reads it, so services that run different releases keep reading
/// each other.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_kinesis::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// /// Reads the stream a service on an earlier release writes, whatever the broker's default.
/// #[subscriber(KinesisStream::new("orders"))]
/// async fn on_order(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
///     tracing::info!(order.id, tenant = ?ctx.headers().get_str("x-tenant"), "order");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(KinesisBroker::new().envelope(NoOp), |b| {
///             b.include(on_order.envelope(Rsk));
///         })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Rsk;

impl Envelope for Rsk {
    const NAME: &'static str = "rsk";

    fn seal(
        &self,
        headers: EnvelopeHeaders<'_>,
        payload: BytesMut,
    ) -> Result<Vec<u8>, EnvelopeError> {
        let block: usize = headers
            .iter()
            .map(|(name, value)| 2 * FIELD_LEN + name.len() + value.len())
            .sum();
        if block == 0 && !looks_enveloped(&payload) {
            // `Vec::from` reclaims the buffer the framework wrote: a record with nothing in front
            // of its payload is that buffer.
            return Ok(Vec::from(payload));
        }
        // A headerless payload that begins with a magic gets an envelope with an empty header
        // block, so the reader takes the magic for payload rather than cutting at a guessed
        // boundary.
        let mut out = Vec::with_capacity(PREFIX + block + payload.len());
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&field_len(block));
        for (name, value) in headers.iter() {
            out.extend_from_slice(&field_len(name.len()));
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(&field_len(value.len()));
            out.extend_from_slice(value);
        }
        out.extend_from_slice(&payload);
        Ok(out)
    }

    fn body(&self, record: &[u8]) -> Body {
        split(record).map_or(Body::Whole, |(_, start)| Body::Within(start..record.len()))
    }

    fn read_headers(
        &self,
        record: &[u8],
        _body: &Body,
        headers: &mut HeaderMap,
    ) -> Result<(), MalformedHeaders> {
        // `split` accepts a binary block only when it parses whole, so a record whose block does
        // not is no envelope at all and is read as payload.
        if let Some((block, _)) = split(record) {
            match block {
                Block::Fields(block) => decode_fields(block, headers),
                Block::Text(block) => decode_text(block, headers),
            }
        }
        Ok(())
    }
}

/// A header block, in the layout its magic names.
enum Block<'a> {
    Fields(&'a [u8]),
    Text(&'a [u8]),
}

/// The header block of an enveloped record and where its payload starts, or `None` for a record
/// that is not one. The binary block is walked to check it parses, without building anything,
/// so a blob that merely starts with the magic stays somebody else's payload.
fn split(record: &[u8]) -> Option<(Block<'_>, usize)> {
    let magic = record.first_chunk::<4>()?;
    let (len, rest) = read_len(&record[4..])?;
    let block = rest.get(..len)?;
    let start = PREFIX + len;
    if *magic == MAGIC {
        fields_parse(block).then_some((Block::Fields(block), start))
    } else if *magic == TEXT_MAGIC {
        Some((Block::Text(block), start))
    } else {
        None
    }
}

/// Whether a reader would take `payload` for an envelope: it begins with either magic.
fn looks_enveloped(payload: &[u8]) -> bool {
    payload
        .first_chunk::<4>()
        .is_some_and(|magic| *magic == MAGIC || *magic == TEXT_MAGIC)
}

/// A length as the envelope writes it. A record is at most 10 MiB, so every length fits; one
/// that does not writes zero, and the service refuses the record for its size anyway.
fn field_len(len: usize) -> [u8; FIELD_LEN] {
    u32::try_from(len).unwrap_or(0).to_be_bytes()
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

/// Reads one header, a UTF-8 name and a value, off the front of `block`.
fn read_header(block: &[u8]) -> Option<(&str, &[u8], &[u8])> {
    let (name, rest) = read_field(block)?;
    let (value, rest) = read_field(rest)?;
    Some((str::from_utf8(name).ok()?, value, rest))
}

/// Whether a binary header block parses whole.
fn fields_parse(mut block: &[u8]) -> bool {
    while !block.is_empty() {
        let Some((_, _, rest)) = read_header(block) else {
            return false;
        };
        block = rest;
    }
    true
}

/// The headers of a binary block that [`fields_parse`] accepted.
fn decode_fields(mut block: &[u8], headers: &mut HeaderMap) {
    while let Some((name, value, rest)) = read_header(block) {
        headers.insert(name.to_owned(), Bytes::copy_from_slice(value));
        block = rest;
    }
}

/// The header block of the text envelope earlier releases wrote.
fn decode_text(block: &[u8], headers: &mut HeaderMap) {
    for line in String::from_utf8_lossy(block).lines() {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_owned(), value.trim().to_owned());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::PARTITION_KEY_HEADER;

    fn seal(headers: &HeaderMap, payload: &[u8]) -> Vec<u8> {
        Rsk.seal(EnvelopeHeaders::new(headers), BytesMut::from(payload))
            .expect("the envelope writes every header")
    }

    fn open(record: &[u8]) -> (HeaderMap, Vec<u8>) {
        let mut headers = HeaderMap::new();
        let body = Rsk.body(record).fit(record.len());
        Rsk.read_headers(record, &body, &mut headers)
            .expect("a block that parses reads");
        (headers, body.payload(record).to_vec())
    }

    /// Content equality cannot tell a hand-over from a copy, so the buffer the framework wrote is
    /// identified by its address.
    #[test]
    fn a_record_without_an_envelope_keeps_the_buffer_the_framework_wrote() {
        let payload = BytesMut::from(&br#"{"id":1}"#[..]);
        let written = payload.as_ptr();

        let data = Rsk
            .seal(EnvelopeHeaders::new(&HeaderMap::new()), payload)
            .expect("a headerless record seals");

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
        assert_eq!(seal(&headers, b"raw"), b"raw");

        headers.insert("x-tenant", "acme");
        let enveloped = seal(&headers, b"raw");
        assert_eq!(enveloped[0..4], MAGIC);
        let (decoded, payload) = open(&enveloped);
        assert_eq!(decoded.get_str("x-tenant"), Some("acme"));
        assert!(decoded.get(PARTITION_KEY_HEADER).is_none());
        assert_eq!(payload, b"raw");
    }

    /// A headerless payload that begins like an envelope, this crate's or the text one earlier
    /// releases wrote, would be cut at a guessed boundary on read, so it is enveloped itself and
    /// reads back whole.
    #[test]
    fn a_headerless_payload_that_looks_like_an_envelope_reads_back_unchanged() {
        let lookalikes: [&[u8]; 4] = [
            b"RSK2\0\0\0\0frame",
            b"RSK1\0\0\0\x05a: b\nframe",
            b"RSK2",
            b"RSK1",
        ];
        for payload in lookalikes {
            let data = seal(&HeaderMap::new(), payload);
            let (headers, read) = open(&data);
            assert_eq!(headers, HeaderMap::new(), "for {payload:?}");
            assert_eq!(read, payload, "for {payload:?}");
        }
    }

    /// The escape is an envelope with an empty header block, which every release that reads
    /// `RSK2` already reads as the payload alone.
    #[test]
    fn the_escape_is_an_envelope_with_no_headers() {
        assert_eq!(seal(&HeaderMap::new(), b"RSK1x"), b"RSK2\0\0\0\0RSK1x");
    }

    /// Only the two magics are escaped: a payload that merely shares their first letters is the
    /// record's data as it stands.
    #[test]
    fn a_payload_with_another_prefix_stays_plain() {
        for payload in [&b"RSK3\0\0\0\0frame"[..], b"RSK", b"RS"] {
            assert_eq!(seal(&HeaderMap::new(), payload), payload);
        }
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

        let (decoded, payload) = open(&seal(&headers, b"raw"));

        assert_eq!(decoded.get("x-binary"), Some(binary));
        assert_eq!(decoded.get_str("x-text"), Some(" a: b\nc "));
        assert_eq!(decoded.get("x-empty"), Some(&b""[..]));
        assert_eq!(decoded.iter().count(), 3);
        assert_eq!(payload, b"raw");
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

        let (decoded, payload) = open(&data);

        assert_eq!(decoded.get_str("x-tenant"), Some("acme"));
        assert_eq!(payload, b"raw");
    }

    /// A blob that starts like an envelope and does not parse as one is somebody else's payload,
    /// handed over whole rather than cut at a guessed boundary.
    #[test]
    fn a_blob_that_does_not_parse_as_an_envelope_reads_as_headerless() {
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        let enveloped = seal(&headers, b"");
        for cut in [6, enveloped.len() - 1] {
            let (decoded, payload) = open(&enveloped[..cut]);
            assert_eq!(decoded, HeaderMap::new(), "cut at {cut}");
            assert_eq!(payload, &enveloped[..cut]);
        }
        let mut not_utf8_name = enveloped;
        not_utf8_name[12] = 0xff;
        let (decoded, payload) = open(&not_utf8_name);
        assert_eq!(decoded, HeaderMap::new());
        assert_eq!(payload, not_utf8_name);
    }

    #[test]
    fn plain_payloads_read_as_headerless() {
        let (headers, payload) = open(b"{\"id\":1}");
        assert_eq!(headers, HeaderMap::new());
        assert_eq!(payload, b"{\"id\":1}");
    }
}
