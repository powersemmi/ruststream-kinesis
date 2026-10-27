//! [`NoOp`]: no envelope; the record's data is the payload.

use ruststream::{BytesMut, HeaderMap};

use super::{Body, Envelope, EnvelopeError, EnvelopeHeaders, MalformedHeaders};

/// No envelope: the record's data is the payload, byte for byte, for a consumer that reads raw
/// records (a Firehose delivery stream, a Lambda, a service in another language).
///
/// The partition key still rides the record's own field, and a delivery still carries the
/// partition key, the sequence number and the shard as headers. It carries no other header. A
/// publish that carries one fails with
/// [`EnvelopeError::UnsupportedHeader`](super::EnvelopeError::UnsupportedHeader), naming the
/// header and the stream: the record would otherwise lose it without a word. That includes the
/// framework's attempt counter, so a registration that counts its attempts (`max_attempts(..)`)
/// does not publish its copies through this envelope.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_kinesis::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Click {
///     page: String,
/// }
///
/// /// Reads the clickstream a browser SDK writes: plain JSON records, nothing in front of them.
/// #[subscriber(KinesisStream::new("clicks"))]
/// async fn on_click(click: &Click) -> HandlerOutcome {
///     tracing::info!(click.page, "click");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("clicks", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
///         b.include(on_click.envelope(NoOp));
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct NoOp;

impl Envelope for NoOp {
    const NAME: &'static str = "none";

    fn seal(
        &self,
        headers: EnvelopeHeaders<'_>,
        payload: BytesMut,
    ) -> Result<Vec<u8>, EnvelopeError> {
        if let Some((name, _)) = headers.iter().next() {
            return Err(EnvelopeError::unsupported_header(name));
        }
        // The buffer the framework wrote is the record as it stands.
        Ok(Vec::from(payload))
    }

    fn body(&self, _record: &[u8]) -> Body {
        Body::Whole
    }

    fn read_headers(
        &self,
        _record: &[u8],
        _body: &Body,
        _headers: &mut HeaderMap,
    ) -> Result<(), MalformedHeaders> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::PARTITION_KEY_HEADER;

    #[test]
    fn a_publish_with_a_header_is_refused_naming_the_header() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("x-tenant", "acme");

        let refused = NoOp.seal(EnvelopeHeaders::new(&headers), BytesMut::from(&b"raw"[..]));

        assert!(
            matches!(&refused, Err(EnvelopeError::UnsupportedHeader { header }) if header == "x-tenant"),
            "{refused:?}"
        );
    }

    #[test]
    fn the_record_is_the_payload_whatever_it_starts_with() {
        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        let payload: &[u8] = b"RSK2\0\0\0\0frame";

        let record = NoOp
            .seal(EnvelopeHeaders::new(&headers), BytesMut::from(payload))
            .expect("the partition key travels natively");

        assert_eq!(record, payload);
        assert_eq!(NoOp.body(&record).payload(&record), payload);
        let mut read = HeaderMap::new();
        NoOp.read_headers(&record, &Body::Whole, &mut read)
            .expect("no envelope, nothing malformed");
        assert!(read.is_empty());
    }
}
