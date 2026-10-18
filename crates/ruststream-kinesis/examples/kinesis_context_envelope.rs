//! The header envelope: a header with arbitrary bytes travels in the record's envelope and comes
//! back byte for byte, and a record that carries no headers reads as headerless.
//!
//! ```text
//! cargo test --example kinesis_context_envelope --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_kinesis::prelude::*;

/// The slot the verified payments leave through.
#[derive(OutSlot)]
#[publishes(Verified)]
struct Ledger;

// --8<-- [start:envelope]
/// Accepts a payment that carries its signature, the raw bytes of the producer's MAC, in the
/// `x-signature` header; an unsigned one is dropped.
#[subscriber(KinesisStream::new("payments"))]
async fn verify(
    payment: &Payment,
    ctx: &mut Context<'_>,
    Out(ledger): Out<impl Publisher, Ledger>,
) -> HandlerOutcome {
    let Some(signature) = ctx.headers().get("x-signature") else {
        tracing::warn!(payment.id, "unsigned payment dropped");
        return HandlerOutcome::drop();
    };
    let verified = Verified {
        payment: payment.id,
        signature: signature.to_vec(),
    };
    if ledger.message(&verified).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:envelope]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
        b.include(verify).out(Ledger, Publish::default()).build();
    })
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "ledger")]
struct Verified {
    payment: u64,
    signature: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_kinesis::prelude::*;

    use super::{Payment, Verified, app};

    /// Bytes no text header could carry: a zero, a lone continuation byte, a newline.
    const SIGNATURE: &[u8] = &[0x00, 0x9f, 0xff, b'\n', 0x42];

    #[tokio::test]
    async fn the_signature_arrives_byte_for_byte() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let mut signed = HeaderMap::new();
        signed.insert("x-signature", SIGNATURE);
        tb.broker::<KinesisBroker>()
            .message(&Payment { id: 7 })
            .with_headers(signed)
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .published::<Verified>("ledger")
            .assert_called_once()
            .with(&Verified {
                payment: 7,
                signature: SIGNATURE.to_vec(),
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_unsigned_payment_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<KinesisBroker>()
            .message(&Payment { id: 8 })
            .publish()
            .await?;

        tb.broker::<KinesisBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<KinesisBroker>()
            .published::<Verified>("ledger")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
