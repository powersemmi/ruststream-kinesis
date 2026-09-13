//! What this crate writes into the generated `AsyncAPI` document.
//!
//! The specification lists no Kinesis binding and its protocol keys are a closed list, so both
//! objects travel as the `x-ruststream-kinesis` extension. Only what the descriptor and the
//! policy hold goes in: the document is built before anything connects, so an existing stream's
//! real shard count and its ARN have no place here.
#![cfg(all(feature = "asyncapi", feature = "testing"))]

use std::time::Duration;

use ruststream::asyncapi::{Spec, build_spec};
use ruststream::conformance::harness;
use ruststream_kinesis::prelude::*;
use ruststream_kinesis::testing::KinesisTestBroker;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JsonSchema)]
struct Order {
    id: u64,
}

/// The reply names its own stream, so the mount site is left to say how it is published.
#[derive(Outgoing, Serialize, JsonSchema)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

#[subscriber(
    KinesisStream::new("orders")
        .poll_interval(Duration::from_millis(250))
        .create_if_missing(2),
    publish
)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// The channel object of a Kinesis subscription, exactly as the document reports it.
// --8<-- [start:channel]
const CHANNEL_BINDING: &str = r#"{
  "x-ruststream-kinesis": {
    "pollIntervalMs": 250,
    "shardCount": 2,
    "stream": "orders"
  }
}"#;
// --8<-- [end:channel]

/// The message object of a record whose mount site fixed a partition key.
// --8<-- [start:message]
const MESSAGE_BINDING: &str = r#"{
  "x-ruststream-kinesis": {
    "partitionKey": "tenant-acme"
  }
}"#;
// --8<-- [end:message]

/// One mount, one document: the subscription describes its channel and the reply policy
/// describes the records that leave through it.
fn document() -> Spec {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KinesisTestBroker::new(),
        |b| {
            b.include(confirm)
                .out_reply(Publish::default())
                .partition_key("tenant-acme");
        },
    );
    build_spec(&app)
}

/// The stream the subscription reads, the pause between reads, and the shards the descriptor
/// would provision. A descriptor that creates no stream reports no shard count, because the
/// count of an existing stream is the service's answer and the document has no connection.
#[test]
fn the_channel_reports_what_the_descriptor_holds() {
    let spec = document();

    let bindings = serde_json::to_string_pretty(&spec.channels["orders"].bindings)
        .expect("a binding body serialized once at construction serializes again here");
    assert_eq!(bindings, CHANNEL_BINDING);
}

/// The key the mount site fixed on the policy travels with the message, which is where the
/// specification puts an ordering key on the brokers it does cover. A key named on one publish
/// call is not here: the document describes the mount, not a single record.
#[test]
fn the_message_reports_the_partition_key_the_mount_site_fixed() {
    let spec = document();

    let bindings = serde_json::to_string_pretty(&spec.components.messages["Receipt"].bindings)
        .expect("a binding body serialized once at construction serializes again here");
    assert_eq!(bindings, MESSAGE_BINDING);
}

/// A policy that fixes no key says nothing rather than writing an empty object, so a document
/// carries no field the service never set.
#[test]
fn a_policy_without_a_key_adds_no_message_binding() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        KinesisTestBroker::new(),
        |b| {
            b.include(confirm).out_reply(Publish::default());
        },
    );
    let spec = build_spec(&app);

    assert!(spec.components.messages["Receipt"].bindings.is_empty());
}

/// The document is generated to be published, so nothing the broker or the descriptor
/// contributes may carry a credential. An endpoint URL is the easy way to leak one.
#[test]
fn the_description_carries_no_credential() {
    harness::describes_without_credentials(
        &KinesisBroker::new().endpoint("https://key:hunter2@kinesis.eu-west-1.amazonaws.com:443"),
        &KinesisStream::new("orders").create_if_missing(2),
        "hunter2",
    );
}
