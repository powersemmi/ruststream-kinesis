//! What this crate writes into the generated `AsyncAPI` document.
//!
//! The specification lists no Kinesis binding and its protocol keys are a closed list, so both
//! objects travel as the `x-ruststream-kinesis` extension. Only what the descriptor and the
//! policy hold goes in: the document is built before anything connects, so an existing stream's
//! real shard count and its ARN have no place here.
#![cfg(feature = "asyncapi")]

use std::time::Duration;

use ruststream::asyncapi::{Bindings, Spec, build_spec};
use ruststream::codec::{CborCodec, JsonCodec};
use ruststream::conformance::harness;
use ruststream_kinesis::prelude::*;
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
    reply
)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// The channel object of a Kinesis subscription, exactly as the document reports it. The
/// documentation embeds the same file, so a change to the binding shows up in the page or fails
/// here.
const CHANNEL_BINDING: &str = include_str!("snippets/asyncapi-channel.json");

/// The message object of a record whose mount site fixed a partition key.
const MESSAGE_BINDING: &str = include_str!("snippets/asyncapi-message.json");

/// The channel object of the stream a reply lands in, which the policy learns from the
/// destination the mount site resolved.
const PUBLISH_CHANNEL_BINDING: &str = include_str!("snippets/asyncapi-publish-channel.json");

/// One mount, one document: the subscription describes its channel and the reply policy
/// describes the records that leave through it.
fn document() -> Spec {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
            b.include(confirm)
                .out_reply(Publish::default())
                .partition_key("tenant-acme");
        });
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
    assert_eq!(bindings, CHANNEL_BINDING.trim_end());
}

/// The key the mount site fixed on the policy travels with the message, which is where the
/// specification puts an ordering key on the brokers it does cover. A key named on one publish
/// call is not here: the document describes the mount, not a single record.
#[test]
fn the_message_reports_the_partition_key_the_mount_site_fixed() {
    let spec = document();

    let bindings = serde_json::to_string_pretty(&spec.components.messages["Receipt"].bindings)
        .expect("a binding body serialized once at construction serializes again here");
    assert_eq!(bindings, MESSAGE_BINDING.trim_end());
}

/// A Kinesis publish names the stream its records land in, and the policy holds no destination:
/// the name is the reply type's own `#[outgoing(name = "receipts")]`, which the runtime hands to
/// the policy as the channel it describes.
#[test]
fn the_reply_channel_reports_the_stream_the_records_land_in() {
    let spec = document();

    let bindings = serde_json::to_string_pretty(&spec.channels["receipts"].bindings)
        .expect("a binding body serialized once at construction serializes again here");
    assert_eq!(bindings, PUBLISH_CHANNEL_BINDING.trim_end());
}

/// The destination reaches the channel object and stops there. A partition key is the record's
/// own routing field, so the message object carries the key the mount site fixed and no stream
/// name: one reply type published to two streams would otherwise report one of them on both.
#[test]
fn the_message_object_names_no_stream() {
    let spec = document();

    let bindings = serde_json::to_string(&spec.components.messages["Receipt"].bindings)
        .expect("a binding body serialized once at construction serializes again here");
    assert!(
        !bindings.contains("receipts"),
        "the message object named a stream: {bindings}"
    );
}

/// A policy that fixes no key says nothing rather than writing an empty object, so a document
/// carries no field the service never set.
#[test]
fn a_policy_without_a_key_adds_no_message_binding() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
            b.include(confirm).out_reply(Publish::default());
        });
    let spec = build_spec(&app);

    assert_eq!(
        spec.components.messages["Receipt"].bindings,
        Bindings::new()
    );
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

/// The document of the same mount, on a broker framing its records in JSON.
fn framed_document() -> serde_json::Value {
    let broker = KinesisBroker::new().envelope(Framed::new(JsonCodec));
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_labeled(
        "kinesis",
        broker,
        |b| {
            b.include(confirm).out_reply(Publish::default());
        },
    );
    serde_json::to_value(build_spec(&app)).expect("the document serializes")
}

/// The broker's envelope is where every record without an envelope of its own travels, so the
/// server reports it, and the media type of the document it frames the payload in.
#[test]
fn the_server_reports_the_brokers_envelope_and_its_media_type() {
    let document = framed_document();

    assert_eq!(
        document["servers"]["kinesis"]["bindings"]["x-ruststream-kinesis"]["envelope"],
        serde_json::json!({"name": "framed", "contentType": "application/json"})
    );
}

/// A subscription or a publish policy that names an envelope of its own reports it beside its
/// other settings; one that names none leaves it to the server.
#[test]
fn a_mount_site_reports_the_envelope_it_names() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(KinesisBroker::new(), |b| {
            b.include(confirm.envelope(Framed::new(CborCodec)))
                .out_reply(Publish::default().envelope(NoOp));
        });
    let document = serde_json::to_value(build_spec(&app)).expect("the document serializes");

    assert_eq!(
        document["channels"]["orders"]["bindings"]["x-ruststream-kinesis"]["envelope"],
        serde_json::json!({"name": "framed", "contentType": "application/cbor"})
    );
    assert_eq!(
        document["components"]["messages"]["Receipt"]["bindings"]["x-ruststream-kinesis"],
        serde_json::json!({"envelope": {"name": "none"}})
    );
    assert!(
        framed_document()["channels"]["orders"]["bindings"]["x-ruststream-kinesis"]
            .get("envelope")
            .is_none()
    );
}
