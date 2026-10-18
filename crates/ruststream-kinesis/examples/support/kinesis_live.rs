//! What the live scenarios of the subscription, settlement and lease examples share: the stand's
//! address, the AWS configuration pointed at it, and the stand-side setup a service expects to
//! find (a stream, a lease table), done with the AWS clients the way an operator's tooling does.

// Each example uses the part of this module its scenarios need.
#![allow(dead_code)]

use std::process;
use std::time::Duration;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_kinesis::client::Waiters as _;
use aws_sdk_kinesis::primitives::Blob;
use ruststream_kinesis::KinesisBroker;

#[path = "../../tests/live/mod.rs"]
mod live;

/// The stand's endpoint, or `None` to skip the live scenario; under `RUSTSTREAM_REQUIRE_LIVE` a
/// missing endpoint fails instead.
pub(crate) fn endpoint() -> Option<String> {
    live::url("KINESIS_TEST_ENDPOINT")
}

/// The broker a service runs on, pointed at the stand.
pub(crate) fn broker(endpoint: &str) -> KinesisBroker {
    KinesisBroker::new()
        .endpoint(endpoint)
        .test_credentials()
        .region("us-east-1")
}

/// The AWS configuration of the stand, for the clients the setup uses and for a lease store.
pub(crate) async fn config(endpoint: &str) -> SdkConfig {
    aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .test_credentials()
        .load()
        .await
}

/// A name no earlier run used, for what a scenario creates and must find empty.
pub(crate) fn unique(name: &str) -> String {
    format!("{name}-{}", process::id())
}

/// Creates `stream` with `shards` shards unless the stand has it already, and waits until it is
/// active.
pub(crate) async fn ensure_stream(config: &SdkConfig, stream: &str, shards: i32) {
    let client = aws_sdk_kinesis::Client::new(config);
    if client
        .describe_stream_summary()
        .stream_name(stream)
        .send()
        .await
        .is_err()
    {
        client
            .create_stream()
            .stream_name(stream)
            .shard_count(shards)
            .send()
            .await
            .expect("the stand creates the stream");
    }
    client
        .wait_until_stream_exists()
        .stream_name(stream)
        .wait(Duration::from_mins(1))
        .await
        .expect("the stream becomes active");
}

/// Deletes `stream` if the stand has it, and waits until it is gone.
pub(crate) async fn delete_stream(config: &SdkConfig, stream: &str) {
    let client = aws_sdk_kinesis::Client::new(config);
    if client
        .delete_stream()
        .stream_name(stream)
        .send()
        .await
        .is_ok()
    {
        client
            .wait_until_stream_not_exists()
            .stream_name(stream)
            .wait(Duration::from_mins(1))
            .await
            .expect("the stream goes away");
    }
}

/// The open shards of `stream`, as the stand reports them.
pub(crate) async fn open_shards(config: &SdkConfig, stream: &str) -> i32 {
    aws_sdk_kinesis::Client::new(config)
        .describe_stream_summary()
        .stream_name(stream)
        .send()
        .await
        .expect("the stand describes the stream")
        .stream_description_summary()
        .expect("the summary is present")
        .open_shard_count()
}

/// Writes `payload` to `stream` the way another producer does, with no framework envelope, and
/// answers the record's sequence number.
pub(crate) async fn put_raw(config: &SdkConfig, stream: &str, payload: &str) -> String {
    aws_sdk_kinesis::Client::new(config)
        .put_record()
        .stream_name(stream)
        .partition_key("producer")
        .data(Blob::new(payload.as_bytes()))
        .send()
        .await
        .expect("the stand stores the record")
        .sequence_number()
        .to_owned()
}

#[cfg(feature = "dynamodb-lease")]
// Only the lease-table examples use these.
#[allow(unused_imports)]
pub(crate) use dynamo::*;

#[cfg(feature = "dynamodb-lease")]
mod dynamo {
    use std::time::Duration;

    use aws_config::SdkConfig;
    use aws_sdk_dynamodb::client::Waiters as _;
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    };

    /// Creates the lease table `DynamoLeaseStore` documents: a string partition key named
    /// `lease_key` and nothing else, on-demand billing.
    pub(crate) async fn create_lease_table(config: &SdkConfig, table: &str) {
        let client = aws_sdk_dynamodb::Client::new(config);
        client
            .create_table()
            .table_name(table)
            .billing_mode(BillingMode::PayPerRequest)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("lease_key")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .expect("the attribute definition is complete"),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("lease_key")
                    .key_type(KeyType::Hash)
                    .build()
                    .expect("the key schema is complete"),
            )
            .send()
            .await
            .expect("the stand creates the table");
        client
            .wait_until_table_exists()
            .table_name(table)
            .wait(Duration::from_mins(1))
            .await
            .expect("the table becomes usable");
    }

    /// The string attribute `name` of the row under `key`, or `None` when the row lacks it.
    pub(crate) async fn row_text(
        config: &SdkConfig,
        table: &str,
        key: &str,
        name: &str,
    ) -> Option<String> {
        aws_sdk_dynamodb::Client::new(config)
            .get_item()
            .table_name(table)
            .key("lease_key", AttributeValue::S(key.to_owned()))
            .consistent_read(true)
            .send()
            .await
            .expect("the stand answers the row")
            .item()
            .and_then(|item| item.get(name))
            .and_then(|value| value.as_s().ok())
            .cloned()
    }

    /// Writes the row an earlier version of the crate kept for `shard`: keyed by the bare shard
    /// id, holding a checkpoint.
    pub(crate) async fn put_bare_row(
        config: &SdkConfig,
        table: &str,
        shard: &str,
        checkpoint: &str,
    ) {
        aws_sdk_dynamodb::Client::new(config)
            .put_item()
            .table_name(table)
            .item("lease_key", AttributeValue::S(shard.to_owned()))
            .item("checkpoint", AttributeValue::S(checkpoint.to_owned()))
            .send()
            .await
            .expect("the stand stores the row");
    }
}
