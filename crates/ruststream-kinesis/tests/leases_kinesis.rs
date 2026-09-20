//! The `DynamoDB` lease store against a real table, gated behind `KINESIS_TEST_ENDPOINT`.
//!
//! [`DynamoLeaseStore`] is what lets several instances of a service share a stream's shards, and
//! every one of its operations is a conditional write. A condition that does not hold means two
//! instances believe they own one shard, which is the one failure mode this store exists to
//! prevent - and no in-process store can stand in for it, because the condition is the database's
//! to evaluate. The end of the file drives a real subscription through it, so the table is
//! checked where it is actually used.
//!
//! Start one with `just brokers-up`, then:
//! `KINESIS_TEST_ENDPOINT=http://127.0.0.1:4566 cargo test --all-features -- --test-threads=1`.

#![cfg(feature = "dynamodb-lease")]

use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_dynamodb::client::Waiters;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use futures::StreamExt;

use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, StartAt, Subscriber,
    SubscriptionSource,
};
use ruststream_kinesis::{
    ConnectedKinesisBroker, DynamoLeaseStore, KinesisBroker, KinesisPosition, KinesisStream,
    LeaseStore, SEQUENCE_HEADER, SHARD_HEADER,
};

mod live;

const RECV_TIMEOUT: Duration = Duration::from_secs(30);
/// Long enough that nothing under test expires on its own.
const TTL: Duration = Duration::from_secs(60);

/// The stack's endpoint, or `None` to skip the live checks below. Under `RUSTSTREAM_REQUIRE_LIVE`
/// a missing endpoint is a failure instead, so a job that started a stack cannot report `ok`
/// without having reached it.
fn test_endpoint() -> Option<String> {
    live::url("KINESIS_TEST_ENDPOINT")
}

/// The AWS configuration both the broker and the lease store are built from, pointed at the
/// stack: one stack answers for both services.
async fn config(endpoint: &str) -> SdkConfig {
    aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .test_credentials()
        .load()
        .await
}

/// Per-test unique names, so runs do not observe each other's leftovers.
fn unique(name: &str) -> String {
    format!("ls-{name}-{}", std::process::id())
}

/// Creates the lease table the store documents - a string partition key named `lease_key` and
/// nothing else - and waits until it is usable.
async fn provision_table(config: &SdkConfig, table: &str) -> aws_sdk_dynamodb::Client {
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
        .expect("the stack creates the table");
    client
        .wait_until_table_exists()
        .table_name(table)
        .wait(Duration::from_secs(60))
        .await
        .expect("the table becomes usable");
    client
}

/// The item the store keeps for one shard, read straight out of the table.
async fn item(
    client: &aws_sdk_dynamodb::Client,
    table: &str,
    shard: &str,
) -> std::collections::HashMap<String, AttributeValue> {
    client
        .get_item()
        .table_name(table)
        .key("lease_key", AttributeValue::S(shard.to_owned()))
        .consistent_read(true)
        .send()
        .await
        .expect("the stack answers the item")
        .item()
        .cloned()
        .unwrap_or_default()
}

/// The string attribute `name` of the item, or `None` when the item does not carry it.
fn text(item: &std::collections::HashMap<String, AttributeValue>, name: &str) -> Option<String> {
    item.get(name)
        .and_then(|value| value.as_s().ok())
        .map(ToOwned::to_owned)
}

/// A live lease is one owner's, and a lapsed one is anybody's. Both halves are the database's
/// answer, not the store's: the condition is evaluated where the item lives, which is the whole
/// reason this store exists next to the in-process one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lease_is_exclusive_while_it_lives_and_stealable_once_it_lapses() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let config = config(&endpoint).await;
    let table = unique("exclusive");
    let client = provision_table(&config, &table).await;
    let store = DynamoLeaseStore::new(&config, &table);

    assert!(
        store
            .acquire("shard-a", "owner-a", TTL)
            .await
            .expect("acquire"),
        "an unowned shard is taken",
    );
    assert!(
        !store
            .acquire("shard-a", "owner-b", TTL)
            .await
            .expect("acquire"),
        "a live lease is refused to a second owner",
    );
    assert!(
        store.renew("shard-a", "owner-a", TTL).await.expect("renew"),
        "the owner heartbeats its own lease",
    );
    assert!(
        !store.renew("shard-a", "owner-b", TTL).await.expect("renew"),
        "a lease that is not yours cannot be heartbeated",
    );
    assert_eq!(
        text(&item(&client, &table, "shard-a").await, "lease_owner"),
        Some("owner-a".to_owned()),
        "the table must name the owner that holds the shard",
    );

    // A lease taken for no time at all has lapsed by the time the next call reaches the table.
    assert!(
        store
            .acquire("shard-b", "owner-a", Duration::ZERO)
            .await
            .expect("acquire"),
        "an unowned shard is taken",
    );
    assert!(
        store
            .acquire("shard-b", "owner-b", TTL)
            .await
            .expect("steal"),
        "a lapsed lease is stealable",
    );
    assert!(
        !store.renew("shard-b", "owner-a", TTL).await.expect("renew"),
        "the fenced owner must not be able to heartbeat",
    );
    assert!(
        !store
            .checkpoint("shard-b", "owner-a", "42")
            .await
            .expect("checkpoint"),
        "the fenced owner must not be able to record progress",
    );
    let stolen = item(&client, &table, "shard-b").await;
    assert_eq!(
        text(&stolen, "lease_owner"),
        Some("owner-b".to_owned()),
        "the table must name the owner that stole the shard",
    );
    assert!(
        !stolen.contains_key("checkpoint"),
        "a refused checkpoint must leave no progress behind",
    );
}

/// Progress is the shard's, not the lease holder's: a checkpoint outlives the release, and the
/// next owner resumes from it. Writing one without the lease is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_checkpoint_is_conditional_on_the_lease_and_outlives_the_release() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let config = config(&endpoint).await;
    let table = unique("checkpoint");
    let client = provision_table(&config, &table).await;
    let store = DynamoLeaseStore::new(&config, &table);

    assert!(
        store
            .acquire("shard-a", "owner-a", TTL)
            .await
            .expect("acquire"),
        "an unowned shard is taken",
    );
    assert!(
        store
            .checkpoint("shard-a", "owner-a", "seq-1")
            .await
            .expect("checkpoint"),
        "the owner records progress",
    );
    store
        .release("shard-a", "owner-a")
        .await
        .expect("the owner hands the shard back");

    let released = item(&client, &table, "shard-a").await;
    assert!(
        !released.contains_key("lease_owner"),
        "a released lease leaves no owner in the table",
    );
    assert_eq!(
        text(&released, "checkpoint"),
        Some("seq-1".to_owned()),
        "progress belongs to the shard and outlives the lease",
    );
    assert_eq!(
        store
            .read("shard-a")
            .await
            .expect("read")
            .checkpoint
            .as_deref(),
        Some("seq-1"),
    );

    assert!(
        !store
            .checkpoint("shard-a", "owner-b", "seq-2")
            .await
            .expect("checkpoint"),
        "a checkpoint without the lease is refused",
    );
    assert_eq!(
        store
            .read("shard-a")
            .await
            .expect("read")
            .checkpoint
            .as_deref(),
        Some("seq-1"),
        "a refused checkpoint must not move the shard's progress",
    );

    assert!(
        store
            .acquire("shard-a", "owner-b", TTL)
            .await
            .expect("acquire"),
        "a released shard is taken by the next owner",
    );
    assert!(
        store
            .checkpoint("shard-a", "owner-b", "seq-2")
            .await
            .expect("checkpoint"),
        "the new owner records progress",
    );
    assert_eq!(
        text(&item(&client, &table, "shard-a").await, "checkpoint"),
        Some("seq-2".to_owned()),
    );
}

/// The store where it is used: a subscription over a real shard, checkpointing into a real table.
/// The acknowledged record's own sequence number has to be what the table holds, and the record
/// left unsettled has to leave it there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_records_its_progress_in_the_table() {
    let Some(endpoint) = test_endpoint() else {
        return;
    };
    let config = config(&endpoint).await;
    let table = unique("progress");
    let client = provision_table(&config, &table).await;
    let store = Arc::new(DynamoLeaseStore::new(&config, &table));

    let connected: ConnectedKinesisBroker = KinesisBroker::from_config(config.clone())
        .lease_store(store as Arc<dyn LeaseStore>)
        .owner_id("instance-a")
        .connect()
        .await
        .expect("broker connects");

    let stream_name = unique("progress");
    let mut subscriber = StartAt::new(
        KinesisStream::new(stream_name.as_str())
            .create_if_missing(1)
            .poll_interval(Duration::from_millis(200)),
        KinesisPosition::horizon(),
    )
    .subscribe(&connected)
    .await
    .expect("subscription opens");

    let publisher = connected.publisher();
    for payload in [b"settled".as_slice(), b"left".as_slice()] {
        publisher
            .publish(OutgoingMessage::new(&stream_name, payload), None)
            .await
            .expect("publish succeeds");
    }

    let mut stream = pin!(subscriber.stream());
    let settled = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(settled.payload(), b"settled");
    let shard = settled
        .headers()
        .get_str(SHARD_HEADER)
        .expect("every delivery names its shard")
        .to_owned();
    let sequence = settled
        .headers()
        .get_str(SEQUENCE_HEADER)
        .expect("every delivery names its sequence number")
        .to_owned();
    settled.ack().await.expect("ack succeeds");

    // The write is the ack's own, so the table answers as soon as the checkpoint lands.
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        if text(&item(&client, &table, &shard).await, "checkpoint").as_deref() == Some(&sequence) {
            break;
        }
    }
    assert_eq!(
        text(&item(&client, &table, &shard).await, "checkpoint"),
        Some(sequence.clone()),
        "the table must hold the sequence number of the record that was acknowledged",
    );

    let left = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(left.payload(), b"left");
    left.nack(true).await.expect("nack succeeds");
    assert_eq!(
        text(&item(&client, &table, &shard).await, "checkpoint"),
        Some(sequence),
        "a record left unhandled must not move the shard's progress",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}
