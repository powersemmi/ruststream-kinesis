//! What the live scenarios of the broker, position and testing examples share: the stand's
//! address, the broker and the AWS configuration pointed at it, and the stand-side setup a service
//! expects to find, done with the AWS client the way an operator's tooling does.

// Each example uses the part of this module its scenarios need.
#![allow(dead_code)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_kinesis::client::Waiters as _;
use aws_sdk_kinesis::primitives::Blob;
use ruststream_kinesis::KinesisBroker;

#[path = "../../tests/live/mod.rs"]
mod live;

/// How long a live start may take: the stand answers in milliseconds, and a subscription opens
/// once its first shard listing and lease pass are done.
pub(crate) const START: Duration = Duration::from_secs(30);

/// The stand's endpoint, or `None` to skip the live scenario; under `RUSTSTREAM_REQUIRE_LIVE` a
/// missing endpoint fails instead.
pub(crate) fn endpoint() -> Option<String> {
    live::url("KINESIS_TEST_ENDPOINT")
}

/// The broker a service runs on, pointed at the stand.
pub(crate) fn broker(endpoint: &str) -> KinesisBroker {
    KinesisBroker::new()
        .endpoint(endpoint)
        .region("us-east-1")
        .test_credentials()
}

/// The AWS configuration of the stand, for the client the setup uses.
pub(crate) async fn config(endpoint: &str) -> SdkConfig {
    aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .test_credentials()
        .load()
        .await
}

/// Creates `stream` with one shard unless the stand has it already, and waits until it is active.
pub(crate) async fn ensure_stream(config: &SdkConfig, stream: &str) {
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
            .shard_count(1)
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

/// Deletes `stream` with everything it retains, then creates it empty with one shard.
pub(crate) async fn recreate_stream(config: &SdkConfig, stream: &str) {
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
    ensure_stream(config, stream).await;
}

/// Writes `payload` to `stream` the way another producer does: the plain bytes, no envelope.
pub(crate) async fn put(config: &SdkConfig, stream: &str, payload: &[u8]) {
    aws_sdk_kinesis::Client::new(config)
        .put_record()
        .stream_name(stream)
        .partition_key("producer")
        .data(Blob::new(payload))
        .send()
        .await
        .expect("the stand stores the record");
}

/// The wall clock in milliseconds since the Unix epoch, the unit of a timestamp position.
pub(crate) fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}
