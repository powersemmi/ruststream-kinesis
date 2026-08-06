//! Amazon Kinesis Data Streams broker implementation for `RustStream`.
//!
//! Handlers, routers, codecs, and middleware come from the framework; this crate supplies
//! the transport over the official [`aws-sdk-kinesis`](https://docs.rs/aws-sdk-kinesis) -
//! plus the coordination the vendor's consumer library provides on other platforms and the
//! Rust SDK does not: shard discovery across splits and merges, shard leasing with fencing,
//! and per-shard checkpointing.
//!
//! - Acknowledgement is a checkpoint: `ack` marks a record handled, and the per-shard
//!   watermark advances (and persists) once every earlier record is handled too - a
//!   checkpoint implies everything before it. An unacknowledged record wedges the watermark,
//!   so the shard replays from it when the lease is next taken (at-least-once delivery).
//! - Leases live in a pluggable [`LeaseStore`]: the built-in in-process store is correct for
//!   a single service instance; the `DynamoDB` store behind the `dynamodb-lease` feature lets
//!   multiple instances share the shards with conditional-write fencing.
//! - Children of a split or merge start only after their parents are fully consumed, which
//!   preserves per-key ordering across resharding.
//! - A subscription's start position is always a [`KinesisPosition`]: a shard resumes from
//!   its stored checkpoint and otherwise opens at the tip, and a position repositions it -
//!   through the framework's `start_at(..)` clause at startup, or the `Seekable` capability
//!   while it runs.
//! - Shared polling only in this release: enhanced fan-out is a different resume machine on
//!   an HTTP/2 push stream with no local emulator support, and is not implemented.
//!   KPL-aggregated records are rejected with an error rather than delivered as opaque
//!   protobuf.

#![forbid(unsafe_code)]

mod broker;
#[cfg(feature = "dynamodb-lease")]
mod dynamo;
mod error;
mod lease;
mod message;
mod publisher;
mod stream;
mod subscriber;
#[cfg(feature = "testing")]
pub mod testing;
mod track;

pub use broker::{ConnectedKinesisBroker, KinesisBroker};
#[cfg(feature = "dynamodb-lease")]
pub use dynamo::DynamoLeaseStore;
pub use error::KinesisError;
pub use lease::{LeaseError, LeaseState, LeaseStore, MemoryLeaseStore, SHARD_END};
pub use message::{
    KinesisMessage, KinesisPosition, PARTITION_KEY_HEADER, SEQUENCE_HEADER, SHARD_HEADER,
};
pub use publisher::{KinesisPublish, KinesisPublisher};
pub use stream::KinesisStream;
pub use subscriber::{KinesisSeeker, KinesisSubscriber};
