//! The imports a service on Kinesis writes every time, in one glob: the framework's own prelude,
//! the broker, its subscription descriptor, its positions and seeker, and its publish policy.
//!
//! Policies carry their concept name with the broker prefix stripped, so a mount site reads the
//! same on every broker; `ruststream_kinesis::KinesisPublish` is the prefixed original, for a file
//! that mounts two brokers and has to say which [`Publish`] it means.
//!
//! [`Publish`] is a publish policy, not the framework's `runtime::Publish` builder.
//!
//! # Examples
//!
//! ```
//! use ruststream_kinesis::prelude::*;
//!
//! async fn handle(order: &[u8]) -> HandlerResult {
//!     let _ = order.len();
//!     HandlerResult::Ack
//! }
//!
//! let orders = KinesisStream::new("orders").batch(500);
//! let broker = KinesisBroker::new();
//! let reply_with = Publish;
//! # let _ = (orders, broker, reply_with, handle);
//! ```

pub use ruststream::prelude::*;

// `Partitioned` is implemented here but stays out: the core surfaces `partition_key` through
// `IncomingMessage`'s defaulted method, so re-exporting the trait makes the natural call ambiguous.
pub use ruststream::{Positioned, Seeker};

pub use crate::broker::KinesisBroker;
#[cfg(feature = "dynamodb-lease")]
pub use crate::dynamo::DynamoLeaseStore;
pub use crate::message::KinesisPosition;
pub use crate::publisher::{KinesisPublish as Publish, KinesisPublishExt};
pub use crate::stream::KinesisStream;
pub use crate::subscriber::KinesisSeeker;
