//! The imports a service on Kinesis writes every time, in one glob.
//!
//! The framework's own prelude, the broker, its subscription descriptor and the settings that
//! chain onto a mount site, its positions and seeker, the delivery context with the keys that
//! read it, and its publish policy.
//!
//! A service writes two kinds of file, and they import different things. A handler body names
//! only what it needs of the framework (`use ruststream::prelude::*`) and bounds an injected
//! publisher with a capability trait, so it never learns which broker it runs on. The file that
//! mounts those handlers imports this glob, where the broker's policies and context keys carry
//! their concept name with the prefix stripped - [`Publish`] here - so a mount site reads the
//! same on every broker; each prefixed original stays at the crate root, for a file that mounts
//! two brokers and has to say which [`Publish`] it means.
//!
//! # Examples
//!
//! ```
//! use std::time::Duration;
//!
//! use ruststream_kinesis::prelude::*;
//! # #[derive(serde::Deserialize)]
//! # struct Order { id: u64 }
//!
//! async fn handle(order: &Order, Ctx(at): Ctx<Position>) -> HandlerOutcome {
//!     println!("order {} sits at {at:?}", order.id);
//!     HandlerOutcome::ack()
//! }
//!
//! let orders = KinesisStream::new("orders").poll_interval(Duration::from_millis(500));
//! let broker = KinesisBroker::new();
//! let reply_with = Publish::default();
//! # let _ = (orders, broker, reply_with, handle);
//! ```

pub use ruststream::prelude::*;

// `Partitioned` is implemented here but stays out: the core surfaces `partition_key` through
// `IncomingMessage`'s defaulted method, so re-exporting the trait makes the natural call ambiguous.
pub use ruststream::{Positioned, Seeker};

pub use crate::broker::KinesisBroker;
pub use crate::context::{KinesisBatchContext, KinesisContext, Position, SeekHandle};
#[cfg(feature = "dynamodb-lease")]
pub use crate::dynamo::DynamoLeaseStore;
pub use crate::message::KinesisPosition;
pub use crate::publisher::{KinesisPublish as Publish, KinesisPublishExt};
pub use crate::stream::{KinesisStream, KinesisSubscriberExt};
pub use crate::subscriber::KinesisSeeker;
