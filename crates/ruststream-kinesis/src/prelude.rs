//! The imports a service on Kinesis writes every time, in one glob.
//!
//! `use ruststream_kinesis::prelude::*;` brings in the framework's own prelude, the framework
//! capabilities this broker implements, and the transport surface a service names at its mount
//! sites: the broker, the subscription descriptor, the position vocabulary, and the publish policy
//! and its steps.
//!
//! The framework's prelude stops short of brokers on purpose, because which broker a service runs
//! on is the one thing every service states for itself. Importing *this* prelude is that
//! statement: the broker is named by the crate path, so the framework's glob rides along and one
//! import serves a service file.
//!
//! The capability re-exports make the glob a manifest of what this broker can do. Kinesis is a
//! sharded, retained log, so it seeks, reports positions and partition keys, resolves a bare
//! stream name, and describes itself; it has no transactions, no request-reply, and hands records
//! to the framework's batching layer one at a time rather than natively. The traits it does not
//! implement are absent, so the glob cannot lend a handler a capability this broker lacks, and a
//! service on several brokers gets what their globs agree on - the same core traits either way, so
//! the compiler checks the overlap rather than the reader.
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
//! // The broker and its descriptors come from the same glob.
//! let orders = KinesisStream::new("orders").batch(500);
//! let broker = KinesisBroker::new();
//! # let _ = (orders, broker, handle);
//! ```

// The framework half. A service on this crate is a RustStream service first, so everything the
// core prelude offers - the app object, the handler surface, the extractors, the macros - is
// already what a Kinesis handler is written against.
pub use ruststream::prelude::*;

// The capability half: precisely the framework capability traits this crate implements, and
// nothing it does not. `Seeker` rides along with `Seekable` because repositioning is a call on the
// injected handle, and a trait's methods need the trait in scope.
//
// Absent because unimplemented, and the absence is the statement: `BatchSubscriber` (the reader
// flattens `GetRecords` batches so each record settles on its own), `RequestReply` (no reply
// address and no correlation primitive), `TransactionalPublisher` and `OwnedTransactions`
// (`PutRecords` entries fail individually, so there is no all-or-nothing to offer).
pub use ruststream::{DescribeServer, Partitioned, Positioned, Seekable, Seeker, Subscribe};

pub use crate::broker::KinesisBroker;
#[cfg(feature = "dynamodb-lease")]
pub use crate::dynamo::DynamoLeaseStore;
pub use crate::message::KinesisPosition;
// The step trait is sealed, so a glob is exactly how a service reaches `with_partition_key`.
pub use crate::publisher::{KinesisPublish, KinesisPublishExt};
pub use crate::stream::KinesisStream;
pub use crate::subscriber::KinesisSeeker;

// The `testing` module is deliberately absent: it is feature-gated broker-author tooling, and a
// test that reaches for the in-process broker says so with an explicit import.
// The delivery and transport types (`KinesisMessage`, `KinesisSubscriber`, `KinesisPublisher`,
// `PartitionKeyed`, `ConnectedKinesisBroker`) are deliberately absent for the reason the core
// prelude leaves out `OutgoingMessage`: the runtime drives that layer, and code working at it
// says which layer it is at by naming the type.
// `KinesisError` is deliberately absent: a service names errors where it handles them.
