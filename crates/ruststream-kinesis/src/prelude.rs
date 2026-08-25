//! The imports a service on Kinesis writes every time, in one glob.
//!
//! `use ruststream_kinesis::prelude::*;` brings in the framework's own prelude, the capability
//! traits a handler on this broker actually writes, and the transport surface a service names at
//! its mount sites: the broker, the subscription descriptor, the position vocabulary, and the
//! publish policy and its steps.
//!
//! The framework's prelude stops short of brokers on purpose, because which broker a service runs
//! on is the one thing every service states for itself. Importing *this* prelude is that
//! statement: the broker is named by the crate path, so the framework's glob rides along and one
//! import serves a service file.
//!
//! The capability re-exports are a manifest of the vocabulary a service writes, which is narrower
//! than the set this broker implements. A trait earns a place by appearing in a service's own
//! source: in a bound it writes, or as the trait behind a method it calls on a value the runtime
//! hands it - and only where that method has no second home. On Kinesis that is repositioning
//! through the injected seeker and reading a delivered record's position. Everything else the
//! broker implements is contract the runtime and the `AsyncAPI` generator consume on the service's
//! behalf, and a service never spells those names.
//!
//! What this broker cannot do is absent by the same rule, so the glob cannot lend a handler a
//! capability it lacks: there are no transactions and no request-reply here, and those traits are
//! service vocabulary elsewhere. A service on several brokers gets what their globs agree on - the
//! same core traits either way, so the compiler checks the overlap rather than the reader.
//!
//! The policies come through broker-agnostic, under their concept names with the broker prefix
//! stripped: `KinesisPublish` is [`Publish`] here. A mount site then reads the same on every
//! broker, and the manifest principle reaches the policy layer too - every policy this broker
//! supports appears under its concept name, and a concept name that is missing means the broker
//! does not have it. Each prefixed original stays at the crate root, for a file that mounts two
//! brokers at once and has to say which `Publish` it means.
//!
//! [`Publish`] is a *policy*: pure declaration, paired with the connected broker by the runtime.
//! It is not the framework's `runtime::Publish`, the builder that `message(..)` and `raw(..)`
//! return - services never name that type, so the two do not meet.
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
//! // The broker, its descriptors, and its policies come from the same glob.
//! let orders = KinesisStream::new("orders").batch(500);
//! let broker = KinesisBroker::new();
//! let reply_with = Publish;
//! # let _ = (orders, broker, reply_with, handle);
//! ```

// The framework half. A service on this crate is a RustStream service first, so everything the
// core prelude offers - the app object, the handler surface, the extractors, the macros - is
// already what a Kinesis handler is written against.
pub use ruststream::prelude::*;

// The capability half: the traits a handler on this broker writes itself. Each is here because a
// service calls its methods on a value the runtime hands it - `seeker.seek(..)` on the injected
// seeker, `position()` on a delivered record - and a trait's methods need the trait in scope.
//
// Implemented, and deliberately absent, because a service never writes the name: `Seekable` is
// subscriber-side, so a handler names the seeker type and the runtime's plumbing consumes the
// trait; `Subscribe`, `DefaultPublish` and `DescribeServer` are contract the runtime and the
// `AsyncAPI` generator read off the broker.
//
// `Partitioned` is implemented here, but the core surfaces `partition_key` through
// `IncomingMessage`'s defaulted method - re-exporting the trait would make the natural call
// ambiguous (E0034).
//
// Unimplemented, and the absence is the statement: `BatchSubscriber` (the reader flattens
// `GetRecords` batches so each record settles on its own), `RequestReply` (no reply address and no
// correlation primitive), `TransactionalPublisher` and `OwnedTransactions` (`PutRecords` entries
// fail individually, so there is no all-or-nothing to offer).
pub use ruststream::{Positioned, Seeker};

pub use crate::broker::KinesisBroker;
#[cfg(feature = "dynamodb-lease")]
pub use crate::dynamo::DynamoLeaseStore;
pub use crate::message::KinesisPosition;
// The policy layer, under the broker-agnostic concept name. `KinesisPublish` itself stays exported
// from the crate root for a file that mounts two brokers and must disambiguate.
pub use crate::publisher::KinesisPublish as Publish;
// Not a policy, so it keeps its name: the step trait is sealed, and a glob is exactly how a service
// reaches `with_partition_key`.
pub use crate::publisher::KinesisPublishExt;
pub use crate::stream::KinesisStream;
pub use crate::subscriber::KinesisSeeker;

// The `testing` module is deliberately absent: it is feature-gated broker-author tooling, and a
// test that reaches for the in-process broker says so with an explicit import.
// The delivery and transport types (`KinesisMessage`, `KinesisSubscriber`, `KinesisPublisher`,
// `PartitionKeyed`, `ConnectedKinesisBroker`) are deliberately absent for the reason the core
// prelude leaves out `OutgoingMessage`: the runtime drives that layer, and code working at it
// says which layer it is at by naming the type.
// `KinesisError` is deliberately absent: a service names errors where it handles them.
