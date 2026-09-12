//! In-process test support, behind the `testing` feature.
//!
//! [`KinesisTestBroker`] is a transport that reproduces the crate's routing in memory - no
//! server, no network - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so application
//! handlers can be unit-tested with the [`TestApp`](ruststream::testing::TestApp) harness.
//!
//! It routes by exact address match over a **retained log**, because that is the one product
//! property a Kinesis handler observes without a server: a subscription opens at the tip, and
//! `start_at(..)` or the [`SeekHandle`](crate::SeekHandle) of a delivery context really re-reads
//! the log. Deliveries carry the crate's full context - a [`KinesisPosition`](crate::KinesisPosition),
//! the position headers, the partition key - so a handler that seeks or reads its position is the
//! same code here and against the service.
//!
//! The mount is the service's own: the crate's descriptor
//! ([`KinesisStream`](crate::KinesisStream)) opens a subscription here, and its publish policy
//! ([`KinesisPublish`](crate::KinesisPublish)) pairs into [`KinesisTestPublisher`] - so a routes
//! file names the same two types in a unit test that it names in production.
//!
//! What it still does not simulate is everything a server owns: shards beyond the one
//! ([`IN_PROCESS_SHARD`]), leases, checkpoint durability, retention limits, resharding, and
//! redelivery timing. Those are verified end to end against a real broker.

mod broker;
mod router;
mod seek;
mod subscriber;

pub use broker::{ConnectedKinesisTestBroker, KinesisTestBroker, KinesisTestPublisher};
pub use seek::IN_PROCESS_SHARD;
pub(crate) use seek::LogSeeker;
pub use subscriber::{KinesisTestMessage, KinesisTestSubscriber};
