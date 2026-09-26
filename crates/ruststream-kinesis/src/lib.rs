#![doc = include_str!("README.md")]
#![forbid(unsafe_code)]

mod broker;
mod context;
#[cfg(feature = "dynamodb-lease")]
mod dynamo;
mod error;
mod lease;
mod message;
pub mod prelude;
mod publisher;
mod stream;
mod subscriber;
#[cfg(feature = "testing")]
pub mod testing;
mod track;

/// Restricts the crate's mount-site publish settings to the crate's own policies: the arguments
/// they carry mean something only to a transport that reads them back.
#[doc(hidden)]
pub mod sealed {
    /// The crate's publish policies behind one bound.
    ///
    /// [`KinesisPublishSettings`](crate::KinesisPublishSettings) covers the service's policy and
    /// the in-process stand-in's with a single blanket impl over the mount chain: two impls
    /// would overlap, since coherence cannot tell the two policy types apart through an
    /// associated type.
    #[diagnostic::on_unimplemented(
        message = "`{Self}` is not a Kinesis publish policy",
        note = "a Kinesis publish setting rides the position named right before it: \
                `.out_reply(Publish::default()).partition_key(\"tenant-acme\")`"
    )]
    pub trait PolicyKey: Sized {
        /// Names the record's partition key on this policy.
        #[must_use]
        fn with_partition_key(self, key: String) -> Self;
    }
}

pub use broker::{ConnectedKinesisBroker, KinesisBroker};
pub use context::{KinesisBatchContext, KinesisContext, Position, SeekHandle};
#[cfg(feature = "dynamodb-lease")]
pub use dynamo::DynamoLeaseStore;
pub use error::KinesisError;
pub use lease::{LeaseError, LeaseKey, LeaseState, LeaseStore, MemoryLeaseStore, SHARD_END};
pub use message::{
    KinesisMessage, KinesisPosition, PARTITION_KEY_HEADER, SEQUENCE_HEADER, SHARD_HEADER,
};
pub use publisher::{
    KinesisPublish, KinesisPublishOptions, KinesisPublishSettings, KinesisPublishSteps,
    KinesisPublisher,
};
pub use stream::{KinesisStream, KinesisSubscriberExt};
pub use subscriber::{KinesisSeeker, KinesisSubscriber};
