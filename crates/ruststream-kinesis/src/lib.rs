#![doc = include_str!("README.md")]
#![forbid(unsafe_code)]

mod broker;
mod claim;
mod clients;
mod context;
mod cursor;
#[cfg(feature = "dynamodb-lease")]
mod dynamo;
pub mod envelope;
mod error;
#[cfg(feature = "testing")]
mod in_process;
mod lease;
mod message;
pub mod prelude;
mod publisher;
mod stream;
mod subscriber;
mod track;

/// Restricts the crate's mount-site publish settings to the crate's own policies: the arguments
/// they carry mean something only to a transport that reads them back.
#[doc(hidden)]
pub mod sealed {
    use crate::envelope::Envelope;

    /// A subscription source whose envelope the mount-site step
    /// [`KinesisEnvelopeExt::envelope`](crate::KinesisEnvelopeExt::envelope) names: the crate's
    /// descriptor, alone or wrapped in a start position.
    #[diagnostic::on_unimplemented(
        message = "`{Self}` does not read in a Kinesis envelope",
        note = "`.envelope(..)` follows a handler whose source is a `KinesisStream`, with or \
                without a `start_at(..)`"
    )]
    pub trait EnvelopedSource: Sized {
        /// The source reading in `Named`.
        type Enveloped<Named: Envelope>;

        /// The source reading in `envelope`.
        fn with_envelope<Named: Envelope>(self, envelope: Named) -> Self::Enveloped<Named>;
    }

    /// The crate's publish policy behind a bound.
    ///
    /// [`KinesisPublishSettings`](crate::KinesisPublishSettings) is one blanket impl over the
    /// mount chain, bounded on the policy the chain named, so the settings appear after this
    /// crate's policy and after no other broker's.
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
pub use envelope::{Envelope, Framed, NoOp, Rsk};
pub use error::KinesisError;
pub use lease::{LeaseError, LeaseKey, LeaseState, LeaseStore, MemoryLeaseStore, SHARD_END};
pub use message::{
    KinesisMessage, KinesisPosition, PARTITION_KEY_HEADER, SEQUENCE_HEADER, SHARD_HEADER,
};
pub use publisher::{
    KinesisPublish, KinesisPublishOptions, KinesisPublishSettings, KinesisPublishSteps,
    KinesisPublisher,
};
pub use stream::{KinesisEnvelopeExt, KinesisStream, KinesisSubscriberExt};
pub use subscriber::{KinesisSeeker, KinesisSubscriber};
