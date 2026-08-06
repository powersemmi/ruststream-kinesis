//! The crate-level error type.

use std::error::Error as StdError;

/// Errors returned by the Amazon Kinesis Data Streams broker.
///
/// One enum for the whole crate, variants by source, per the `RustStream` broker conventions.
/// The wrapped sources are boxed `std` errors formatted with their full cause chain, so the
/// public API does not leak the SDK's layered error types.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum KinesisError {
    /// Loading the AWS configuration failed.
    #[error("aws config error: {0}")]
    Config(String),

    /// A stream admin call (describe, create, shard listing) failed.
    #[error("kinesis stream error for '{stream}': {source}")]
    Stream {
        /// The stream the call was about.
        stream: String,
        /// The SDK's failure, with its cause chain.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Reading a shard failed permanently.
    #[error("kinesis read error on '{stream}' shard '{shard}': {source}")]
    Read {
        /// The stream the shard belongs to.
        stream: String,
        /// The shard the read targeted.
        shard: String,
        /// The SDK's failure, with its cause chain.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// A delivered record is KPL-aggregated, which this crate does not deaggregate yet;
    /// failing loudly beats handing the handler an opaque protobuf blob.
    #[error("kinesis record on shard '{shard}' is KPL-aggregated (unsupported)")]
    AggregatedRecord {
        /// The shard the record arrived on.
        shard: String,
    },

    /// Writing a record failed.
    #[error("kinesis publish error to '{stream}': {source}")]
    Publish {
        /// The stream the record targeted.
        stream: String,
        /// The SDK's failure, with its cause chain.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The lease store failed.
    #[error("kinesis lease store error for shard '{shard}': {source}")]
    Lease {
        /// The shard whose lease was involved.
        shard: String,
        /// The store's failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The handle is used before `connect` filled the shared connection, or after `shutdown`.
    #[error("kinesis broker is not connected")]
    NotConnected,

    /// A stream descriptor is invalid.
    #[error("invalid kinesis descriptor: {0}")]
    Invalid(String),
}

/// Formats an SDK error with its full cause chain and boxes it.
pub(crate) fn sdk_err<E, R>(
    err: &aws_sdk_kinesis::error::SdkError<E, R>,
) -> Box<dyn StdError + Send + Sync>
where
    E: StdError + Send + Sync + 'static,
    R: std::fmt::Debug + Send + Sync + 'static,
{
    Box::from(aws_sdk_kinesis::error::DisplayErrorContext(err).to_string())
}
