//! The cursors a shard's reader opens at, and the iterator request each one becomes.

use aws_sdk_kinesis::operation::get_shard_iterator::builders::GetShardIteratorFluentBuilder;
use aws_sdk_kinesis::primitives::DateTime;
use aws_sdk_kinesis::types::ShardIteratorType;

#[cfg(doc)]
use crate::message::KinesisPosition;

/// A position every shard of the subscription can open at: the stream-wide half of
/// [`KinesisPosition`]. Kept apart from the shard-scoped forms so that "install this for the
/// whole subscription" cannot be handed a position that only one shard understands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamStart {
    Horizon,
    Latest,
    Timestamp(u64),
}

/// The cursor one shard's reader opens with, in the shapes the service's iterator types take.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShardStart {
    /// A position shared by the whole subscription.
    Stream(StreamStart),
    /// Redelivers exactly this record: a captured, pinned position.
    At(String),
    /// Resumes after this record: a stored checkpoint.
    After(String),
}

/// Whether a shard's reader picks up a reading its parents left off, or opens one of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lineage {
    /// The shard opens at the subscription's position: the stored checkpoint, a sought position,
    /// or the tip.
    Opens,
    /// A parent was read to its end, so whatever landed on this shard since the split follows
    /// what was delivered. Without a checkpoint the shard opens at its own beginning, as the
    /// vendor's consumer library opens a child: opening it at the tip would skip every record
    /// written to it before its reader started.
    Continues,
}

/// Points a `GetShardIterator` request at a cursor.
pub(crate) fn iterator_at(
    request: GetShardIteratorFluentBuilder,
    start: &ShardStart,
) -> GetShardIteratorFluentBuilder {
    match start {
        ShardStart::Stream(StreamStart::Latest) => {
            request.shard_iterator_type(ShardIteratorType::Latest)
        }
        ShardStart::Stream(StreamStart::Horizon) => {
            request.shard_iterator_type(ShardIteratorType::TrimHorizon)
        }
        ShardStart::Stream(StreamStart::Timestamp(millis)) => request
            .shard_iterator_type(ShardIteratorType::AtTimestamp)
            .timestamp(DateTime::from_millis(
                i64::try_from(*millis).unwrap_or(i64::MAX),
            )),
        ShardStart::At(sequence) => request
            .shard_iterator_type(ShardIteratorType::AtSequenceNumber)
            .starting_sequence_number(sequence),
        ShardStart::After(sequence) => request
            .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
            .starting_sequence_number(sequence),
    }
}
