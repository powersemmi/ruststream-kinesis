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

/// The cursor a shard's reader opens at: when it starts, and when it lost its iterator.
///
/// The stored checkpoint wins: what was settled is not read again. Without one, a reader that
/// already read the shard goes back to where it started reading (`rewind`), so a failure loses
/// none of what it read and had not settled. Only a reader that never read the shard falls to its
/// lineage: a child of a consumed parent opens at its own beginning, any other shard at the
/// subscription's position or the tip.
pub(crate) fn reopen_at(
    checkpoint: Option<String>,
    rewind: Option<&ShardStart>,
    lineage: Lineage,
    installed: Option<StreamStart>,
) -> ShardStart {
    match (checkpoint, rewind, lineage, installed) {
        (Some(sequence), ..) => ShardStart::After(sequence),
        (None, Some(rewind), ..) => rewind.clone(),
        (None, None, Lineage::Continues, Some(StreamStart::Timestamp(millis))) => {
            ShardStart::Stream(StreamStart::Timestamp(millis))
        }
        (None, None, Lineage::Continues, _) => ShardStart::Stream(StreamStart::Horizon),
        (None, None, Lineage::Opens, installed) => {
            ShardStart::Stream(installed.unwrap_or(StreamStart::Latest))
        }
    }
}

/// The cursor that reads the shard again from `start` later on, if there is one.
///
/// The tip moves, so a reader that opened there has no position to go back to until it reads its
/// first record; every other cursor names the same place each time it is fetched.
pub(crate) fn rewind_to(start: &ShardStart) -> Option<ShardStart> {
    match start {
        ShardStart::Stream(StreamStart::Latest) => None,
        other => Some(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reader that opened at the tip and lost its iterator before any record was settled has no
    /// checkpoint to go back to. The tip has moved on since it opened, so reopening there would
    /// skip every record it had not yet handed out; it goes back to the first record it read.
    #[test]
    fn a_reader_without_a_checkpoint_reopens_at_the_first_record_it_read() {
        let first =
            ShardStart::At("49590338271490256608559692538361571095921575989136588898".into());
        assert_eq!(reopen_at(None, Some(&first), Lineage::Opens, None), first);
        assert_eq!(
            reopen_at(
                None,
                Some(&first),
                Lineage::Opens,
                Some(StreamStart::Latest)
            ),
            first
        );
    }

    /// What was settled is not read again: the checkpoint wins over where the reader started.
    #[test]
    fn the_checkpoint_wins_over_where_the_reader_started() {
        let first = ShardStart::At("1".into());
        assert_eq!(
            reopen_at(Some("7".into()), Some(&first), Lineage::Opens, None),
            ShardStart::After("7".into())
        );
    }

    /// The tip is where a reader is told to start, not a place it can return to.
    #[test]
    fn the_tip_is_no_place_to_rewind_to() {
        assert_eq!(rewind_to(&ShardStart::Stream(StreamStart::Latest)), None);
        assert_eq!(
            rewind_to(&ShardStart::Stream(StreamStart::Horizon)),
            Some(ShardStart::Stream(StreamStart::Horizon))
        );
        assert_eq!(
            rewind_to(&ShardStart::After("7".into())),
            Some(ShardStart::After("7".into()))
        );
    }
}
