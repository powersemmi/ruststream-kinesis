//! The per-shard watermark tracker: checkpoint-as-acknowledgement for an ordered log.
//!
//! A checkpoint implies everything before it was handled, so acknowledgements advance a
//! contiguous watermark over deliveries in arrival order (tracked by a monotonic index, never
//! by parsing the 129-digit decimal sequence numbers).

use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Debug, Default)]
struct TrackState {
    /// Outstanding deliveries in arrival order: index -> (sequence number, done).
    pending: BTreeMap<u64, (String, bool)>,
    next_index: u64,
}

/// Tracks outstanding deliveries for one shard.
#[derive(Debug, Default)]
pub(crate) struct Watermark {
    state: Mutex<TrackState>,
}

impl Watermark {
    /// Registers a delivery; returns its tracking index.
    pub(crate) fn deliver(&self, sequence: &str) -> u64 {
        let mut state = self.state.lock().expect("watermark mutex poisoned");
        let index = state.next_index;
        state.next_index += 1;
        state.pending.insert(index, (sequence.to_owned(), false));
        index
    }

    /// Marks a delivery done; returns the highest sequence number the watermark advanced to,
    /// when it moved (every earlier delivery is settled too).
    // The guard intentionally spans the mark and the advance: both must see one consistent
    // pending set.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn settle(&self, index: u64) -> Option<String> {
        let mut state = self.state.lock().expect("watermark mutex poisoned");
        if let Some(entry) = state.pending.get_mut(&index) {
            entry.1 = true;
        }
        let mut advanced = None;
        while let Some((&front, (_, done))) = state.pending.first_key_value() {
            if !done {
                break;
            }
            let (sequence, _) = state.pending.remove(&front).expect("front exists");
            advanced = Some(sequence);
        }
        advanced
    }

    /// Whether every registered delivery has settled (used before closing a finished shard).
    pub(crate) fn drained(&self) -> bool {
        self.state
            .lock()
            .expect("watermark mutex poisoned")
            .pending
            .is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_watermark_advances_only_contiguously() {
        let track = Watermark::default();
        let a = track.deliver("100");
        let b = track.deliver("101");
        let c = track.deliver("102");

        // Settling out of order does not advance past the gap.
        assert_eq!(track.settle(b), None);
        assert_eq!(track.settle(c), None);
        // Settling the front releases the whole contiguous run.
        assert_eq!(track.settle(a), Some("102".to_owned()));
        assert!(track.drained());
    }

    #[test]
    fn an_unsettled_delivery_wedges_the_watermark() {
        let track = Watermark::default();
        let _skipped = track.deliver("100");
        let b = track.deliver("101");
        assert_eq!(track.settle(b), None);
        assert!(!track.drained());
    }
}
