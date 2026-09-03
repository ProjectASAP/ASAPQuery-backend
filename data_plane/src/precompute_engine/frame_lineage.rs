//! Receiver-side ordering and checkpoint validation for summary frames.
//!
//! [`TransmissionPlan::validate_frame`] validates a frame's static shape
//! against the active physical plan. This module owns the stateful half of
//! the wire contract: deltas are only safe to apply after an observed full
//! checkpoint, in sequence, within the exact producer/window lineage.

use control_plane::physical::compiler::{SummaryFrameIdentity, SummaryFrameKind};
use dashmap::mapref::entry::Entry;
use thiserror::Error;

/// The result of accepting a frame into its lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameLineageDecision {
    /// This frame advances (or re-establishes) the lineage and may be applied.
    Apply,
    /// This exact frame was already accepted. Callers must acknowledge it but
    /// must not apply or persist it a second time.
    Duplicate,
}

/// Stateful frame-contract failures. Every failure is recoverable by the
/// producer emitting a new full checkpoint for the same scoped lineage.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FrameLineageError {
    #[error("full frame is missing its checkpoint id")]
    MissingCheckpoint,
    #[error("delta frame is missing its base checkpoint id")]
    MissingBaseCheckpoint,
    #[error("delta has no established full checkpoint")]
    MissingBase,
    #[error("delta base checkpoint does not match the established checkpoint")]
    BaseCheckpointMismatch,
    #[error("frame sequence is stale or conflicts with an accepted frame")]
    StaleOrConflictingSequence,
    #[error("delta sequence gap: expected {expected}, received {received}")]
    SequenceGap { expected: u64, received: u64 },
    #[error("lineage is incomplete and requires a new full checkpoint")]
    Incomplete,
}

/// Exact scope mandated by `SequenceScope::MaterializationWindowProducerEpoch`.
/// `producer_id` is included because epochs are only unique within a producer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FrameLineageKey {
    plan_id: u64,
    plan_version: u64,
    materialization: asap_types::PolicyFingerprint,
    series_identity: String,
    producer_id: String,
    producer_epoch: String,
    window_start_unix_nano: u64,
    window_end_unix_nano: u64,
}

impl From<&SummaryFrameIdentity> for FrameLineageKey {
    fn from(frame: &SummaryFrameIdentity) -> Self {
        Self {
            plan_id: frame.plan_id,
            plan_version: frame.plan_version,
            materialization: frame.materialization,
            series_identity: frame.series_identity.clone(),
            producer_id: frame.producer_id.clone(),
            producer_epoch: frame.producer_epoch.clone(),
            window_start_unix_nano: frame.window_start_unix_nano,
            window_end_unix_nano: frame.window_end_unix_nano,
        }
    }
}

#[derive(Debug, Clone)]
struct FrameLineageState {
    checkpoint_id: Option<String>,
    last_frame: SummaryFrameIdentity,
    incomplete: bool,
}

/// Concurrency-safe receiver state for frame checkpoint/sequence lineages.
///
/// The occupied DashMap entry is held across each transition. Acceptance is
/// therefore linearizable for one lineage (two concurrent copies of sequence
/// N+1 cannot both be applied) without serializing unrelated producers.
#[derive(Debug, Default)]
pub struct FrameLineageTracker {
    lineages: dashmap::DashMap<FrameLineageKey, FrameLineageState>,
}

impl FrameLineageTracker {
    /// Validate and atomically record one frame.
    ///
    /// Full frames establish (or replace) the checkpoint and clear an
    /// incomplete lineage. Delta frames must reference that checkpoint and
    /// advance the last accepted sequence by exactly one. An exact replay of
    /// the last frame is idempotently classified as [`Duplicate`].
    pub fn observe(
        &self,
        frame: &SummaryFrameIdentity,
    ) -> Result<FrameLineageDecision, FrameLineageError> {
        match frame.kind {
            SummaryFrameKind::Full if frame.checkpoint_id.is_none() => {
                return Err(FrameLineageError::MissingCheckpoint);
            }
            SummaryFrameKind::Delta if frame.base_checkpoint_id.is_none() => {
                return Err(FrameLineageError::MissingBaseCheckpoint);
            }
            _ => {}
        }
        let key = FrameLineageKey::from(frame);
        match self.lineages.entry(key) {
            Entry::Vacant(entry) => match frame.kind {
                SummaryFrameKind::Full => {
                    entry.insert(FrameLineageState {
                        checkpoint_id: frame.checkpoint_id.clone(),
                        last_frame: frame.clone(),
                        incomplete: false,
                    });
                    Ok(FrameLineageDecision::Apply)
                }
                // A rejected frame never advances receiver state. In
                // particular, a producer may recover this exact sequence by
                // retransmitting it as a full checkpoint.
                SummaryFrameKind::Delta => Err(FrameLineageError::MissingBase),
            },
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                if state.last_frame == *frame {
                    return Ok(FrameLineageDecision::Duplicate);
                }

                match frame.kind {
                    // A full frame is the explicit recovery boundary. It may
                    // jump over missing delta sequences and resets the base.
                    SummaryFrameKind::Full => {
                        if frame.sequence <= state.last_frame.sequence {
                            return Err(FrameLineageError::StaleOrConflictingSequence);
                        }
                        state.checkpoint_id = frame.checkpoint_id.clone();
                        state.last_frame = frame.clone();
                        state.incomplete = false;
                        Ok(FrameLineageDecision::Apply)
                    }
                    SummaryFrameKind::Delta => {
                        if state.incomplete {
                            return Err(FrameLineageError::Incomplete);
                        }
                        if frame.base_checkpoint_id.as_ref() != state.checkpoint_id.as_ref() {
                            return Err(FrameLineageError::BaseCheckpointMismatch);
                        }
                        if frame.sequence <= state.last_frame.sequence {
                            return Err(FrameLineageError::StaleOrConflictingSequence);
                        }
                        let expected = state.last_frame.sequence.saturating_add(1);
                        if frame.sequence != expected {
                            state.incomplete = true;
                            return Err(FrameLineageError::SequenceGap {
                                expected,
                                received: frame.sequence,
                            });
                        }
                        state.last_frame = frame.clone();
                        Ok(FrameLineageDecision::Apply)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use control_plane::physical::compiler::StateEncoding;

    fn frame(sequence: u64, kind: SummaryFrameKind) -> SummaryFrameIdentity {
        SummaryFrameIdentity {
            identity_version: 1,
            plan_id: 7,
            plan_version: 3,
            backend_compat: "asap-query-backend.v1".into(),
            materialization: asap_types::PolicyFingerprint(41),
            series_identity: "service=checkout,zone=a".into(),
            schema_id: "schema-41".into(),
            producer_id: "edge-a".into(),
            producer_epoch: "boot-9".into(),
            window_start_unix_nano: 100,
            window_end_unix_nano: 200,
            sequence,
            kind: kind.clone(),
            encoding: StateEncoding::SketchlibProtobufV1,
            checkpoint_id: (kind == SummaryFrameKind::Full).then(|| format!("cp-{sequence}")),
            base_checkpoint_id: (kind == SummaryFrameKind::Delta).then(|| "cp-1".into()),
        }
    }

    #[test]
    fn full_establishes_base_and_contiguous_delta_advances() {
        let tracker = FrameLineageTracker::default();
        assert_eq!(
            tracker.observe(&frame(1, SummaryFrameKind::Full)),
            Ok(FrameLineageDecision::Apply)
        );
        assert_eq!(
            tracker.observe(&frame(2, SummaryFrameKind::Delta)),
            Ok(FrameLineageDecision::Apply)
        );
    }

    #[test]
    fn exact_duplicate_is_idempotently_ignored() {
        let tracker = FrameLineageTracker::default();
        let full = frame(1, SummaryFrameKind::Full);
        let delta = frame(2, SummaryFrameKind::Delta);
        tracker.observe(&full).unwrap();
        tracker.observe(&delta).unwrap();
        assert_eq!(tracker.observe(&delta), Ok(FrameLineageDecision::Duplicate));
    }

    #[test]
    fn delta_requires_the_established_checkpoint() {
        let tracker = FrameLineageTracker::default();
        assert_eq!(
            tracker.observe(&frame(1, SummaryFrameKind::Delta)),
            Err(FrameLineageError::MissingBase)
        );
        assert_eq!(
            tracker.observe(&frame(1, SummaryFrameKind::Full)),
            Ok(FrameLineageDecision::Apply),
            "a rejected leading delta must not consume its sequence"
        );

        let tracker = FrameLineageTracker::default();
        tracker.observe(&frame(1, SummaryFrameKind::Full)).unwrap();
        let mut wrong_base = frame(2, SummaryFrameKind::Delta);
        wrong_base.base_checkpoint_id = Some("some-other-checkpoint".into());
        assert_eq!(
            tracker.observe(&wrong_base),
            Err(FrameLineageError::BaseCheckpointMismatch)
        );
    }

    #[test]
    fn checkpoint_identity_is_required_for_both_frame_kinds() {
        let tracker = FrameLineageTracker::default();
        let mut full = frame(1, SummaryFrameKind::Full);
        full.checkpoint_id = None;
        assert_eq!(
            tracker.observe(&full),
            Err(FrameLineageError::MissingCheckpoint)
        );

        let mut delta = frame(2, SummaryFrameKind::Delta);
        delta.base_checkpoint_id = None;
        assert_eq!(
            tracker.observe(&delta),
            Err(FrameLineageError::MissingBaseCheckpoint)
        );
    }

    #[test]
    fn gap_marks_lineage_incomplete_until_next_full() {
        let tracker = FrameLineageTracker::default();
        tracker.observe(&frame(1, SummaryFrameKind::Full)).unwrap();
        assert_eq!(
            tracker.observe(&frame(3, SummaryFrameKind::Delta)),
            Err(FrameLineageError::SequenceGap {
                expected: 2,
                received: 3
            })
        );
        assert_eq!(
            tracker.observe(&frame(2, SummaryFrameKind::Delta)),
            Err(FrameLineageError::Incomplete)
        );

        let recovery = frame(4, SummaryFrameKind::Full);
        assert_eq!(tracker.observe(&recovery), Ok(FrameLineageDecision::Apply));
        let mut next = frame(5, SummaryFrameKind::Delta);
        next.base_checkpoint_id = recovery.checkpoint_id.clone();
        assert_eq!(tracker.observe(&next), Ok(FrameLineageDecision::Apply));
    }

    #[test]
    fn plan_series_epoch_and_window_have_independent_lineages() {
        let tracker = FrameLineageTracker::default();
        let first = frame(1, SummaryFrameKind::Full);
        tracker.observe(&first).unwrap();

        let mut next_plan = first.clone();
        next_plan.plan_version += 1;
        assert_eq!(tracker.observe(&next_plan), Ok(FrameLineageDecision::Apply));

        let mut next_series = first.clone();
        next_series.series_identity = "service=checkout,zone=b".into();
        assert_eq!(
            tracker.observe(&next_series),
            Ok(FrameLineageDecision::Apply)
        );

        let mut next_epoch = first.clone();
        next_epoch.producer_epoch = "boot-10".into();
        assert_eq!(
            tracker.observe(&next_epoch),
            Ok(FrameLineageDecision::Apply)
        );

        let mut next_window = first;
        next_window.window_start_unix_nano = 200;
        next_window.window_end_unix_nano = 300;
        assert_eq!(
            tracker.observe(&next_window),
            Ok(FrameLineageDecision::Apply)
        );
    }
}
