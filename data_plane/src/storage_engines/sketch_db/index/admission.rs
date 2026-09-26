//! Store-owned tracking of accepted, not necessarily published summary updates.
//! This records known work; it does not infer an event-time watermark.

use asap_types::sds::StoredOutputId;
use asap_types::sds::{CatalogGeneration, HalfOpenTimeRange, SummaryInstanceCoordinates};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
struct WindowRevision {
    admitted: u64,
    published: u64,
    series_id: Option<u64>,
    pending: BTreeSet<u64>,
}

#[derive(Default, PartialEq, Eq)]
enum FiniteInputState {
    #[default]
    Open,
    Closing,
    Complete,
}

#[derive(Default)]
pub(super) struct AdmissionInventory {
    generation: Option<CatalogGeneration>,
    revision: u64,
    windows: BTreeMap<SummaryInstanceCoordinates, WindowRevision>,
    metadata_bytes: usize,
    replay_floors: BTreeMap<StoredOutputId, i64>,
    observed_extent: Option<HalfOpenTimeRange>,
    finite_input: FiniteInputState,
    published_series: BTreeMap<u64, u64>,
    pending_revisions: usize,
}

impl AdmissionInventory {
    const MAX_WINDOWS: usize = 262_144;
    const MAX_METADATA_BYTES: usize = 64 * 1024 * 1024;

    pub(super) fn install(&mut self, generation: CatalogGeneration) {
        if self.generation.as_ref() != Some(&generation) {
            self.generation = Some(generation);
            self.windows.clear();
            self.metadata_bytes = 0;
            self.replay_floors.clear();
            self.observed_extent = None;
            self.finite_input = FiniteInputState::Open;
            self.published_series.clear();
            self.pending_revisions = 0;
            self.revision = self.revision.saturating_add(1);
        }
    }

    /// All validation and capacity checks precede mutation. The caller reserves
    /// the whole queue batch before invoking this operation.
    pub(super) fn admit(
        &mut self,
        generation: &CatalogGeneration,
        coordinates: BTreeSet<SummaryInstanceCoordinates>,
    ) -> Result<u64, String> {
        if self.generation.as_ref() != Some(generation) {
            return Err("summary admission catalog generation differs".into());
        }
        if self.is_finite_closed() {
            return Err("finite summary input is closed".into());
        }
        let mut added = 0usize;
        let mut bytes = 0usize;
        for coordinate in &coordinates {
            if self
                .replay_floors
                .get(&coordinate.stored_output_id)
                .is_some_and(|floor| coordinate.time_range.end_ms <= *floor)
            {
                return Err("summary input precedes retained replay horizon".into());
            }
            coordinate
                .time_range
                .validate()
                .map_err(|e| e.to_string())?;
            if !self.windows.contains_key(coordinate) {
                added += 1;
                bytes = bytes.saturating_add(Self::coordinate_bytes(coordinate));
            }
        }
        if self.pending_revisions.saturating_add(coordinates.len()) > Self::MAX_WINDOWS
            || self.windows.len().saturating_add(added) > Self::MAX_WINDOWS
            || self.metadata_bytes.saturating_add(bytes) > Self::MAX_METADATA_BYTES
        {
            return Err("summary admission inventory capacity exceeded".into());
        }
        let revision = self
            .revision
            .checked_add(1)
            .ok_or("summary admission revision exhausted")?;
        for coordinate in coordinates {
            self.observed_extent = Some(match self.observed_extent {
                Some(range) => HalfOpenTimeRange {
                    start_ms: range.start_ms.min(coordinate.time_range.start_ms),
                    end_ms: range.end_ms.max(coordinate.time_range.end_ms),
                },
                None => coordinate.time_range,
            });
            let window = self.windows.entry(coordinate).or_default();
            window.admitted = revision;
            window.pending.insert(revision);
            self.pending_revisions += 1;
        }
        self.metadata_bytes += bytes;
        self.revision = revision;
        Ok(revision)
    }

    pub(super) fn validate_publication(
        &self,
        generation: &CatalogGeneration,
        coordinate: &SummaryInstanceCoordinates,
        first_revision: u64,
        revision: u64,
    ) -> Result<bool, String> {
        if self.generation.as_ref() != Some(generation) {
            return Err("summary publication catalog generation differs".into());
        }
        let window = self
            .windows
            .get(coordinate)
            .ok_or("summary publication was not admitted")?;
        if revision == 0 || revision > window.admitted {
            return Err("summary publication revision was not admitted".into());
        }
        if window.published >= revision {
            return Ok(true);
        }
        if window.series_id.is_none() && self.published_series.len() >= Self::MAX_WINDOWS {
            return Err("summary admission series capacity exceeded".into());
        }
        if self.revision == u64::MAX {
            return Err("summary admission revision exhausted".into());
        }
        if !window.pending.contains(&revision) {
            return Err("summary output revision was not admitted to this coordinate".into());
        }
        if first_revision == 0
            || first_revision > revision
            || window
                .pending
                .first()
                .is_some_and(|pending| first_revision > *pending)
        {
            return Err("summary output omitted an earlier unpublished input revision".into());
        }
        Ok(false)
    }

    pub(super) fn acknowledge(
        &mut self,
        generation: &CatalogGeneration,
        coordinate: &SummaryInstanceCoordinates,
        revision: u64,
    ) -> Result<(), String> {
        if self.generation.as_ref() != Some(generation) {
            return Err("summary publication catalog generation differs".into());
        }
        let window = self
            .windows
            .get_mut(coordinate)
            .ok_or("summary publication was not admitted")?;
        if revision == 0 || revision > window.admitted {
            return Err("summary publication revision was not admitted".into());
        }
        window.published = window.published.max(revision);
        let before = window.pending.len();
        window.pending.retain(|pending| *pending > revision);
        self.pending_revisions -= before - window.pending.len();
        // A newer admission remains pending even if an old write completes now.
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or("summary admission revision exhausted")?;
        Ok(())
    }

    pub(super) fn record_series(
        &mut self,
        generation: &CatalogGeneration,
        coordinate: &SummaryInstanceCoordinates,
        series_id: u64,
    ) -> Result<(), String> {
        if self.generation.as_ref() != Some(generation) {
            return Err("summary series publication catalog generation differs".into());
        }
        if !self.published_series.contains_key(&series_id)
            && self.published_series.len() >= Self::MAX_WINDOWS
        {
            return Err("summary admission series capacity exceeded".into());
        }
        let window = self
            .windows
            .get_mut(coordinate)
            .ok_or("published window was not admitted")?;
        if window
            .series_id
            .is_some_and(|previous| previous != series_id)
        {
            return Err("summary coordinate changed series identity".into());
        }
        window.series_id = Some(series_id);
        let end = u64::try_from(coordinate.time_range.end_ms)
            .map_err(|_| "published window end is outside storage timestamp range")?;
        self.published_series
            .entry(series_id)
            .and_modify(|current| *current = (*current).max(end))
            .or_insert(end);
        Ok(())
    }

    pub(super) fn validate_finite(&self, generation: &CatalogGeneration) -> Result<u64, String> {
        if self.generation.as_ref() != Some(generation) {
            return Err("finite completion catalog generation differs".into());
        }
        if self
            .windows
            .values()
            .any(|window| window.published < window.admitted)
        {
            return Err("finite source has unpublished summary windows".into());
        }
        self.revision
            .checked_add(1)
            .ok_or_else(|| "summary admission revision exhausted".into())
    }

    pub(super) fn seal_finite(&mut self, generation: &CatalogGeneration) -> Result<(), String> {
        let revision = self.validate_finite(generation)?;
        self.finite_input = FiniteInputState::Complete;
        self.revision = revision;
        Ok(())
    }

    pub(super) fn begin_finite_close(&mut self) {
        self.finite_input = FiniteInputState::Closing;
    }

    pub(super) fn is_finite_closed(&self) -> bool {
        self.finite_input != FiniteInputState::Open
    }

    pub(super) fn is_finite_complete(&self) -> bool {
        self.finite_input == FiniteInputState::Complete
    }

    pub(super) fn known_empty(
        &self,
        definition: StoredOutputId,
        series_id: u64,
        range: HalfOpenTimeRange,
    ) -> bool {
        self.known_empty_with_layout(definition, series_id, range, false)
    }

    pub(super) fn known_empty_with_layout(
        &self,
        definition: StoredOutputId,
        series_id: u64,
        range: HalfOpenTimeRange,
        full_window: bool,
    ) -> bool {
        self.finite_input == FiniteInputState::Complete
            && self.published_series.contains_key(&series_id)
            && self.observed_extent.is_some_and(|extent| {
                range.start_ms >= extent.start_ms && range.end_ms <= extent.end_ms
            })
            && self
                .replay_floors
                .get(&definition)
                .is_none_or(|floor| range.start_ms >= *floor)
            && !self.windows.iter().any(|(coordinate, state)| {
                coordinate.stored_output_id == definition
                    && (if full_window {
                        coordinate.time_range == range
                    } else {
                        coordinate.time_range.start_ms < range.end_ms
                            && coordinate.time_range.end_ms > range.start_ms
                    })
                    && state.series_id == Some(series_id)
            })
    }

    pub(super) fn published_frontiers(&self) -> &BTreeMap<u64, u64> {
        &self.published_series
    }

    pub(super) fn revision(&self) -> u64 {
        self.revision
    }

    pub(super) fn has_pending(
        &self,
        definition: StoredOutputId,
        range: HalfOpenTimeRange,
        full_window: bool,
    ) -> bool {
        self.windows.iter().any(|(coordinate, state)| {
            coordinate.stored_output_id == definition
                && (if full_window {
                    coordinate.time_range == range
                } else {
                    coordinate.time_range.start_ms < range.end_ms
                        && coordinate.time_range.end_ms > range.start_ms
                })
                && state.published < state.admitted
        })
    }

    /// Advancing this configured replay floor also rejects future old admission.
    /// Pending work is never forgotten because a different series ran ahead.
    pub(super) fn retire_completed_before(&mut self, definition: StoredOutputId, frontier_ms: i64) {
        let floor = self.replay_floors.entry(definition).or_insert(i64::MIN);
        *floor = (*floor).max(frontier_ms);
        let frontier_ms = *floor;
        let pending: BTreeSet<_> = self
            .windows
            .values()
            .flat_map(|state| state.pending.iter().copied())
            .collect();
        self.windows.retain(|coordinate, state| {
            let remove = coordinate.stored_output_id == definition
                && coordinate.time_range.end_ms <= frontier_ms
                && state.published >= state.admitted
                && !pending.contains(&state.admitted);
            if remove {
                self.metadata_bytes = self
                    .metadata_bytes
                    .saturating_sub(Self::coordinate_bytes(coordinate));
            }
            !remove
        });
    }

    fn coordinate_bytes(coordinate: &SummaryInstanceCoordinates) -> usize {
        std::mem::size_of::<SummaryInstanceCoordinates>()
            + coordinate
                .group_values
                .iter()
                .map(|(key, value)| key.len() + value.len() + 64)
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn generation(version: u64) -> CatalogGeneration {
        CatalogGeneration {
            schema_version: 1,
            plan_id: 1,
            plan_version: version,
            snapshot_sha256: format!("snapshot-{version}"),
        }
    }
    fn window(series: &str) -> SummaryInstanceCoordinates {
        SummaryInstanceCoordinates {
            stored_output_id: StoredOutputId::from(asap_types::PolicyFingerprint(7)),
            time_range: HalfOpenTimeRange {
                start_ms: 0,
                end_ms: 1000,
            },
            group_values: BTreeMap::from([("instance".into(), series.into())]),
        }
    }

    #[test]
    fn fast_series_publication_cannot_hide_a_queued_series() {
        let generation = generation(1);
        let mut inventory = AdmissionInventory::default();
        inventory.install(generation.clone());
        let a = window("a");
        let b = window("b");
        let revision = inventory
            .admit(&generation, BTreeSet::from([a.clone(), b.clone()]))
            .unwrap();
        inventory.acknowledge(&generation, &a, revision).unwrap();
        assert!(inventory.has_pending(a.stored_output_id, a.time_range, false));
        inventory.acknowledge(&generation, &b, revision).unwrap();
        assert!(!inventory.has_pending(a.stored_output_id, a.time_range, false));
    }

    #[test]
    fn old_publication_cannot_acknowledge_a_newer_update() {
        let generation = generation(1);
        let mut inventory = AdmissionInventory::default();
        inventory.install(generation.clone());
        let coordinate = window("a");
        let first = inventory
            .admit(&generation, BTreeSet::from([coordinate.clone()]))
            .unwrap();
        let second = inventory
            .admit(&generation, BTreeSet::from([coordinate.clone()]))
            .unwrap();
        let before = inventory.revision();
        inventory
            .acknowledge(&generation, &coordinate, first)
            .unwrap();
        assert_ne!(before, inventory.revision());
        assert!(inventory.has_pending(coordinate.stored_output_id, coordinate.time_range, false));
        inventory.retire_completed_before(coordinate.stored_output_id, 1000);
        assert_eq!(inventory.windows.len(), 1);
        inventory
            .record_series(&generation, &coordinate, 42)
            .unwrap();
        inventory
            .acknowledge(&generation, &coordinate, second)
            .unwrap();
        inventory.retire_completed_before(coordinate.stored_output_id, 1000);
        assert!(inventory.windows.is_empty());
        assert_eq!(
            inventory.published_frontiers().get(&42),
            Some(&(coordinate.time_range.end_ms as u64))
        );
    }

    #[test]
    fn later_output_cannot_hide_an_unpublished_earlier_input() {
        let generation = generation(1);
        let mut inventory = AdmissionInventory::default();
        inventory.install(generation.clone());
        let coordinate = window("a");
        let first = inventory
            .admit(&generation, BTreeSet::from([coordinate.clone()]))
            .unwrap();
        let second = inventory
            .admit(&generation, BTreeSet::from([coordinate.clone()]))
            .unwrap();
        assert!(inventory
            .validate_publication(&generation, &coordinate, second, second)
            .is_err());
        assert_eq!(
            inventory.validate_publication(&generation, &coordinate, first, second),
            Ok(false)
        );
        inventory
            .acknowledge(&generation, &coordinate, first)
            .unwrap();
        assert_eq!(
            inventory.validate_publication(&generation, &coordinate, second, second),
            Ok(false)
        );
    }

    #[test]
    fn generation_switch_rejects_old_completion_and_admission() {
        let mut inventory = AdmissionInventory::default();
        let old = generation(1);
        inventory.install(old.clone());
        let coordinate = window("a");
        let revision = inventory
            .admit(&old, BTreeSet::from([coordinate.clone()]))
            .unwrap();
        inventory.install(generation(2));
        assert!(inventory.acknowledge(&old, &coordinate, revision).is_err());
        assert!(inventory.admit(&old, BTreeSet::from([coordinate])).is_err());
    }

    // Unpublished future snapshots cannot block an already-published full window.
    #[test]
    fn pending_full_window_checks_only_the_requested_snapshot() {
        let generation = generation(1);
        let mut inventory = AdmissionInventory::default();
        inventory.install(generation.clone());
        let current = window("a");
        let mut future = current.clone();
        future.time_range = HalfOpenTimeRange {
            start_ms: 500,
            end_ms: 1500,
        };
        let revision = inventory
            .admit(&generation, BTreeSet::from([current.clone(), future]))
            .unwrap();
        assert!(inventory.has_pending(current.stored_output_id, current.time_range, true));
        inventory
            .acknowledge(&generation, &current, revision)
            .unwrap();
        assert!(inventory.has_pending(current.stored_output_id, current.time_range, false));
        assert!(!inventory.has_pending(current.stored_output_id, current.time_range, true));
    }

    // A neighboring full snapshot may overlap an empty query population.
    #[test]
    fn full_window_empty_proof_uses_exact_window_identity() {
        let generation = generation(1);
        let mut inventory = AdmissionInventory::default();
        inventory.install(generation.clone());
        let first = window("a");
        let mut neighbor = first.clone();
        neighbor.time_range = HalfOpenTimeRange {
            start_ms: 500,
            end_ms: 1500,
        };
        let mut other = window("b");
        other.time_range = HalfOpenTimeRange {
            start_ms: 1000,
            end_ms: 2000,
        };
        let revision = inventory
            .admit(
                &generation,
                BTreeSet::from([first.clone(), neighbor.clone(), other.clone()]),
            )
            .unwrap();
        for (coordinate, sid) in [(&first, 1), (&neighbor, 1), (&other, 2)] {
            inventory
                .record_series(&generation, coordinate, sid)
                .unwrap();
            inventory
                .acknowledge(&generation, coordinate, revision)
                .unwrap();
        }
        assert!(!inventory.known_empty_with_layout(
            first.stored_output_id,
            1,
            other.time_range,
            true
        ));
        inventory.seal_finite(&generation).unwrap();
        assert!(!inventory.known_empty(first.stored_output_id, 1, other.time_range));
        assert!(inventory.known_empty_with_layout(
            first.stored_output_id,
            1,
            other.time_range,
            true
        ));
        assert!(!inventory.known_empty_with_layout(
            first.stored_output_id,
            2,
            other.time_range,
            true
        ));
        inventory.retire_completed_before(first.stored_output_id, 2000);
        assert!(!inventory.known_empty_with_layout(
            first.stored_output_id,
            1,
            other.time_range,
            true
        ));
    }

    #[test]
    fn only_finite_completion_proves_an_inactive_series_window_empty() {
        let generation = generation(1);
        let mut inventory = AdmissionInventory::default();
        inventory.install(generation.clone());
        let first = window("a");
        let mut second = window("b");
        second.time_range = HalfOpenTimeRange {
            start_ms: 1000,
            end_ms: 2000,
        };
        let revision = inventory
            .admit(&generation, BTreeSet::from([first.clone(), second.clone()]))
            .unwrap();
        inventory.record_series(&generation, &first, 1).unwrap();
        inventory
            .acknowledge(&generation, &first, revision)
            .unwrap();
        assert!(inventory.seal_finite(&generation).is_err());
        inventory.record_series(&generation, &second, 2).unwrap();
        inventory
            .acknowledge(&generation, &second, revision)
            .unwrap();
        assert!(!inventory.known_empty(first.stored_output_id, 1, second.time_range));
        inventory.seal_finite(&generation).unwrap();
        assert!(inventory.known_empty(first.stored_output_id, 1, second.time_range));
        assert!(!inventory.known_empty(first.stored_output_id, 2, second.time_range));
        assert!(!inventory.known_empty(first.stored_output_id, 999, second.time_range));
        assert!(!inventory.known_empty(
            first.stored_output_id,
            1,
            HalfOpenTimeRange {
                start_ms: 2000,
                end_ms: 3000
            }
        ));
        assert!(inventory
            .admit(&generation, BTreeSet::from([first]))
            .is_err());
    }

    #[test]
    fn metadata_budget_rejects_admission_without_partial_mutation() {
        let generation = generation(1);
        let mut inventory = AdmissionInventory::default();
        inventory.install(generation.clone());
        let before = inventory.revision();
        let mut coordinate = window("a");
        coordinate.group_values.insert(
            "large".into(),
            "x".repeat(AdmissionInventory::MAX_METADATA_BYTES),
        );
        assert!(inventory
            .admit(&generation, BTreeSet::from([window("b"), coordinate]))
            .is_err());
        assert!(inventory.windows.is_empty());
        assert_eq!(before, inventory.revision());
    }
}
