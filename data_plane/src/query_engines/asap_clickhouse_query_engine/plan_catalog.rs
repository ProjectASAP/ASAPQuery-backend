//! Generation-aware SQL plan lookup, kept separate from SDS.
//!
//! Entries own backend execution templates and only hold foreign keys to the
//! immutable SDS snapshot. Staging validates all foreign keys before a whole
//! generation can become visible to readers.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use asap_types::sds::{DataDescriptorId, SummaryDescriptorId};
use asap_types::summary_catalog::{SummaryCatalog, SummaryCatalogReference};
use xxhash_rust::xxh64::xxh64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SqlQueryFingerprint(String);

impl SqlQueryFingerprint {
    pub fn canonical(&self) -> &str {
        &self.0
    }
}

/// Stable lookup identity for the exact SQL template submitted at planning.
/// SQL parsing and semantic canonicalization remain ASAPPlanner's job; keeping
/// the text alongside the hash makes hash collisions fail closed.
pub fn fingerprint_sql(sql: &str) -> SqlQueryFingerprint {
    let key = control_plane::clickhouse::sql_request_template_identity(sql);
    SqlQueryFingerprint(format!(
        "clickhouse-sql:v1:{:016x}",
        xxh64(key.as_bytes(), 0)
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SdsDescriptorReferences {
    pub summaries: BTreeSet<SummaryDescriptorId>,
    pub data: BTreeSet<DataDescriptorId>,
}

impl SdsDescriptorReferences {
    pub fn empty() -> Self {
        Self {
            summaries: BTreeSet::new(),
            data: BTreeSet::new(),
        }
    }

    fn validate(&self, catalog: &SummaryCatalog) -> Result<(), SqlPlanCatalogError> {
        for id in &self.summaries {
            if !catalog.summary_descriptors.contains_key(id) {
                return Err(SqlPlanCatalogError::MissingSummaryDescriptor(
                    id.canonical().to_owned(),
                ));
            }
        }
        for id in &self.data {
            if !catalog.data_descriptors.contains_key(id) {
                return Err(SqlPlanCatalogError::MissingDataDescriptor(
                    id.canonical().to_owned(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SqlPlanEntry<P> {
    /// Collision guard and parameter-binding template identity.
    pub sql_template: String,
    /// Planner-produced execution DAG (or its runtime-owned compiled form).
    pub plan: P,
    pub descriptors: SdsDescriptorReferences,
}

impl<P> SqlPlanEntry<P> {
    pub fn fingerprint(&self) -> SqlQueryFingerprint {
        fingerprint_sql(&self.sql_template)
    }
}

#[derive(Debug, Clone)]
pub struct SqlPlanCatalogGeneration<P> {
    pub sds: SummaryCatalogReference,
    pub catalog: Arc<SummaryCatalog>,
    entries: BTreeMap<SqlQueryFingerprint, Arc<SqlPlanEntry<P>>>,
}

impl<P> SqlPlanCatalogGeneration<P> {
    pub fn build(
        sds: &SummaryCatalog,
        entries: impl IntoIterator<Item = SqlPlanEntry<P>>,
    ) -> Result<Self, SqlPlanCatalogError> {
        let sds_reference = sds
            .reference()
            .map_err(|error| SqlPlanCatalogError::InvalidSds(error.to_string()))?;
        let mut indexed = BTreeMap::new();
        for entry in entries {
            entry.descriptors.validate(sds)?;
            let fingerprint = entry.fingerprint();
            if indexed
                .insert(fingerprint.clone(), Arc::new(entry))
                .is_some()
            {
                return Err(SqlPlanCatalogError::DuplicateFingerprint(
                    fingerprint.canonical().to_owned(),
                ));
            }
        }
        Ok(Self {
            sds: sds_reference,
            catalog: Arc::new(sds.clone()),
            entries: indexed,
        })
    }

    pub fn lookup(&self, sql: &str) -> Option<Arc<SqlPlanEntry<P>>> {
        let normalized = control_plane::clickhouse::sql_request_template_identity(sql);
        self.entries
            .get(&fingerprint_sql(&normalized))
            .filter(|entry| {
                control_plane::clickhouse::sql_request_template_identity(&entry.sql_template)
                    == normalized
            })
            .cloned()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct SqlPlanCatalogAck {
    pub plan_id: u64,
    pub plan_version: u64,
    pub sds_snapshot_sha256: String,
    pub entry_count: usize,
    pub phase: SqlPlanCatalogPhase,
}

#[derive(Debug, Clone, Copy, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SqlPlanCatalogPhase {
    Staged,
    Active,
}

impl SqlPlanCatalogAck {
    fn for_generation<P>(
        generation: &SqlPlanCatalogGeneration<P>,
        phase: SqlPlanCatalogPhase,
    ) -> Self {
        Self {
            plan_id: generation.sds.plan_id,
            plan_version: generation.sds.plan_version,
            sds_snapshot_sha256: generation.sds.snapshot_sha256.clone(),
            entry_count: generation.len(),
            phase,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SqlPlanCatalogError {
    #[error("invalid SDS catalog: {0}")]
    InvalidSds(String),
    #[error("SQL plan references missing summary descriptor {0}")]
    MissingSummaryDescriptor(String),
    #[error("SQL plan references missing data descriptor {0}")]
    MissingDataDescriptor(String),
    #[error("duplicate SQL plan fingerprint {0}")]
    DuplicateFingerprint(String),
    #[error("another SQL plan generation is already staged")]
    CandidateAlreadyStaged,
    #[error("SQL plan generation {plan_id}/{plan_version} was not staged")]
    NotStaged { plan_id: u64, plan_version: u64 },
    #[error("SQL plan generation {candidate} is not newer than active generation {active}")]
    StaleGeneration { active: u64, candidate: u64 },
}

#[derive(Debug)]
struct CatalogState<P> {
    active: Option<Arc<SqlPlanCatalogGeneration<P>>>,
    staged: Option<Arc<SqlPlanCatalogGeneration<P>>>,
}

/// Two-phase publisher for SQL plans. A lookup snapshots one immutable active
/// generation, so readers cannot observe a mixture of SDS generations.
#[derive(Debug)]
pub struct SqlPlanCatalog<P> {
    state: RwLock<CatalogState<P>>,
}

impl<P> Default for SqlPlanCatalog<P> {
    fn default() -> Self {
        Self {
            state: RwLock::new(CatalogState {
                active: None,
                staged: None,
            }),
        }
    }
}

impl<P> SqlPlanCatalog<P> {
    pub fn stage(
        &self,
        generation: SqlPlanCatalogGeneration<P>,
    ) -> Result<SqlPlanCatalogAck, SqlPlanCatalogError> {
        let mut state = self.state.write().unwrap();
        if state.staged.is_some() {
            return Err(SqlPlanCatalogError::CandidateAlreadyStaged);
        }
        if let Some(active) = state.active.as_ref() {
            if generation.sds.plan_version <= active.sds.plan_version {
                return Err(SqlPlanCatalogError::StaleGeneration {
                    active: active.sds.plan_version,
                    candidate: generation.sds.plan_version,
                });
            }
        }
        let generation = Arc::new(generation);
        let ack =
            SqlPlanCatalogAck::for_generation(generation.as_ref(), SqlPlanCatalogPhase::Staged);
        state.staged = Some(generation);
        Ok(ack)
    }

    pub fn activate(
        &self,
        plan_id: u64,
        plan_version: u64,
    ) -> Result<SqlPlanCatalogAck, SqlPlanCatalogError> {
        let mut state = self.state.write().unwrap();
        let staged = state
            .staged
            .as_ref()
            .filter(|generation| {
                generation.sds.plan_id == plan_id && generation.sds.plan_version == plan_version
            })
            .cloned()
            .ok_or(SqlPlanCatalogError::NotStaged {
                plan_id,
                plan_version,
            })?;
        let ack = SqlPlanCatalogAck::for_generation(staged.as_ref(), SqlPlanCatalogPhase::Active);
        state.active = Some(staged);
        state.staged = None;
        Ok(ack)
    }

    pub fn active(&self) -> Option<Arc<SqlPlanCatalogGeneration<P>>> {
        self.state.read().unwrap().active.clone()
    }

    pub fn lookup(&self, sql: &str) -> Option<Arc<SqlPlanEntry<P>>> {
        self.active()?.lookup(sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};

    fn sds(plan_version: u64) -> SummaryCatalog {
        let config = PrecomputeMaterialization::new(
            AggregationType::Sum,
            String::new(),
            Default::default(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowKind::Tumbling,
            String::new(),
            "requests".into(),
            None,
            None,
            None,
        );
        SummaryCatalog::from_materializations(7, plan_version, &[config]).unwrap()
    }

    fn entry(catalog: &SummaryCatalog, sql: &str, plan: &str) -> SqlPlanEntry<String> {
        SqlPlanEntry {
            sql_template: sql.into(),
            plan: plan.into(),
            descriptors: SdsDescriptorReferences {
                summaries: catalog.summary_descriptors.keys().cloned().collect(),
                data: catalog.data_descriptors.keys().cloned().collect(),
            },
        }
    }

    // Publication validates foreign keys before exposing an atomic generation.
    #[test]
    fn validates_stages_activates_and_looks_up() {
        let sds = sds(3);
        let generation = SqlPlanCatalogGeneration::build(
            &sds,
            [entry(&sds, "SELECT sum(value) FROM requests", "dag")],
        )
        .unwrap();
        let catalog = SqlPlanCatalog::default();
        let staged = catalog.stage(generation).unwrap();
        assert_eq!((staged.plan_id, staged.plan_version), (7, 3));
        assert_eq!(staged.phase, SqlPlanCatalogPhase::Staged);
        assert!(catalog.lookup("SELECT sum(value) FROM requests").is_none());
        let activated = catalog.activate(7, 3).unwrap();
        assert_eq!(activated.entry_count, 1);
        assert_eq!(activated.phase, SqlPlanCatalogPhase::Active);
        assert_eq!(
            catalog
                .lookup("  SELECT sum(value) FROM requests  ")
                .unwrap()
                .plan,
            "dag"
        );
    }

    #[test]
    fn clickhouse_only_template_does_not_require_planner_parsing() {
        let sds = sds(3);
        let exact = "SELECT samples[1].1, arraySum(i -> samples[i].2, range(1, 3)) FROM raw";
        let generation =
            SqlPlanCatalogGeneration::build(&sds, [entry(&sds, exact, "dag")]).unwrap();
        assert_eq!(
            generation.lookup(&format!("  {exact};  ")).unwrap().plan,
            "dag"
        );
    }

    // A dangling descriptor reference rejects the whole candidate generation.
    #[test]
    fn rejects_missing_descriptor_and_stale_successor() {
        let sds = sds(3);
        let mut invalid = entry(&sds, "SELECT 1", "dag");
        invalid
            .descriptors
            .summaries
            .insert(serde_json::from_value(serde_json::json!("missing-summary")).unwrap());
        assert!(matches!(
            SqlPlanCatalogGeneration::build(&sds, [invalid]),
            Err(SqlPlanCatalogError::MissingSummaryDescriptor(_))
        ));

        let catalog = SqlPlanCatalog::default();
        catalog
            .stage(SqlPlanCatalogGeneration::build(&sds, [entry(&sds, "SELECT 1", "v3")]).unwrap())
            .unwrap();
        catalog.activate(7, 3).unwrap();
        assert_eq!(
            catalog.stage(
                SqlPlanCatalogGeneration::build(&sds, [entry(&sds, "SELECT 1", "another-v3")],)
                    .unwrap()
            ),
            Err(SqlPlanCatalogError::StaleGeneration {
                active: 3,
                candidate: 3
            })
        );
        assert_eq!(catalog.lookup("SELECT 1").unwrap().plan, "v3");
    }
}
