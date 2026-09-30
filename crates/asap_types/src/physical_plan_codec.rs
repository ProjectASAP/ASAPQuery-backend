//! Persisted byte format of Planner physical plans.
//!
//! Planner exposes physical plans through serde only; the deployment owns the
//! versioned envelope. These formats are unchanged from when Planner encoded
//! them, so published plans and snapshots remain readable.
use asap_physical_operators::physical_planner::{
    CompiledPhysicalDag, InputContract, PhysicalCandidate,
};
use asap_physical_operators::{plan::NodeId, Error};
use serde_json::Value;
use std::collections::BTreeMap;

const DAG_VERSION: u64 = 2;
const CANDIDATE_VERSION: u64 = 1;

pub trait PhysicalPlanCodec: Sized {
    fn encode(&self) -> Result<Vec<u8>, Error>;
    fn decode(bytes: &[u8]) -> Result<Self, Error>;
}

fn invalid(error: impl ToString) -> Error {
    Error::Invalid(error.to_string())
}

fn unversioned(version: u64, value: Value, format: &str) -> Result<Value, Error> {
    let Value::Object(mut fields) = value else {
        return Err(invalid(format!("unsupported {format} format")));
    };
    if fields.remove("version").and_then(|v| v.as_u64()) != Some(version) {
        return Err(invalid(format!("unsupported {format} format")));
    }
    Ok(Value::Object(fields))
}

#[derive(serde::Serialize)]
struct StoredDag<'a> {
    version: u64,
    #[serde(flatten)]
    dag: &'a CompiledPhysicalDag,
}

#[derive(serde::Serialize)]
struct StoredCandidate<'a> {
    version: u64,
    precompute: Option<Value>,
    query: Value,
    materialized_outputs: &'a BTreeMap<NodeId, InputContract>,
}

fn dag_from_value(value: Value) -> Result<CompiledPhysicalDag, Error> {
    serde_json::from_value(unversioned(DAG_VERSION, value, "physical plan")?).map_err(invalid)
}

impl PhysicalPlanCodec for CompiledPhysicalDag {
    /// Persist selected operators and input slots, never live state.
    fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let bytes = serde_json::to_vec(&StoredDag {
            version: DAG_VERSION,
            dag: self,
        })
        .map_err(invalid)?;
        // JSON cannot preserve non-finite literal values. Fail at publication,
        // rather than persisting a document that cannot be recovered.
        Self::decode(&bytes)?;
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        dag_from_value(serde_json::from_slice(bytes).map_err(invalid)?)
    }
}

impl PhysicalPlanCodec for PhysicalCandidate {
    fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let graph = |dag: &CompiledPhysicalDag| -> Result<Value, Error> {
            serde_json::from_slice(&dag.encode()?).map_err(invalid)
        };
        serde_json::to_vec(&StoredCandidate {
            version: CANDIDATE_VERSION,
            precompute: self.precompute.as_ref().map(graph).transpose()?,
            query: graph(&self.query)?,
            materialized_outputs: &self.materialized_outputs,
        })
        .map_err(invalid)
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let Value::Object(mut fields) = unversioned(
            CANDIDATE_VERSION,
            serde_json::from_slice(bytes).map_err(invalid)?,
            "physical candidate",
        )?
        else {
            unreachable!("unversioned returns an object");
        };
        for key in ["precompute", "query"] {
            if let Some(graph) = fields.remove(key) {
                let graph = match graph {
                    Value::Null => Value::Null,
                    graph => serde_json::to_value(dag_from_value(graph)?).map_err(invalid)?,
                };
                fields.insert(key.into(), graph);
            }
        }
        serde_json::from_value(Value::Object(fields)).map_err(invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::post_asap::{ExactKind, ExactParams, SummaryFamilyType};

    fn dag() -> CompiledPhysicalDag {
        let family = SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum);
        let schema =
            asap_physical_operators::physical_planner::precompute::population_schema(family);
        CompiledPhysicalDag::from_operators(
            BTreeMap::from([(1, InputContract::bounded(schema))]),
            BTreeMap::new(),
            vec![1],
        )
        .unwrap()
    }

    // A plan keeps its versioned envelope and round-trips unchanged.
    #[test]
    fn physical_dag_roundtrips_in_versioned_envelope() {
        let bytes = dag().encode().unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["version"], 2);
        assert_eq!(
            CompiledPhysicalDag::decode(&bytes)
                .unwrap()
                .encode()
                .unwrap(),
            bytes
        );
        let mut other = value.clone();
        other["version"] = 3.into();
        assert!(CompiledPhysicalDag::decode(&serde_json::to_vec(&other).unwrap()).is_err());
        let mut unknown = value;
        unknown["extra"] = 1.into();
        assert!(CompiledPhysicalDag::decode(&serde_json::to_vec(&unknown).unwrap()).is_err());
    }

    // A candidate nests versioned plans and rejects other candidate versions.
    #[test]
    fn physical_candidate_roundtrips_with_nested_plans() {
        let candidate = PhysicalCandidate {
            precompute: None,
            query: dag(),
            materialized_outputs: BTreeMap::new(),
        };
        let bytes = candidate.encode().unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["version"], 1);
        assert_eq!(value["query"]["version"], 2);
        assert_eq!(
            PhysicalCandidate::decode(&bytes).unwrap().encode().unwrap(),
            bytes
        );
        let mut other = value;
        other["version"] = 2.into();
        assert!(PhysicalCandidate::decode(&serde_json::to_vec(&other).unwrap()).is_err());
    }
}
