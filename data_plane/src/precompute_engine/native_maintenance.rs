//! Bind a complete durable counter cohort to a Planner-owned maintenance graph.
use std::{collections::BTreeMap, sync::Arc};

use asap_physical_operators::{
    operators::Operator,
    physical_planner::{CompiledPhysicalDag, Source},
    runtime::{Limits, RunContext, Scope},
    values::{Batch, Value},
};
use asap_types::executable_plan::{BackendNodeBinding, InstalledPostAsapDag};
use futures::{executor::block_on, StreamExt};
use planner_types::{
    post_asap::{PostAsapNodeId, SummaryFamilyType},
    pre_asap::DataType,
};

#[tracing::instrument(level = "debug", target = "asap_runtime_debug", skip_all,
    fields(stage = "physical.maintenance.execute", query_id = %installed.document.query_id, window_start_ms = window.0, window_end_ms = window.1, max_bytes, input_populations = inputs.len()), err)]
pub(super) fn execute(
    installed: &InstalledPostAsapDag,
    program: &CompiledPhysicalDag,
    inputs: &[crate::storage_engines::sketch_db::index::FrozenExactWindows],
    window: (u64, u64),
    max_bytes: usize,
) -> Result<Batch, String> {
    let mut sources = BTreeMap::new();
    let mut input_bytes = 0usize;
    for (id, contract) in program.input_contracts() {
        let node = PostAsapNodeId(u32::try_from(id).map_err(|_| "native source id overflow")?);
        let Some(BackendNodeBinding::Materialization { stored_output }) =
            installed.binding.node(node)
        else {
            return Err("native maintenance input has no stored binding".into());
        };
        let mut rows = Vec::new();
        for input in inputs
            .iter()
            .filter(|input| input.stored_output_reference.stored_output_id == *stored_output)
        {
            let state = input
                .windows
                .get(&window)
                .ok_or("native maintenance requires an exact complete counter window")?;
            let row = contract
                .schema
                .fields
                .iter()
                .map(|field| match &field.dtype {
                    SummaryFamilyType::ExactAggregate(
                        planner_types::post_asap::ExactKind::Rate,
                        _,
                    ) => Ok(Value::Summary {
                        family: field.dtype.clone(),
                        state: Arc::clone(state),
                    }),
                    SummaryFamilyType::Plain(DataType::Timestamp) => Ok(Value::Timestamp(
                        i64::try_from(window.1).map_err(|_| "native window overflow")?,
                    )),
                    SummaryFamilyType::Plain(DataType::Utf8)
                        if field.name == "$promql_series_identity" =>
                    {
                        Ok(Value::Utf8(
                            serde_json::to_string(&input.group)
                                .map_err(|e| e.to_string())?
                                .into(),
                        ))
                    }
                    SummaryFamilyType::Plain(DataType::Utf8) => Ok(Value::Utf8(
                        input
                            .group
                            .get(&field.name)
                            .cloned()
                            .unwrap_or_default()
                            .into(),
                    )),
                    _ => Err("unsupported native maintenance stored input field".to_string()),
                })
                .collect::<Result<Vec<_>, String>>()?;
            rows.push(row);
        }
        let batch = Batch::try_new(contract.schema.clone(), rows).map_err(|e| e.to_string())?;
        input_bytes = input_bytes
            .checked_add(batch.bytes())
            .ok_or("native input size overflow")?;
        if input_bytes > max_bytes {
            return Err("native maintenance input exceeds run budget".into());
        }
        sources.insert(
            id,
            Box::new(
                Operator::source(contract.schema.clone(), vec![batch])
                    .map_err(|e| e.to_string())?,
            ) as Source<'_>,
        );
    }
    let graph = program.instantiate(sources).map_err(|e| e.to_string())?;
    let context = RunContext::new(
        Scope::Ingestion {
            window_start_ms: i64::try_from(window.0).map_err(|_| "native window overflow")?,
            window_end_ms: i64::try_from(window.1).map_err(|_| "native window overflow")?,
            revision: 0,
        },
        Limits {
            max_bytes,
            ..Limits::default()
        },
    )
    .map_err(|e| e.to_string())?;
    let schema = program
        .output_contract(program.roots()[0])
        .map_err(|e| e.to_string())?
        .schema;
    let mut stream = graph
        .execute(program.roots(), context)
        .map_err(|e| e.to_string())?
        .remove(0);
    block_on(async {
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| e.to_string())?;
            bytes = bytes
                .checked_add(batch.bytes())
                .ok_or("native output size overflow")?;
            if bytes > max_bytes {
                return Err("native maintenance output exceeds publication budget".into());
            }
            rows.extend(batch.rows().iter().cloned());
        }
        Batch::try_new(schema, rows).map_err(|e| e.to_string())
    })
}
