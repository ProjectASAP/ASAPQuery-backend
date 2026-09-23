//! Execute a bounded in-memory batch through native operators. This is also the
//! bridge for deployments whose boundary values are not yet streaming batches.
use super::{operators::Operator, values::Batch, Error, PhysicalDag, RunContext};
use futures::{FutureExt, StreamExt};

/// Every input is already in memory; the chain contains native operators only.
/// This deliberately does not enter a nested executor when called from a DAG
/// adapter. I/O belongs to source operators in the surrounding execution.
pub fn evaluate_batch(
    input: Batch,
    operators: Vec<Operator>,
    context: RunContext,
) -> Result<Vec<Batch>, Error> {
    let mut graph = PhysicalDag::default();
    graph.add(
        0,
        vec![],
        Operator::source(input.schema().clone(), vec![input])?,
    )?;
    let mut root = 0;
    for operator in operators {
        graph.add(root + 1, vec![root], operator)?;
        root += 1;
    }
    let mut output = graph.execute(&[root], context)?.remove(0);
    let mut batches = Vec::new();
    loop {
        match output.next().now_or_never() {
            Some(Some(Ok(batch))) => batches.push(batch.value().clone()),
            Some(Some(Err(error))) => return Err(error),
            Some(None) => return Ok(batches),
            None => {
                return Err(Error::Operator(
                    "in-memory native batch chain unexpectedly awaited I/O".into(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{operators::Expression, values::Value, Limits, Scope};
    use planner_types::{
        post_asap::{SummaryFamilyType, SummaryField, SummarySchema},
        pre_asap::DataType,
    };
    use std::sync::Arc;

    // Engine adapters can run the identical native chain from an outer executor.
    #[test]
    fn same_native_chain_inside_query_and_ingestion_execution() {
        let schema = Arc::new(SummarySchema {
            fields: vec![SummaryField {
                name: "value".into(),
                dtype: SummaryFamilyType::Plain(DataType::Float64),
                nullable: false,
            }],
            time_index: None,
        });
        for scope in [
            Scope::Query {
                evaluation_time_ms: 20,
                revision: 1,
            },
            Scope::Ingestion {
                window_start_ms: 10,
                window_end_ms: 20,
                revision: 1,
            },
        ] {
            let batch = Batch::try_new(schema.clone(), vec![vec![Value::Float64(7.)]]).unwrap();
            let negate = Operator::project(
                schema.clone(),
                vec![(
                    "value".into(),
                    Expression::Negate(Box::new(Expression::Column(0))),
                )],
            )
            .unwrap();
            let context = RunContext::new(scope, Limits::default()).unwrap();
            let result =
                futures::executor::block_on(async { evaluate_batch(batch, vec![negate], context) })
                    .unwrap();
            assert!(matches!(result[0].rows()[0][0], Value::Float64(-7.)));
        }
    }

    // A cancelled surrounding execution also prevents its native computation.
    #[test]
    fn cancellation_is_not_bypassed_by_in_memory_execution() {
        let schema = Arc::new(SummarySchema {
            fields: vec![],
            time_index: None,
        });
        let batch = Batch::try_new(schema, vec![vec![]]).unwrap();
        let context = RunContext::new(
            Scope::Query {
                evaluation_time_ms: 0,
                revision: 0,
            },
            Limits::default(),
        )
        .unwrap();
        context.cancel();
        assert!(matches!(
            evaluate_batch(batch, vec![], context),
            Err(Error::Cancelled)
        ));
    }
}
