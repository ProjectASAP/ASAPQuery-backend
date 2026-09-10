use asap_frontend_sql::SqlCatalog;
use control_plane::physical::post_asap::bind_query_expr;
use planner_types::pre_asap::{Column, DataType, Schema};
use planner_types::types::AccuracyTarget;
use serde::Deserialize;
use serde_json::json;
use std::{fs, path::PathBuf};
#[derive(Deserialize)]
struct Corpus {
    queries: Vec<Row>,
}
#[derive(Deserialize)]
struct Row {
    id: String,
    metricsql: String,
    clickhouse_sql: Option<String>,
    clickhouse_planning_sql: Option<String>,
    clickhouse_planning_status: String,
}
#[tokio::main]
async fn main() {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("corpus path");
    let corpus: Corpus = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let schema = Schema::with_time_index(
        vec![
            Column::new("metric", DataType::Utf8, false),
            Column::new("labels", DataType::Utf8, false),
            Column::new("job", DataType::Utf8, false),
            Column::new("le", DataType::Utf8, false),
            Column::new("ts_ms", DataType::Timestamp, false),
            Column::new("value", DataType::Float64, false),
        ],
        4,
        // A raw sample row is unique by metric, complete label set, and time.
        // Projected labels such as job/le never replace series identity.
        vec![vec![0, 1, 4], vec![0, 1, 2, 4]],
    );
    let catalog = SqlCatalog::new().with_table("raw_samples", schema);
    let accuracy = AccuracyTarget::Epsilon(0.01);
    let mut rows = Vec::new();
    for row in corpus.queries {
        let ml = asap_frontend_metricsql::lower_metricsql(&row.metricsql, accuracy.clone());
        let (mcanon, mplan) = match ml {
            Ok(expr) => (
                json!({"status":"pass"}),
                match control_plane::physical::compiler::validate_metricsql_acceleration_shape(
                    &expr,
                )
                .and_then(|_| {
                    bind_query_expr(&expr, accuracy.clone())
                        .map(|_| ())
                        .map_err(|e| Box::leak(e.to_string().into_boxed_str()) as &str)
                }) {
                    Ok(_) => json!({"status":"pass"}),
                    Err(e) => json!({"status":"typed_fallback","reason":e}),
                },
            ),
            Err(e) => (
                json!({"status":"failed","reason":e.to_string()}),
                json!({"status":"not_reached"}),
            ),
        };
        let sql = match (&row.clickhouse_sql, &row.clickhouse_planning_sql) {
            (Some(_), Some(sql)) => match control_plane::clickhouse::plan_clickhouse_sql(
                &sql.replace("{eval_ms}", "1788891296000"),
                &catalog,
                accuracy.clone(),
            )
            .await
            {
                Ok(_) => json!({"parser_canonical":"pass","planner":"pass"}),
                Err(e) => json!({"status":"typed_fallback","reason":e.to_string()}),
            },
            (Some(_), None) => json!({"status":"typed_fallback","reason":row.clickhouse_planning_status}),
            (None, _) => json!({"status":"missing_mapping"}),
        };
        rows.push(json!({"id":row.id,"metricsql":{"parser_canonical":mcanon,"planner":mplan},"clickhouse_sql":sql}));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({"queries":rows})).unwrap()
    );
}
