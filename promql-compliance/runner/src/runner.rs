use crate::{
    compare,
    compose::Compose,
    input::{self, Dataset, Policy, Suite, Tolerance},
    planning, sql, transport,
};
use anyhow::{ensure, Context, Result};
use clap::Parser;
use reqwest::Client;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Parser, Debug, Clone)]
pub struct Args {
    #[arg(long)]
    pub dataset: PathBuf,
    #[arg(long)]
    pub suite: PathBuf,
    #[arg(
        long = "reference-url",
        alias = "prometheus-url",
        default_value = "http://127.0.0.1:19090"
    )]
    pub reference: String,
    #[arg(
        long = "test-url",
        alias = "backend-url",
        default_value = "http://127.0.0.1:19091"
    )]
    pub backend: String,
    #[arg(long, default_value = "http://127.0.0.1:18428")]
    pub victoria_url: String,
    #[arg(long, default_value = "http://127.0.0.1:18123")]
    pub clickhouse_url: String,
    #[arg(long, default_value = "compliance-report.json")]
    pub output: PathBuf,
    #[arg(long)]
    pub compose_file: Vec<PathBuf>,
    #[arg(long, default_value = "asapquery-rust-compliance")]
    pub compose_project: String,
    #[arg(long, default_value = "compliance-logs")]
    pub logs_dir: PathBuf,
    #[arg(long)]
    pub keep_services: bool,
    #[arg(long)]
    pub base_time_ms: Option<i64>,
    #[arg(long, default_value_t = 3)]
    pub warmups: usize,
    #[arg(long, default_value_t = 10)]
    pub trials: usize,
    /// Mutate fixture quotes and execute every admitted candidate for each query and the full ensemble.
    #[arg(long)]
    pub all_candidates: bool,
}
pub fn write_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}
pub async fn run(args: Args, benefit: bool) -> Result<()> {
    let result = if args.all_candidates {
        ensure!(
            !benefit && !args.keep_services && !args.compose_file.is_empty(),
            "candidate sweep requires isolated Compose execution without benefit/keep-services"
        );
        execute_candidates(&args).await
    } else {
        execute(&args, benefit).await
    };
    match result {
        Ok(report) => {
            write_json(&args.output, &report)?;
            ensure!(
                report["passed"] == true,
                "acceptance failed; inspect {}",
                args.output.display()
            );
            Ok(())
        }
        Err(e) => {
            write_json(
                &args.output,
                &json!({"passed":false,"benefitPassed":false,"dataset":args.dataset,"suite":args.suite,"error":format!("{e:#}")}),
            )?;
            Err(e)
        }
    }
}
async fn execute(args: &Args, benefit: bool) -> Result<Value> {
    let data = Dataset::load(&args.dataset)?;
    let suite = Suite::load(&args.suite)?;
    if benefit {
        ensure!(
            suite.queries.len() == 10
                && suite
                    .queries
                    .iter()
                    .all(|q| !q.instant_offsets_seconds.is_empty())
                && args.trials >= 2
                && !args.compose_file.is_empty(),
            "benefit needs ten shared instant cases, two trials and Compose"
        );
    }
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
    let base = args.base_time_ms.unwrap_or(now as i64 - 1_800_000);
    let snapshot = planning::snapshot(&suite, &data, now, base, benefit)
        .context("compile unquoted workload snapshot")?;
    let snapshot = if benefit {
        snapshot
    } else {
        planning::with_fixture_costs(snapshot)?
    };
    let snapshot_path = args.output.with_extension("snapshot.json");
    write_json(&snapshot_path, &snapshot)?;
    // Compile before starting services. Preserve real planning failures without
    // spending a container build or substituting a hand-selected candidate.
    let plan = snapshot
        .compile_promql()
        .context("compile unquoted workload snapshot")?;
    let plan_path = args.output.with_extension("plan.json");
    write_json(&plan_path, &plan)?;
    if benefit {
        planning::validate_cost(&plan).context("historical automatic cost gate")?;
    } else {
        planning::validate_fixture_cost(&plan).context("synthetic workload cost gate")?;
    }
    planning::validate_local(&plan).context("ASAP-local plan gate")?;
    execute_installed(args, benefit, &suite, &data, base, plan).await
}

async fn execute_installed(
    args: &Args,
    benefit: bool,
    suite: &Suite,
    data: &Dataset,
    base: i64,
    plan: control_plane::physical::compiler::CompiledPhysicalPlan,
) -> Result<Value> {
    let plan_path = args.output.with_extension("plan.json");
    let installation_path = args.output.with_extension("install.json");
    write_json(&installation_path, &planning::installation(plan))?;
    let installation_path = std::fs::canonicalize(installation_path)?;
    let mut compose = Compose::new(
        args.compose_file.clone(),
        args.compose_project.clone(),
        installation_path,
        args.logs_dir.clone(),
        args.keep_services,
    );
    compose.start(benefit)?;
    let client = transport::client()?;
    for url in [
        format!("{}/api/v1/status/runtimeinfo", args.reference),
        format!("{}/api/v1/health", args.backend),
    ] {
        transport::wait(&client, &url).await?;
    }
    let body = transport::encode(data, base)?;
    let mut targets = vec![args.reference.as_str(), args.backend.as_str()];
    if benefit {
        transport::wait(&client, &format!("{}/health", args.victoria_url)).await?;
        transport::wait(&client, &format!("{}/ping", args.clickhouse_url)).await?;
        targets.push(&args.victoria_url);
    }
    transport::push(&client, &body, &targets).await?;
    transport::drain(&client, &args.backend).await?;
    let semantic = compare_suite(
        &client,
        &args.reference,
        &args.backend,
        suite,
        &data.name,
        base,
    )
    .await;
    if !benefit {
        compose.finish()?;
        return Ok(semantic);
    }
    write_json(&args.output.with_extension("semantic.json"), &semantic)?;
    ensure!(
        semantic["passed"] == true,
        "semantic gate failed; inspect semantic report"
    );
    sql::seed(&client, &args.clickhouse_url, data, base).await?;
    verify_baselines(&client, args, suite, data, base).await?;
    let report = measure(&client, args, suite, data, &compose, base, &plan_path).await?;
    compose.finish()?;
    Ok(report)
}
fn response_result(result: Result<Value>) -> Value {
    result.unwrap_or_else(|e| json!({"status":"error","error":format!("{e:#}")}))
}
fn backend_compare(reference: &Value, backend: &Value, policy: &Policy) -> Result<()> {
    compare::compare(reference, backend, policy)?;
    ensure!(
        backend["servedBy"]
            .as_str()
            .is_some_and(|s| s == "asap_query"),
        "backend response lacks ASAP provenance"
    );
    Ok(())
}
pub async fn compare_suite(
    client: &Client,
    reference: &str,
    backend: &str,
    suite: &Suite,
    dataset: &str,
    base: i64,
) -> Value {
    let mut results = vec![];
    let mut passed = true;
    for q in &suite.queries {
        let policy = q.policy(&suite.comparison_defaults);
        let mut report = json!({"name":q.name,"expr":q.expr,"tolerance":policy,"passed":true,"instant":[],"referenceParity":[],"backendParity":[]});
        let ranges = if let Some(r) = &q.range {
            let a = response_result(
                transport::query(client, reference, &q.expr, base, Some(r), false).await,
            );
            let b = response_result(
                transport::query(client, backend, &q.expr, base, Some(r), true).await,
            );
            let outcome = compare::outcome(backend_compare(&a, &b, &policy));
            report["passed"] = outcome["passed"].clone();
            report["range"] = outcome;
            report["rangeResponses"] = json!({"reference":a,"backend":b});
            Some((a, b))
        } else {
            None
        };
        for offset in &q.instant_offsets_seconds {
            let at = match input::at_ms(base, *offset) {
                Ok(at) => at,
                Err(e) => {
                    report["passed"] = json!(false);
                    report["error"] = json!(e.to_string());
                    continue;
                }
            };
            let a = response_result(
                transport::query(client, reference, &q.expr, at, None, false).await,
            );
            let b =
                response_result(transport::query(client, backend, &q.expr, at, None, true).await);
            let outcome = compare::outcome(backend_compare(&a, &b, &policy));
            report["passed"] = json!(report["passed"] == true && outcome["passed"] == true);
            report["instant"].as_array_mut().unwrap().push(json!({"offsetSeconds":offset,"timeMs":at,"comparison":outcome,"responses":{"reference":a,"backend":b}}));
            if let Some((ra, rb)) = &ranges {
                for (name, range, instant) in
                    [("referenceParity", ra, &a), ("backendParity", rb, &b)]
                {
                    let p = compare::outcome(compare::parity(range, instant, at, &policy));
                    report["passed"] = json!(report["passed"] == true && p["passed"] == true);
                    report[name]
                        .as_array_mut()
                        .unwrap()
                        .push(json!({"offsetSeconds":offset,"timeMs":at,"comparison":p}));
                }
            }
        }
        passed &= report["passed"] == true;
        results.push(report);
    }
    json!({"suite":suite.name,"dataset":dataset,"baseTimeMs":base,"queries":results,"passed":passed})
}
async fn verify_baselines(
    client: &Client,
    args: &Args,
    suite: &Suite,
    data: &Dataset,
    base: i64,
) -> Result<()> {
    let exact = Policy {
        value_tolerance: Some(Tolerance {
            absolute: Some(1e-8),
            relative: Some(1e-8),
        }),
    };
    for q in &suite.queries {
        let at = input::at_ms(base, q.instant_offsets_seconds[0])?;
        let prom = transport::query(client, &args.reference, &q.expr, at, None, false).await?;
        let vm = transport::query(client, &args.victoria_url, &q.expr, at, None, false).await?;
        compare::compare(&prom, &vm, &q.policy(&suite.comparison_defaults))
            .with_context(|| format!("VictoriaMetrics {}", q.name))?;
        let sql = sql::baseline(&q.name, at, sql::window_ms(&q.expr)?)?;
        let rows = sql::rows(client, &args.clickhouse_url, &sql).await?;
        let mut values = vec![];
        for row in rows {
            let labels = if let Some(id) = row["series_id"]
                .as_u64()
                .or_else(|| row["series_id"].as_str().and_then(|s| s.parse().ok()))
            {
                data.series
                    .get(id.checked_sub(1).context("invalid SQL series id")? as usize)
                    .context("unknown SQL series id")?
                    .labels
                    .clone()
            } else {
                BTreeMap::from([(
                    "label_0".into(),
                    row["label_0"]
                        .as_str()
                        .context("SQL row has no group")?
                        .to_owned(),
                )])
            };
            let number = row["value"].as_f64().context("non-numeric SQL value")?;
            values.push(json!({"metric":labels,"value":[at as f64/1000.,number.to_string()]}));
        }
        let mut prom = prom;
        // SQL series IDs resolve to complete source labels; metric-name
        // retention differs between native SQL and PromQL functions.
        for row in prom["data"]["result"]
            .as_array_mut()
            .context("expected PromQL vector")?
        {
            row["metric"]
                .as_object_mut()
                .context("invalid labels")?
                .remove("__name__");
        }
        let sql_response =
            json!({"status":"success","data":{"resultType":"vector","result":values}});
        compare::compare(&prom, &sql_response, &exact)
            .with_context(|| format!("ClickHouse {}", q.name))?;
    }
    Ok(())
}
async fn execute_query(
    client: &Client,
    args: &Args,
    name: &str,
    q: &input::Query,
    at: i64,
) -> Result<()> {
    if name == "clickhouse" {
        sql::rows(
            client,
            &args.clickhouse_url,
            &sql::baseline(&q.name, at, sql::window_ms(&q.expr)?)?,
        )
        .await?;
    } else {
        let url = match name {
            "backend" => &args.backend,
            "victoria" => &args.victoria_url,
            _ => &args.reference,
        };
        let response = transport::query(client, url, &q.expr, at, None, name == "backend").await?;
        ensure!(
            response["status"] == "success",
            "measurement query failed: {response}"
        );
        if name == "backend" {
            ensure!(
                response["servedBy"]
                    .as_str()
                    .is_some_and(|s| s == "asap_query"),
                "fallback during performance measurement"
            );
        }
    }
    Ok(())
}
pub fn percentile(samples: &[f64], p: f64) -> Result<f64> {
    ensure!(
        !samples.is_empty()
            && samples.iter().all(|n| n.is_finite() && *n >= 0.)
            && p > 0.
            && p <= 1.,
        "invalid latency samples"
    );
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    Ok(sorted[(p * sorted.len() as f64).ceil() as usize - 1])
}
async fn measure(
    client: &Client,
    args: &Args,
    suite: &Suite,
    data: &Dataset,
    compose: &Compose,
    base: i64,
    plan: &Path,
) -> Result<Value> {
    let mut targets = serde_json::Map::new();
    for (name, service) in [
        ("backend", "data-plane"),
        ("prometheus", "prometheus"),
        ("victoria", "victoria"),
        ("clickhouse", "clickhouse"),
    ] {
        for q in &suite.queries {
            for _ in 0..args.warmups {
                execute_query(
                    client,
                    args,
                    name,
                    q,
                    input::at_ms(base, q.instant_offsets_seconds[0])?,
                )
                .await?;
            }
        }
        let before = compose.usage(service)?;
        let mut queries = serde_json::Map::new();
        for q in &suite.queries {
            let mut samples = vec![];
            let at = input::at_ms(base, q.instant_offsets_seconds[0])?;
            for _ in 0..args.trials {
                let start = Instant::now();
                execute_query(client, args, name, q, at).await?;
                samples.push(start.elapsed().as_secs_f64() * 1000.);
            }
            queries.insert(q.name.clone(),json!({"p50Ms":percentile(&samples,0.5)?,"p95Ms":percentile(&samples,0.95)?,"samplesMs":samples}));
        }
        let after = compose.usage(service)?;
        let cpu = after
            .0
            .checked_sub(before.0)
            .context("CPU counter reset during measurement")?;
        targets.insert(
            name.into(),
            json!({"queries":queries,"cpuUsec":cpu,"memoryPeakBytes":after.1}),
        );
    }
    let failures = benefit_failures(&Value::Object(targets.clone()), suite)?;
    Ok(
        json!({"suite":suite.name,"dataset":data.name,"baseTimeMs":base,"selectedPlan":plan,"warmups":args.warmups,"trials":args.trials,"targets":targets,"failures":failures,"passed":failures.is_empty(),"benefitPassed":failures.is_empty()}),
    )
}
pub fn benefit_failures(targets: &Value, suite: &Suite) -> Result<Vec<String>> {
    let mut failures = vec![];
    for name in ["prometheus", "victoria", "clickhouse"] {
        for metric in ["cpuUsec", "memoryPeakBytes"] {
            let backend = targets["backend"][metric]
                .as_u64()
                .context("missing backend usage")?;
            let baseline = targets[name][metric]
                .as_u64()
                .context("missing baseline usage")?;
            if backend >= baseline {
                failures.push(format!("backend {metric} >= {name}"));
            }
        }
        for q in &suite.queries {
            let backend = targets["backend"]["queries"][&q.name]["p95Ms"]
                .as_f64()
                .context("missing backend latency")?;
            let baseline = targets[name]["queries"][&q.name]["p95Ms"]
                .as_f64()
                .context("missing baseline latency")?;
            ensure!(
                backend.is_finite() && baseline.is_finite() && backend >= 0. && baseline >= 0.,
                "invalid latency"
            );
            if backend >= baseline {
                failures.push(format!("{} backend p95 >= {name}", q.name));
            }
        }
    }
    Ok(failures)
}
pub fn report_card(directory: &Path) -> Result<()> {
    let mut cases = vec![];
    let mut passed = true;
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        let filename = path.file_name().unwrap().to_string_lossy();
        if path.extension().is_none_or(|s| s != "json")
            || filename == "summary.json"
            || [
                ".snapshot.json",
                ".plan.json",
                ".install.json",
                ".semantic.json",
            ]
            .iter()
            .any(|suffix| filename.ends_with(suffix))
        {
            continue;
        }
        let report: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        ensure!(
            report.get("passed").and_then(Value::as_bool).is_some(),
            "not a compliance report"
        );
        passed &= report["passed"] == true;
        let queries = report["queries"].as_array().cloned().unwrap_or_default();
        let (mut local, mut fallback) = (0, 0);
        for q in &queries {
            let responses = std::iter::once(&q["rangeResponses"]["backend"]).chain(
                q["instant"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|i| &i["responses"]["backend"]),
            );
            for response in responses.filter(|r| !r.is_null()) {
                if response["servedBy"]
                    .as_str()
                    .is_some_and(|s| s == "asap_query")
                {
                    local += 1
                } else {
                    fallback += 1
                }
            }
        }
        cases.push(json!({"dataset":report["dataset"],"suite":report["suite"],"passed":report["passed"],"queries":queries.len(),"asapQuery":local,"prometheusFallback":fallback}));
    }
    ensure!(!cases.is_empty(), "no comparison reports");
    cases.sort_by_key(|c| c["dataset"].to_string());
    let mut markdown=format!("# PromQL compliance report card\n\nOverall: **{passed}**\n\n| Dataset | Suite | Passed | Queries | ASAPQuery | Fallback |\n|---|---|---|---:|---:|---:|\n");
    for c in &cases {
        markdown.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            c["dataset"],
            c["suite"],
            c["passed"],
            c["queries"],
            c["asapQuery"],
            c["prometheusFallback"]
        ));
    }
    write_json(
        &directory.join("summary.json"),
        &json!({"cases":cases,"passed":passed}),
    )?;
    std::fs::write(directory.join("summary.md"), markdown)?;
    Ok(())
}

/// Exercise each admitted physical workload by changing quotes, never by
/// replacing the selected DAG after the production selector has run.
async fn execute_candidates(args: &Args) -> Result<Value> {
    let data = Dataset::load(&args.dataset)?;
    let suite = Suite::load(&args.suite)?;
    let mut scopes: Vec<(String, Suite)> = suite
        .queries
        .iter()
        .map(|query| {
            (
                format!("query:{}", query.name),
                Suite {
                    queries: vec![query.clone()],
                    ..suite.clone()
                },
            )
        })
        .collect();
    for (name, names) in [
        (
            "shared-rate",
            vec!["temporal-rate", "grouped-rate", "topk-rate"],
        ),
        (
            "shared-quantiles",
            vec!["temporal-quantile", "quantile-ratio"],
        ),
    ] {
        let queries: Vec<_> = suite
            .queries
            .iter()
            .filter(|q| names.contains(&q.name.as_str()))
            .cloned()
            .collect();
        if queries.len() == names.len() {
            scopes.push((
                name.into(),
                Suite {
                    queries,
                    ..suite.clone()
                },
            ));
        }
    }
    if suite.queries.len() > 1 {
        scopes.push(("full-ensemble".into(), suite));
    }
    let mut runs = Vec::new();
    let mut inventories = Vec::new();
    let mut results = Vec::new();
    let mut passed = true;
    for (scope_index, (name, suite)) in scopes.into_iter().enumerate() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        let base = args.base_time_ms.unwrap_or(now as i64 - 1_800_000);
        let input =
            planning::with_fixture_costs(planning::snapshot(&suite, &data, now, base, false)?)?;
        let baseline = input.clone().compile_promql()?;
        planning::validate_fixture_cost(&baseline)?;
        let report = baseline.cost_comparison.context("missing inventory")?;
        let targets = planning::executable_fixture_targets(&input, &report)?;
        inventories.push(json!({"scope":name,"queries":suite.queries.iter().map(|q| &q.expr).collect::<Vec<_>>(),
            "candidateEvaluations": report.candidate_evaluations,"executableCandidates":targets.len(),
            "searchScope": report.materialization_search_coverage}));
        for (candidate_index, target) in targets.into_iter().enumerate() {
            let mut trial = args.clone();
            let directory = args
                .output
                .with_extension("")
                .join(format!("scope-{scope_index}"));
            trial.output = directory.join(format!("candidate-{candidate_index}.json"));
            trial.logs_dir = args
                .logs_dir
                .join(format!("scope-{scope_index}-candidate-{candidate_index}"));
            trial.compose_project =
                format!("{}-s{scope_index}-c{candidate_index}", args.compose_project);
            eprintln!(
                "Executing {name} candidate {candidate_index}: plan {}",
                target.plan_id
            );
            let trial_result: Result<Value> = async {
                let priced = planning::prefer_fixture_candidate(input.clone(), &target)?;
                write_json(&trial.output.with_extension("snapshot.json"), &priced)?;
                let selected = priced.compile_promql()?;
                planning::validate_fixture_cost(&selected)?;
                planning::validate_local(&selected)?;
                ensure!(
                    selected
                        .cost_comparison
                        .as_ref()
                        .context("missing selected report")?
                        .selected_manifest
                        == target,
                    "synthetic prices did not select the intended candidate"
                );
                write_json(&trial.output.with_extension("plan.json"), &selected)?;
                execute_installed(&trial, false, &suite, &data, base, selected).await
            }
            .await;
            let result = match trial_result {
                Ok(result) => result,
                Err(error) => json!({"passed":false,"error":format!("{error:#}")}),
            };
            passed &= result["passed"] == true;
            write_json(&trial.output, &result)?;
            if let Some(queries) = result["queries"].as_array() {
                results.extend(queries.iter().cloned().map(|mut query| {
                    query["candidatePlanId"] = json!(target.plan_id);
                    query["scope"] = json!(name);
                    query
                }));
            }
            runs.push(
                json!({"scope":name,"candidatePlanId":target.plan_id,"passed":result["passed"],
                "report":trial.output,"error":result.get("error")}),
            );
            // Persist partial coverage so an interrupted run never looks complete.
            write_json(
                &args.output,
                &json!({"passed":false,"complete":false,"inventories":inventories,"candidateRuns":runs}),
            )?;
        }
    }
    ensure!(!runs.is_empty(), "no executable candidate trials");
    Ok(
        json!({"passed":passed,"complete":true,"dataset":data.name,"inventories":inventories,
        "candidateRuns":runs,"queries":results,"costModel":planning::FIXTURE_COST_MODEL}),
    )
}
