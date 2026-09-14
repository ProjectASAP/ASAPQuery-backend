#!/usr/bin/env python3
"""Run small, explicit fixtures for every Prometheus 3.5 aggregation and rollup."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
FEATURE = "promql-experimental-functions"
SPECIAL_YAML = {"NaN": ".nan", "+Inf": ".inf", "-Inf": "-.inf"}


def selector(labels):
    name = labels.get("__name__", "")
    rest = ",".join(
        f"{key}={json.dumps(value)}"
        for key, value in sorted(labels.items()) if key != "__name__"
    )
    return name + "{" + rest + "}"


def save_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def check_coverage(cases, catalog, verify_source=False):
    """Fail if any registered function is missing, even if all present tests pass."""
    if cases["prometheus_version"] != catalog["prometheus_version"]:
        raise ValueError("Fixture and catalog versions differ")
    ids = [query["id"] for query in cases["queries"]]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate case IDs")
    report = {"prometheus_version": catalog["prometheus_version"], "scope": catalog["scope"]}
    for category in ("aggregation", "rollup"):
        entries = {entry["name"]: entry for entry in catalog[category]}
        covered = {}
        for query in cases["queries"]:
            if query["category"] != category:
                continue
            name = query["function"]
            if name not in entries:
                raise ValueError(f"Unregistered {category}: {name}")
            if query["experimental"] != entries[name]["experimental"]:
                raise ValueError(f"Incorrect experimental flag: {query['id']}")
            if not re.search(rf"\b{re.escape(name)}\s*(?:\(|by\b|without\b)", query["expr"]):
                raise ValueError(f"Case does not exercise its declared function: {query['id']}")
            covered.setdefault(name, []).append(query["id"])
        missing = sorted(entries.keys() - covered.keys())
        if missing:
            raise ValueError(f"Missing {category} coverage: {missing}")
        report[category] = {"covered": len(covered), "total": len(entries), "cases": covered}
    report["source_verified"] = False
    if verify_source:
        texts = {}
        for name, source in catalog["sources"].items():
            with urllib.request.urlopen(source["url"], timeout=30) as response:
                raw = response.read()
            actual = hashlib.sha256(raw).hexdigest()
            if actual != source["sha256"]:
                raise ValueError(f"Upstream source hash changed: {source['url']}")
            texts[name] = raw.decode()
        blocks = re.findall(r'\n\t"([^"]+)": \{(.*?)\n\t\},', texts["functions.go"], re.S)
        rollups = {
            name: bool(re.search(r"Experimental:\s*true", body))
            for name, body in blocks if "ValueTypeMatrix" in body
        }
        block = texts["lex.go"].split("// Aggregators.")[1].split("// Keywords.")[0]
        aggregates = {
            name: name in ("limitk", "limit_ratio")
            for name in re.findall(r'"([^"]+)":', block)
        }
        for category, actual in (("aggregation", aggregates), ("rollup", rollups)):
            expected = {entry["name"]: entry["experimental"] for entry in catalog[category]}
            if actual != expected:
                raise ValueError(f"Catalog does not match official {category} registry")
        report["source_verified"] = True
    report["case_count"] = len(cases["queries"])
    report["experimental_case_count"] = sum(q["experimental"] for q in cases["queries"])
    return report


def expected_samples(query, start=0):
    return [
        {"labels": sample["labels"], "value": (
            sample["value"] + start if query.get("value_is_timestamp") else sample["value"]
        )}
        for sample in query["expected"]
    ]


def generate(cases, out):
    lines = []
    previous_metric = None
    count = 0
    for series in cases["series"]:
        metric = series["labels"]["__name__"]
        if metric != previous_metric:
            family, kind = (metric[:-6], "counter") if metric.endswith("_total") else (metric, "gauge")
            lines.append(f"# TYPE {family} {kind}")
            previous_metric = metric
        for index, value in enumerate(series["values"]):
            if value is None:
                continue  # A missing sample is not a zero-valued observation.
            timestamp = cases["start"] + index * cases["interval"]
            lines.append(f'{selector(series["labels"])} {value} {timestamp}')
            count += 1
    (out / "samples.openmetrics").write_text("\n".join(lines + ["# EOF", ""]))
    inputs = [
        {"series": selector(series["labels"]), "values": " ".join(
            "_" if value is None else str(value) for value in series["values"]
        )}
        for series in cases["series"]
    ]
    suites = []
    for experimental in (False, True):
        groups = []
        for query in cases["queries"]:
            if query["experimental"] != experimental:
                continue
            groups.append({
                "name": query["id"], "interval": f'{cases["interval"]}s',
                "input_series": inputs,
                "promql_expr_test": [{
                    "expr": query.get("promtool_expr", query["expr"]),
                    "eval_time": f'{cases["eval_offset"]}s',
                    "exp_samples": [
                        {"labels": selector(s["labels"]), "value": s["value"]}
                        for s in query.get("promtool_expected", expected_samples(query))
                    ],
                }],
            })
        path = out / ("experimental.test.yml" if experimental else "rules.test.yml")
        # JSON is YAML; special float expectations require YAML numeric scalars.
        rendered = json.dumps({"fuzzy_compare": True, "tests": groups}, indent=2)
        for value, yaml in SPECIAL_YAML.items():
            rendered = rendered.replace(f'"value": "{value}"', f'"value": {yaml}')
        path.write_text(rendered + "\n")
        suites.append((path, experimental, len(groups)))
    print(f"Generated {len(cases['series'])} series / {count} samples / {len(cases['queries'])} queries in {out}", flush=True)
    return suites


def reference_checks(promtool, suites, out, version):
    version_run = subprocess.run([promtool, "--version"], capture_output=True, text=True, check=True)
    version_text = version_run.stdout + version_run.stderr
    if not re.search(rf"version {re.escape(version)}(?:\s|\(|,|$)", version_text):
        raise ValueError(f"Expected promtool {version}, got: {version_text.strip()}")
    results = []
    for path, experimental, count in suites:
        command = [promtool]
        if experimental:
            command.append(f"--enable-feature={FEATURE}")
        command += ["test", "rules", f"--junit={path.with_suffix('.xml')}", str(path)]
        run = subprocess.run(command, capture_output=True, text=True)
        log = run.stdout + run.stderr
        path.with_suffix(".log").write_text(log)
        status = "PASS" if run.returncode == 0 else "FAIL"
        if run.returncode:
            print(log, flush=True)
        case_results = []
        junit = path.with_suffix(".xml")
        if junit.exists():
            for case in ET.parse(junit).iter("testcase"):
                passed = not any(case.find(tag) is not None for tag in ("failure", "error", "skipped"))
                case_results.append({"id": case.attrib["name"], "status": "PASS" if passed else "FAIL"})
        if len(case_results) != count or any(row["status"] != "PASS" for row in case_results):
            status = "FAIL"
        print(f"Prometheus {path.name}: {status} ({count} cases)", flush=True)
        results.append({"suite": path.name, "case_count": count, "status": status, "cases": case_results,
                        "command": command, "exit_code": run.returncode, "log": log})
    report = {"version": version_text.strip(), "suites": results}
    save_json(out / "reference-results.json", report)
    return all(row["status"] == "PASS" for row in results)


def compare_vector(body, expected, evaluation, ordered=False):
    if body.get("status") != "success":
        raise ValueError(f"Query error: {body.get('errorType')}: {body.get('error')}")
    if body["data"]["resultType"] != "vector":
        raise ValueError(f"Expected vector, got {body['data']['resultType']}")
    actual = body["data"]["result"]
    key = lambda labels: tuple(sorted(labels.items()))
    wanted = {key(sample["labels"]): sample["value"] for sample in expected}
    if len(actual) != len(wanted):
        raise ValueError(f"Expected {len(wanted)} series, got {len(actual)}")
    seen = set()
    for series in actual:
        labels = key(series["metric"])
        if labels in seen or labels not in wanted:
            raise ValueError(f"Unexpected or duplicate labels: {labels}")
        seen.add(labels)
        timestamp, value = series["value"]
        if float(timestamp) != evaluation:
            raise ValueError(f"Expected timestamp {evaluation}, got {timestamp}")
        actual_value, reference_value = float(value), float(wanted[labels])
        equal = (
            math.isnan(actual_value) and math.isnan(reference_value)
            if math.isnan(reference_value)
            else math.isclose(actual_value, reference_value, rel_tol=1e-12, abs_tol=1e-12)
        )
        if not equal:
            raise ValueError(f"{labels}: expected {reference_value}, got {actual_value}")
    if ordered and [key(s["metric"]) for s in actual] != [key(s["labels"]) for s in expected]:
        raise ValueError("Series order differs")


def backend_checks(base_url, cases, out):
    evaluation = cases["start"] + cases["eval_offset"]
    results = []
    for query in cases["queries"]:
        expected = expected_samples(query, cases["start"])
        record = {"id": query["id"], "query": query["expr"], "expected": expected}
        url = base_url.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode(
            {"query": query["expr"], "time": evaluation}
        )
        try:
            try:
                response = urllib.request.urlopen(url, timeout=15)
            except urllib.error.HTTPError as error:
                response = error  # Retain the actual error body in the report.
            with response:
                record["http_status"] = response.code
                body = json.load(response)
            record["response"] = body
            if record["http_status"] != 200:
                raise ValueError(f"HTTP {record['http_status']}: {body}")
            compare_vector(body, expected, evaluation, query.get("ordered", False))
            record["status"] = "PASS"
        except (ValueError, KeyError, TypeError, OSError) as error:
            record.update(status="FAIL", error=str(error))
        results.append(record)
        print(record["status"], query["id"], record.get("error", ""), flush=True)
    save_json(out / "backend-results.json", results)
    print("Exact HTTP result checks only; inspect saved responses for execution/fallback provenance.")
    return all(row["status"] == "PASS" for row in results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/asap-promql-smoke"))
    parser.add_argument("--promtool", help="Run both stable and experimental official expression suites")
    parser.add_argument("--backend-url", help="Query a backend already loaded with these samples")
    parser.add_argument("--verify-catalog", action="store_true", help="Verify pinned official registry source hashes online")
    args = parser.parse_args()
    cases = json.loads((HERE / "cases.json").read_text())
    catalog = json.loads((HERE / "catalog.json").read_text())
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    coverage = check_coverage(cases, catalog, args.verify_catalog)
    save_json(out / "coverage.json", coverage)
    print("Coverage: " + ", ".join(
        f"{coverage[category]['covered']}/{coverage[category]['total']} {category}"
        for category in ("aggregation", "rollup")
    ))
    suites = generate(cases, out)
    success = True
    if args.promtool:
        success = reference_checks(args.promtool, suites, out, catalog["prometheus_version"])
    if args.backend_url:
        success = backend_checks(args.backend_url, cases, out) and success
    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
