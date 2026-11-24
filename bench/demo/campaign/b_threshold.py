#!/usr/bin/env python3
"""Query B's threshold edge: do ASAP's extra series (returned by ASAP, filtered out by the
reference) come from rate vs rate_prometheus extrapolation?

usage: b_threshold.py <run dir> <query id prefix, default B->

For every B evaluation ASAP answered, asks the run's kept VictoriaMetrics data (untimed)
for the UNFILTERED expression (the part before `>= 80`) twice: with rate_prometheus (PromQL
extrapolation, what ASAP implements and the reference uses) and with MetricsQL's rate. For
each series ASAP returned that the reference did not (and vice versa), prints ASAP's value,
both reference values and which side of the threshold each falls on.
"""
import os
import json, re, subprocess, sys, time, urllib.parse, urllib.request
from pathlib import Path
_argv, sys.argv = sys.argv, sys.argv[:1] + ["/tmp", "/tmp"]
exec(open(os.environ["ASAPQ_BENCH"] + "/harness/accuracy_vs_vm.py").read().split("def answers")[0])  # promql_truth
sys.argv = _argv

run = Path(sys.argv[1])
prefix = sys.argv[2] if len(sys.argv) > 2 else "B-"
VM = os.environ["BIN_DIR"] + "/victoria-metrics-v1.156.0"
PORT = 19781


def key(m):
    return tuple(sorted((k, v) for k, v in m.items() if k != "__name__"))


def main():
    queries = {q["id"]: q for q in json.load(open(next(run.parent.parent.glob(f"prep/{run.name.split('.', 1)[1]}")) / "queries.json"))["queries"]}
    qid = next(q for q in queries if q.startswith(prefix))
    promql = queries[qid]["promql"]
    m = re.match(r"^\((.*)\)\s*(>=|>|<=|<)\s*([0-9.]+)$", promql.strip())
    inner, op, thr = m.group(1), m.group(2), float(m.group(3))
    prom_inner = re.sub(r"\b(rate|increase)\(", lambda x: x.group(1) + "_prometheus(", inner)
    v0 = json.load(open(run / "victoriametrics/resources.json"))["start_ms"]
    vm = subprocess.Popen([VM, f"-storageDataPath={run / 'victoriametrics/victoria-metrics-data'}",
                           f"-httpListenAddr=127.0.0.1:{PORT}", "-retentionPeriod=100y", "-search.latencyOffset=0s",
                           "-loggerLevel=ERROR"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    rows, tally = [], {"extra": 0, "missing": 0, "explained_by_extrapolation": 0, "near_threshold_1pct": 0}
    try:
        for _ in range(240):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{PORT}/health", timeout=2); break
            except Exception:  # noqa: BLE001
                time.sleep(0.5)

        def q(expr, t):
            url = f"http://127.0.0.1:{PORT}/api/v1/query?" + urllib.parse.urlencode({"query": expr, "time": f"{t:.3f}"})
            return {key(r["metric"]): float(r["value"][1]) for r in json.load(urllib.request.urlopen(url, timeout=600))["data"]["result"]}

        passes = (lambda v: v >= thr) if op == ">=" else (lambda v: v > thr) if op == ">" else \
                 (lambda v: v <= thr) if op == "<=" else (lambda v: v < thr)
        for line in open(run / "asap/answers.jsonl"):
            a = json.loads(line)
            if a["query"] != qid or a["status"] != 200 or a.get("user", 0) != 0:
                continue
            t = (v0 + a["offset_ms"]) / 1000
            asap = {key(r["metric"]): float(r["value"][1]) for r in a["result"]["data"]["result"]}
            p, r_, x = q(prom_inner, t), q(inner, t), q(promql_truth(inner), t)
            ref_pass = {k for k, v in x.items() if passes(v)}  # the truth: PromQL extrapolated rate
            tally["evaluations"] = tally.get("evaluations", 0) + 1
            for k in asap:
                if k in x:
                    tally["max_rel_err_vs_extrapolated"] = max(tally.get("max_rel_err_vs_extrapolated", 0.0),
                                                                abs(asap[k] - x[k]) / max(abs(x[k]), 1e-12))
            for k in set(asap) ^ ref_pass:
                kind = "extra" if k in asap else "missing"
                tally[kind] += 1
                av, pv, rv = asap.get(k), p.get(k), r_.get(k)
                near = pv is not None and abs(pv - thr) / thr <= 0.01
                tally["near_threshold_1pct"] += int(near)
                # Extrapolation explains it when ASAP's value matches the plain-rate value's side
                # of the threshold while rate_prometheus falls on the other side.
                expl = rv is not None and pv is not None and passes(rv) != passes(pv) and \
                    av is not None and passes(av) == passes(rv)
                tally["explained_by_extrapolation"] += int(expl)
                rows.append({"offset_s": a["offset_ms"] // 1000, "series": dict(k).get("job"), "kind": kind,
                             "asap": av, "ref_extrapolated": x.get(k), "ref_rate_prometheus": pv, "ref_metricsql_rate": rv,
                             "near_threshold_1pct": near, "explained_by_rate_vs_rate_prometheus": expl})
    finally:
        vm.terminate(); vm.wait()
    out = {"query": qid, "threshold": f"{op} {thr}", "tally": tally, "rows": rows[:60]}
    (run / "b_threshold.json").write_text(json.dumps(out, indent=1))
    print(json.dumps({"query": qid, "threshold": f"{op} {thr}", "tally": tally}, indent=1))
    print("| offset s | job | kind | ASAP | truth (PromQL extrapolated) | VM rate_prometheus | VM MetricsQL rate | within 1% | rate vs rate_prometheus explains |")
    print("|---|---|---|---|---|---|---|---|---|")
    for r in rows[:25]:
        f = lambda v: "-" if v is None else f"{v:.2f}"  # noqa: E731
        print(f"| {r['offset_s']} | {r['series']} | {r['kind']} | {f(r['asap'])} | {f(r['ref_extrapolated'])} | {f(r['ref_rate_prometheus'])} | "
              f"{f(r['ref_metricsql_rate'])} | {r['near_threshold_1pct']} | {r['explained_by_rate_vs_rate_prometheus']} |")


if __name__ == "__main__":
    main()
