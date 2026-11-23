#!/usr/bin/env python3
"""ASAP accuracy against VictoriaMetrics as ground truth, for one finished run.

usage: accuracy_vs_vm.py <run dir> <prep dir> [vm binary]

Ground truth per (query, evaluation):
  - every query: VictoriaMetrics' own answer in the run (victoriametrics/answers.jsonl),
    when it answered (a VM timeout/error leaves that point "unverified");
  - rate/increase queries additionally: the *_prometheus variant (rate_prometheus,
    increase_prometheus: PromQL's extrapolation) asked afterwards, untimed, of a
    VictoriaMetrics started on the run's stored data; that one is the ground truth
    for those queries, and both differences are recorded.
Series are matched on labels without __name__. Instant queries only.
Writes <run dir>/accuracy_vs_vm.json and prints a table.
"""
import os
import json, re, subprocess, sys, time, urllib.parse, urllib.request
from pathlib import Path

run, prep = Path(sys.argv[1]), Path(sys.argv[2])
VM = sys.argv[3] if len(sys.argv) > 3 else os.environ["BIN_DIR"] + "/victoria-metrics-v1.156.0"
PORT = 19777
TARGET = 0.05
RATE = re.compile(r"\b(rate|increase)\(")


def answers(arm):
    out = {}
    p = run / arm / "answers.jsonl"
    if not p.exists():
        return out
    for line in open(p):
        a = json.loads(line)
        out[(a["query"], a["offset_ms"])] = a
    return out


def vector(a):
    if not a or a.get("status") != 200:
        return None
    res = (a.get("result") or {}).get("data", {})
    if res.get("resultType") != "vector":
        return None
    return {tuple(sorted((k, v) for k, v in r["metric"].items() if k != "__name__")): float(r["value"][1])
            for r in res["result"]}, (res["result"][0]["value"][0] if res["result"] else None)


def compare(asap, ref):
    """max relative error over the reference's series; series ASAP lacks or adds count as misses."""
    errs, missing = [], 0
    for k, v in ref.items():
        if k not in asap:
            missing += 1; continue
        a = asap[k]
        errs.append(0.0 if a == v else abs(a - v) / max(abs(v), 1e-12))
    extra = sum(1 for k in asap if k not in ref)
    return (max(errs) if errs else 0.0), missing, extra


def main():
    queries = {q["id"]: q for q in json.load(open(prep / "queries.json"))["queries"]}
    if any("range" in q for q in queries.values()):
        print("range run: not handled (instant only)"); return
    A, V = answers("asap"), answers("victoriametrics")
    # Post-run, untimed reference on VictoriaMetrics' stored data for every query
    # (in-run VM answers can be stale when VM's ingestion falls behind under query
    # load); rate/increase as *_prometheus. Needs the run's VM data (KEEP_VM_DATA).
    have_data = any((run / "victoriametrics" / "victoria-metrics-data").glob("**/*.bin")) or \
        any((run / "victoriametrics" / "victoria-metrics-data").glob("data/*/*"))
    need_ref = set(queries) if have_data else set()
    vm = None
    if need_ref:
        data = run / "victoriametrics" / "victoria-metrics-data"
        vm = subprocess.Popen([VM, f"-storageDataPath={data}", f"-httpListenAddr=127.0.0.1:{PORT}",
                               "-retentionPeriod=100y", "-search.maxQueryDuration=1h", "-search.latencyOffset=0s",
                               "-search.maxSamplesPerQuery=100000000000", "-search.maxSamplesPerSeries=1000000000",
                               "-search.disableCache", "-loggerLevel=ERROR"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(240):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{PORT}/health", timeout=2); break
            except Exception:  # noqa: BLE001
                time.sleep(0.5)

    def ref_query(promql, t):
        q = RATE.sub(lambda m: m.group(1) + "_prometheus(", promql)
        url = f"http://127.0.0.1:{PORT}/api/v1/query?" + urllib.parse.urlencode({"query": q, "time": t})
        try:
            body = json.load(urllib.request.urlopen(url, timeout=3600))
        except Exception as e:  # noqa: BLE001
            return None
        return {tuple(sorted((k, v) for k, v in r["metric"].items() if k != "__name__")): float(r["value"][1])
                for r in body["data"]["result"]}

    rows = {}
    try:
        for (qid, off), a in sorted(A.items()):
            r = rows.setdefault(qid, {"n": 0, "asap_failed": 0, "unverified": 0, "within": 0, "max_err_vm": 0.0,
                                      "max_err_ref": None, "missing": 0, "extra": 0, "ref": "vm in-run"})
            r["n"] += 1
            av = vector(a)
            if av is None:
                r["asap_failed"] += 1; continue
            asap, t = av
            vv = vector(V.get((qid, off)))
            err_vm = None
            if vv is not None:
                err_vm, _, _ = compare(asap, vv[0])
                r["max_err_vm"] = max(r["max_err_vm"], err_vm)
            if qid in need_ref and t is not None:
                r["ref"] = "vm post-run" + (" *_prometheus" if RATE.search(queries[qid]["promql"]) else "")
                ref = ref_query(queries[qid]["promql"], t)
                if not ref:  # no answer, or no series (e.g. data not kept): unverified
                    r["unverified"] += 1; continue
                err, miss, extra = compare(asap, ref)
                r["max_err_ref"] = max(r["max_err_ref"] or 0.0, err)
            elif vv is None:
                r["unverified"] += 1; continue
            else:
                err, miss, extra = err_vm, *compare(asap, vv[0])[1:]
            r["missing"] += miss; r["extra"] += extra
            r["within"] += int(err <= TARGET and miss == 0)
    finally:
        if vm:
            vm.terminate(); vm.wait()
    (run / "accuracy_vs_vm.json").write_text(json.dumps(rows, indent=1))
    print("| query | ground truth | n | asap failed | unverified | within 5% | max err vs ground truth | max err vs in-run VM answer | series missing/extra |")
    print("|---|---|---|---|---|---|---|---|---|")
    for qid, r in rows.items():
        gt = r["max_err_ref"] if r["ref"] != "vm in-run" else r["max_err_vm"]
        print(f"| {qid} | {r['ref']} | {r['n']} | {r['asap_failed']} | {r['unverified']} | {r['within']} | "
              f"{'-' if gt is None else f'{gt:.2%}'} | {r['max_err_vm']:.2%} | {r['missing']}/{r['extra']} |")


if __name__ == "__main__":
    main()
