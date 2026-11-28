#!/usr/bin/env python3
"""check_queries_post.py <run_dir> <prep> <vm_bin> <ref_data> <query_id>...: re-check an arm's kept answers
of the given queries against the replayed reference, sending each reference query as POST
(accuracy_vs_vm.py sends GET; a long PromQL, e.g. L4's 256-host regex rewritten to Prometheus-
extrapolated form, exceeds the URL limit: HTTP 400). Same truth and comparison as accuracy_vs_vm.py."""
import os
import json, subprocess, sys, time, urllib.parse, urllib.request
from pathlib import Path
run, prep, vmbin, ref = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3], sys.argv[4]
qids = sys.argv[5:]
sys.argv = [sys.argv[0], str(run), str(prep)]
sys.path.insert(0, os.environ["ASAPQ_BENCH"] + "/harness")
import accuracy_vs_vm as A  # noqa: E402
PORT = 19788
queries = {q["id"]: q for q in json.load(open(prep / "queries.json"))["queries"]}
vm = subprocess.Popen([vmbin, f"-storageDataPath={ref}", f"-httpListenAddr=127.0.0.1:{PORT}", "-retentionPeriod=100y",
                       "-search.maxQueryDuration=1h", "-search.latencyOffset=0s", "-search.maxSamplesPerQuery=100000000000",
                       "-search.disableCache", "-loggerLevel=ERROR", "-search.maxQueryLen=1000000"],
                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
for _ in range(240):
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{PORT}/health", timeout=2); break
    except Exception:
        time.sleep(0.5)
try:
    for arm in ("asap", "victoriametrics"):
        if not (run / arm / "answers.jsonl").exists():
            continue
        a0 = json.load(open(run / arm / "resources.json"))["start_ms"]
        v0 = json.load(open(run / "victoriametrics" / "resources.json"))["start_ms"]
        stats = {}
        for line in open(run / arm / "answers.jsonl"):
            if not any(f'"query": "{q}"' in line for q in qids) or '"omitted": true' in line:
                continue
            a = json.loads(line)
            if a.get("user", 0) != 0:
                continue
            q = queries[a["query"]]
            promql = A.promql_truth(q["promql"]) if A.RATE_CALL.search(q["promql"]) else q["promql"]
            t = (v0 + a["offset_ms"]) / 1000
            body = urllib.parse.urlencode({"query": promql, "time": f"{t:.3f}"}).encode()
            r = stats.setdefault(q["id"], {"n": 0, "failed": 0, "ref_error": 0, "both_empty": 0, "within": 0, "max_err": 0.0,
                                         "missing": 0, "extra": 0})
            r["n"] += 1
            mine = A.answered(a, a0)
            if mine is None:
                r["failed"] += 1; continue
            try:
                res = json.load(urllib.request.urlopen(f"http://127.0.0.1:{PORT}/api/v1/query", data=body, timeout=600))
            except Exception as e:
                r["ref_error"] += 1; r["last_error"] = str(e)[:120]; continue
            truth = A.points(res, v0)
            if not truth and not mine:
                r["both_empty"] += 1; r["within"] += 1; continue
            err, miss, extra = A.compare(mine, truth)
            r["max_err"] = max(r["max_err"], err); r["missing"] += miss; r["extra"] += extra
            r["within"] += int(err <= A.TARGET and miss == 0 and extra == 0)
        for qid, r in stats.items():
            print(f"{arm} {qid}: {json.dumps(r)}")
finally:
    vm.terminate(); vm.wait()
