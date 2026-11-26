#!/usr/bin/env python3
"""ASAP's rate/increase across counter resets, against the post-run reference.

usage: verify_resets.py <run dir> [vm binary]

Finds every counter reset in the run's data (data_total samples whose value drops),
exported from VictoriaMetrics' kept data, then splits ASAP's per-series and per-job
rate/increase answers into evaluations whose window contains a reset of that
series (or of a series of that job) and the others, and reports for both groups:
points compared, within 5% of the reference (reference.jsonl: rate_prometheus /
increase_prometheus), and the max relative error.
"""
import os
import json, re, subprocess, sys, time, urllib.parse, urllib.request
from collections import defaultdict
from pathlib import Path

run = Path(sys.argv[1])
VM = sys.argv[2] if len(sys.argv) > 2 else os.environ["BIN_DIR"] + "/victoria-metrics-v1.156.0"
PORT = 19779
WIN = re.compile(r"\[(\d+)([smh])\]")


def key(metric):
    return tuple(sorted((k, v) for k, v in metric.items() if k != "__name__"))


def main():
    a0 = json.load(open(run / "asap/resources.json"))["start_ms"]
    v0 = json.load(open(run / "victoriametrics/resources.json"))["start_ms"]
    data = run / "victoriametrics/victoria-metrics-data"
    vm = subprocess.Popen([VM, f"-storageDataPath={data}", f"-httpListenAddr=127.0.0.1:{PORT}", "-retentionPeriod=100y",
                           "-loggerLevel=ERROR"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    resets = defaultdict(list)  # series key -> [offset ms of the first sample after a drop]
    try:
        for _ in range(240):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{PORT}/health", timeout=2); break
            except Exception:  # noqa: BLE001
                time.sleep(0.5)
        body = urllib.request.urlopen(f"http://127.0.0.1:{PORT}/api/v1/export?" + urllib.parse.urlencode(
            {"match[]": "data_total", "start": str(v0 // 1000 - 60)}), timeout=3600).read().decode()
        for line in body.splitlines():
            s = json.loads(line)
            pairs = sorted(zip(s["timestamps"], s["values"]))
            for (t1, x1), (t2, x2) in zip(pairs, pairs[1:]):
                if x2 < x1:
                    resets[key(s["metric"])].append(t2 - v0)
    finally:
        vm.terminate(); vm.wait()
    ref = {}
    for line in open(run / "victoriametrics/reference.jsonl"):
        r = json.loads(line)
        if r.get("status") == 200:
            ref[(r["query"], r["offset_ms"])] = {key(x["metric"]): float(x["value"][1])
                                                 for x in r["result"]["data"]["result"]}
    stats = defaultdict(lambda: {"points": 0, "within": 0, "max_err": 0.0})
    for line in open(run / "asap/answers.jsonl"):
        a = json.loads(line)
        q = a["query"]
        if not (q.startswith("r01") or q.startswith("r02") or q.startswith("r03")) or a["status"] != 200:
            continue
        rv = ref.get((q, a["offset_ms"]))
        if rv is None:
            continue
        w_s = {"r01": 600, "r02": 300, "r03": 600}[q[:3]]
        t = a["offset_ms"]
        for x in a["result"]["data"]["result"]:
            k = key(x["metric"])
            if k not in rv:
                continue
            if q.startswith("r03"):
                job = dict(k).get("job")
                spans = any(t - w_s * 1000 < r <= t for s, rs in resets.items() if dict(s).get("job") == job for r in rs)
            else:
                spans = any(t - w_s * 1000 < r <= t for r in resets.get(k, []))
            v, e = float(x["value"][1]), rv[k]
            err = 0.0 if v == e else abs(v - e) / max(abs(e), 1e-12)
            st = stats[(q, "window contains a reset" if spans else "no reset in window")]
            st["points"] += 1
            st["within"] += int(err <= 0.05)
            st["max_err"] = max(st["max_err"], err)
    out = {"resets_found": sum(len(v) for v in resets.values()), "series_with_resets": len(resets),
           "by_query": {f"{q} | {g}": v for (q, g), v in sorted(stats.items())}}
    (run / "verify_resets.json").write_text(json.dumps(out, indent=1))
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
