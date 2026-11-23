#!/usr/bin/env python3
"""Cross-check accuracy_vs_vm.promql_truth (MetricsQL template) against a direct Python
port of Prometheus' extrapolatedRate over the raw samples exported from the run's kept
VictoriaMetrics data.  usage: check_extrapolated.py <run dir> <metric> <window s> [n series]"""
import os
import json, random, subprocess, sys, time, urllib.parse, urllib.request
from pathlib import Path
sys.argv, args = sys.argv[:1] + ["/tmp", "/tmp"], sys.argv[1:]
exec(open(os.environ["ASAPQ_BENCH"] + "/harness/accuracy_vs_vm.py").read().split("def answers")[0])
run, metric, w = Path(args[0]), args[1], int(args[2])
nser = int(args[3]) if len(args) > 3 else 5
PORT = 19782


def prom_rate(samples, start, end, is_rate):
    """Prometheus promql/functions.go extrapolatedRate (counter), samples (t s, v) in (start, end]."""
    s = [(t, v) for t, v in samples if start < t <= end]
    if len(s) < 2:
        return None
    result = s[-1][1] - s[0][1]
    prev = s[0][1]
    for _, v in s[1:]:
        if v < prev:
            result += prev
        prev = v
    d_start, d_end = s[0][0] - start, end - s[-1][0]
    sampled = s[-1][0] - s[0][0]
    avg = sampled / (len(s) - 1)
    if result > 0 and s[0][1] >= 0:
        d_zero = sampled * (s[0][1] / result)
        if d_zero < d_start:
            d_start = d_zero
    thr = avg * 1.1
    ext = sampled + (d_start if d_start < thr else avg / 2) + (d_end if d_end < thr else avg / 2)
    result = result * (ext / sampled)
    return result / (end - start) if is_rate else result


v0 = json.load(open(run / "victoriametrics/resources.json"))["start_ms"]
vm = subprocess.Popen([os.environ["BIN_DIR"] + "/victoria-metrics-v1.156.0", f"-storageDataPath={run / 'victoriametrics/victoria-metrics-data'}",
                       f"-httpListenAddr=127.0.0.1:{PORT}", "-retentionPeriod=100y", "-search.latencyOffset=0s",
                       "-loggerLevel=ERROR"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    for _ in range(240):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{PORT}/health", timeout=2); break
        except Exception:
            time.sleep(0.5)
    get = lambda path, p: json.load(urllib.request.urlopen(f"http://127.0.0.1:{PORT}{path}?" + urllib.parse.urlencode(p), timeout=600))
    series = get("/api/v1/series", {"match[]": metric, "start": str(v0 // 1000 - 60)})["data"]
    random.Random(1).shuffle(series)
    worst = {"rate": 0.0, "increase": 0.0}
    n = 0
    for labels in series[:nser]:
        sel = metric + "{" + ",".join(f'{k}="{v}"' for k, v in labels.items() if k != "__name__") + "}"
        exp = [json.loads(l) for l in urllib.request.urlopen(f"http://127.0.0.1:{PORT}/api/v1/export?" + urllib.parse.urlencode(
            {"match[]": sel, "start": str(v0 // 1000 - 60)}), timeout=600).read().decode().splitlines()][0]
        samples = sorted((t / 1000, float(v)) for t, v in zip(exp["timestamps"], exp["values"]))
        for k in range(6):
            t = samples[0][0] + w + 37.3 * (k + 1) + 0.05 * k
            t = round(t)  # whole-second evaluation times, as the harness uses
            for func in ("rate", "increase"):
                res = get("/api/v1/query", {"query": promql_truth(f"{func}({sel}[{w}s])"), "time": f"{t:.3f}"})["data"]["result"]
                got = float(res[0]["value"][1]) if res else None
                want = prom_rate(samples, t - w, t, func == "rate")
                if got is None or want is None:
                    continue
                err = abs(got - want) / max(abs(want), 1e-12)
                worst[func] = max(worst[func], err)
                n += 1
    print(json.dumps({"metric": metric, "window_s": w, "points": n, "max_rel_diff_template_vs_python": worst}))
finally:
    vm.terminate(); vm.wait()
