#!/bin/bash
# Live demo sequence: ingest only -> query load on (N users) -> off, with screenshots and a per-state
# table (feeder lag, overrun ticks, visible-sample fraction, accuracy, load outcomes) from the
# monitoring Prometheus.   demo/live/demo_sequence.sh <start epoch s> [users]
: "${ASAPQ_BENCH:?set ASAPQ_BENCH}" "${BENCH_ROOT:?set BENCH_ROOT}"
cd $ASAPQ_BENCH
export SERIES=10000 WORKLOAD=asap-demo AUX_CPUS=48-55 BIN_DIR=${BIN_DIR:?set BIN_DIR}
USERS=${2:-8}
TAG=${3:-v2}
until [ "$(date +%s)" -gt "${1:-0}" ]; do sleep 30; done
mark() { date +%s > "demo/live/logs/$TAG-state-$1.t"; echo "== $(date -u +%T) $1"; }
mark A-start; sleep 600; mark A-end
../deployment/monitoring/screenshot.sh asap-demo now-30m now demo/live/screenshots/$TAG-state-A-ingest-only 2>&1 | tail -1
demo/run_live_demo.sh load on "$USERS"; mark B-start
sleep 1500; mark B-end
../deployment/monitoring/screenshot.sh asap-demo now-45m now demo/live/screenshots/$TAG-state-B-load-on 2>&1 | tail -1
demo/run_live_demo.sh load off; mark C-start
sleep 900; mark C-end
../deployment/monitoring/screenshot.sh asap-demo now-1h now demo/live/screenshots/$TAG-state-C-load-off 2>&1 | tail -1
TAG=$TAG python3 - <<'PY'
import json, os, urllib.parse, urllib.request
TAG = os.environ['TAG']
t = {k: int(open(f"demo/live/logs/{TAG}-state-{k}.t").read()) for k in
     ("A-start", "A-end", "B-start", "B-end", "C-start", "C-end")}
def q(expr, at):
    url = "http://127.0.0.1:19090/api/v1/query?" + urllib.parse.urlencode({"query": expr, "time": at})
    return json.load(urllib.request.urlopen(url))["data"]["result"]
CLASSES = ("answered_ok", "inaccurate_missing_samples", "inaccurate_approximation", "slow", "rejected", "timeout", "partial")
print("| state | engine | max feeder lag s | overrun ticks | min visible fraction | " + " | ".join(CLASSES) + " |")
print("|---|---|---|---|---|" + "---|" * len(CLASSES))
for st in ("A", "B", "C"):
    a, b = t[f"{st}-start"], t[f"{st}-end"]; w = f"{b - a}s"
    for eng in ("asap", "victoriametrics"):
        def one(expr):
            r = q(expr, b)
            return float(r[0]["value"][1]) if r else None
        lag = one(f'max_over_time(live_feeder_start_lag_seconds{{engine="{eng}"}}[{w}])')
        ovr = one(f'increase(live_feeder_overrun_ticks_total{{engine="{eng}"}}[{w}])')
        frac = one(f'min_over_time(live_window_fraction{{engine="{eng}"}}[{w}])')
        outc = {o: one(f'sum(increase(live_load_requests_total{{engine="{eng}",outcome="{o}"}}[{w}]))') or 0
                for o in CLASSES}
        print(f"| {st} | {eng} | {lag} | {ovr} | {frac} | " + " | ".join(f"{outc[o]:.0f}" for o in CLASSES) + " |")
print()
print("| state | query | engine | max rel error vs reference | min share within 5% |")
print("|---|---|---|---|---|")
for st in ("A", "B", "C"):
    a, b = t[f"{st}-start"], t[f"{st}-end"]; w = f"{b - a}s"
    errs = {(r["metric"]["query"], r["metric"]["engine"]): float(r["value"][1])
            for r in q(f"max by (query, engine) (max_over_time(asap_demo_accuracy_max_rel_error[{w}]))", b)}
    within = {(r["metric"]["query"], r["metric"]["engine"]): float(r["value"][1])
              for r in q(f"min by (query, engine) (min_over_time(asap_demo_accuracy_within_target_ratio[{w}]))", b)}
    for k in sorted(errs):
        print(f"| {st} | {k[0]} | {k[1]} | {errs[k]:.4f} | {within.get(k, float('nan')):.3f} |")
PY
ps -o rss,pcpu,cmd -p "$(cat demo/live/pids/data_plane)","$(cat demo/live/pids/victoria-metrics)" | cut -c1-70
echo SEQUENCE_DONE
