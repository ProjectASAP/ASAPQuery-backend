#!/bin/bash
# postrun.sh <cfg>: for each finished run of <cfg> on this host, the unified harness's
# post-run ground truth (accuracy_vs_vm.py on VM's kept data), then report.py and demo_table.py again.
source $BENCH_ENV
cd $ASAPQ_BENCH/harness
VMB=$BIN_DIR/victoria-metrics-v1.156.0
for r in ../runs/camp1120-$1.*; do
  [ -d "$r" ] && [ -f "$r.demo_table.txt" ] || continue
  p=../prep/${r#../runs/camp1120-$1.}
  [ -d "$r/victoriametrics/victoria-metrics-data" ] || { echo "skip $r: VM data not kept (keeps its own report)"; continue; }
  echo "== $r"
  python3 accuracy_vs_vm.py "$r" "$p" "$VMB" > "$r.accuracy_vs_vm.txt" 2>&1 || echo "accuracy failed"
  python3 report.py --prep "$p" --run "$r" > "$r.summary.txt" 2>&1 || echo "report failed"
  python3 demo_table.py --prep "$p" --run "$r" > "$r.demo_table.txt" 2>&1 || echo "demo_table failed"
  tail -n 25 "$r.accuracy_vs_vm.txt"
done
