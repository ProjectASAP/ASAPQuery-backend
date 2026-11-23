#!/bin/bash
# redo_accuracy.sh <tag> : re-score a final demo run against the independent replayed reference
# ($BENCH_ROOT/replay-ref/vm-data), keeping the arm-own-data outputs as *.own-data.*.
set -u
tag=$1
# PREP_NAME: the prepared workload's directory name (default demo.demo48.s2800); the run dir is <tag>.<PREP_NAME>.
PREP_NAME=${PREP_NAME:-demo.demo48.s2800}
B=$ASAPQ_BENCH; R=$B/runs/$tag.$PREP_NAME; P=$B/prep/$PREP_NAME
VM=$BENCH_ROOT/campaign-bin-8368fcfd/victoria-metrics-v1.156.0
cd $B/harness
for f in $R/victoriametrics/reference.jsonl $R/accuracy_vs_vm.json $R/report.json; do
  [ -f $f ] && [ ! -f ${f%.*}.own-data.${f##*.} ] && mv $f ${f%.*}.own-data.${f##*.}
done
for f in $R.accuracy_vs_vm.txt $R.summary.txt $R.demo_table.txt; do
  [ -f $f ] && [ ! -f ${f%.txt}.own-data.txt ] && mv $f ${f%.txt}.own-data.txt
done
# REF_START: the replay's start (= the run's victoriametrics arm start_ms); REF_DIR: the replayed store.
until=$(( ${REF_START:-1795395120000} + ${REF_TICKS:-14421} * 1000 ))
REFERENCE_DATA=${REF_DIR:?set REF_DIR} REFERENCE_SENT_UNTIL_MS=$until \
  python3 accuracy_vs_vm.py $R $P $VM > $R.accuracy_vs_vm.txt 2>&1
python3 report.py --prep $P --run $R > $R.summary.txt 2>&1
python3 demo_table.py --prep $P --run $R > $R.demo_table.txt 2>&1
echo REDO_DONE >> $R.accuracy_vs_vm.txt
