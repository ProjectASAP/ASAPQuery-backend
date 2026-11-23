#!/bin/bash
# seed_demo.sh <commit> <engine_ip> <run_s> <workload-name>...: exploratory seed-862 runs on the DRIVER host
# against the ENGINE host, one workload after another: ASAP then VictoriaMetrics, each run_s from zero,
# 1 user, SERIES 10000 (10k series per metric, 10 Hz), Zipf data, open-loop client, every-10th answers kept
# (offset-aligned). For each run: when its victoriametrics arm starts, replay the seeded feed from the same start
# on the reference host $REF_HOST (no query load), copy the store here, and after the post-run re-score both arms against it.
set -u
commit=$1 eip=$2 run_s=$3; shift 3
cd $CAMPAIGN_DIR
REF_HOST=${REF_HOST:?set REF_HOST}; SSH="ssh -o BatchMode=yes"; LOG=$CAMPAIGN_DIR/seed_demo.log
log() { echo "$(date -u +%FT%TZ) $*" >> $LOG; }
port=19878
for wl in "$@"; do
  tag=$wl-r1; prep=demo.$wl.s10000; ticks=$(( run_s + 21 ))
  log "start $tag ($wl, $commit)"
  ( # reference replay for this run's victoriametrics arm
    while :; do
      start=$(pgrep -af "paced_feeder_realistic .*:19500/api/v1/write" | grep -o -- "--start-ms [0-9]*" | head -1 | awk '{print $2}')
      [ -n "$start" ] && grep -q "victoriametrics" <(ls $ASAPQ_BENCH/runs/$tag.$prep 2>/dev/null) && break; sleep 30
    done
    log "$tag: victoriametrics start_ms=$start; replaying on $REF_HOST (port $port)"
    $SSH $REF_HOST "mkdir -p $BENCH_ROOT/replay/$prep"
    for f in $ASAPQ_BENCH/prep/$prep/feeder_table.*.json; do $SSH $REF_HOST "cat > $BENCH_ROOT/replay/$prep/$(basename $f)" < $f; done
    $SSH $REF_HOST "cd $BENCH_ROOT/replay && setsid nohup ./replay_gen.sh $start $prep $ticks $port > replay-$start.out 2>&1 < /dev/null &"
    until $SSH $REF_HOST "grep -q 'vm stopped' $BENCH_ROOT/replay/replay-$start.status" 2>/dev/null; do sleep 60; done
    log "$tag: replay done: $($SSH $REF_HOST "tr '\n' ' ' < $BENCH_ROOT/replay/replay-$start.status")"
    rm -rf $BENCH_ROOT/replay-ref-$start; mkdir -p $BENCH_ROOT/replay-ref-$start
    $SSH $REF_HOST "tar -C $BENCH_ROOT/replay -cf - vm-data-$start" | tar -C $BENCH_ROOT/replay-ref-$start -xf -
    echo $start > $CAMPAIGN_DIR/$tag.ref_start
    log "$tag: reference copied"
  ) &
  watcher=$!
  QUERY_MAX_INFLIGHT=4096 ANSWER_RESULTS_EVERY=10 KEEP_VM_DATA=1 FEEDER_SHARDS=2 \
  ENGINE_SSH=$eip ENGINE_ADDR=$eip NET_IFACE=${NET_IFACE:?set NET_IFACE} ENGINE_SSH_OPTS="-o BatchMode=yes" \
    ./demo_job.sh $tag $commit 10000 1 $run_s asap,victoriametrics demo/$wl > $tag.out 2>&1
  log "$tag: run and post-run ended rc=$?"
  wait $watcher
  start=$(cat $CAMPAIGN_DIR/$tag.ref_start)
  PREP_NAME=$prep REF_START=$start REF_TICKS=$ticks REF_DIR=$BENCH_ROOT/replay-ref-$start/vm-data-$start \
    ./redo_accuracy.sh $tag > redo-$tag.out 2>&1
  log "$tag: re-scored against the replay"
  port=$((port + 1))
done
log SEED_DEMO_DONE
