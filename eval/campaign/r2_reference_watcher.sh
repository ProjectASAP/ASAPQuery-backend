#!/bin/bash
# r2_reference_watcher.sh (on the driver host): when the r2 run's victoriametrics arm starts, replay its
# seeded feed from the same start on the reference host $REF_HOST (no query load), copy the replayed store here, and after the
# run's post-run ends, re-score both arms against it (redo_accuracy.sh). Log: r2_reference.log.
set -u
TAG=final-demo48-s2800-u4-r2; REF_HOST=${REF_HOST:?set REF_HOST}; SSH="ssh -o BatchMode=yes"
log() { echo "$(date -u +%FT%TZ) $*" >> $CAMPAIGN_DIR/r2_reference.log; }
P=$ASAPQ_BENCH/prep/demo.demo48.s2800
log "waiting for the victoriametrics arm's feeders"
while :; do
  start=$(pgrep -af "paced_feeder_realistic .*:19500/api/v1/write" | grep -o -- "--start-ms [0-9]*" | head -1 | awk '{print $2}')
  [ -n "$start" ] && break; sleep 30
done
log "victoriametrics arm start_ms=$start"
for i in 0 1 2 3; do
  a=$(sha256sum < $P/feeder_table.$i.json | cut -c1-16); b=$($SSH $REF_HOST "sha256sum < $BENCH_ROOT/replay/demo.demo48.s2800/feeder_table.$i.json | cut -c1-16")
  [ "$a" = "$b" ] || { log "feeder table $i differs ($a vs $b): copying"; $SSH $REF_HOST "cat > $BENCH_ROOT/replay/demo.demo48.s2800/feeder_table.$i.json" < $P/feeder_table.$i.json; }
done
$SSH $REF_HOST "cd $BENCH_ROOT/replay && setsid nohup ./replay_at.sh $start > replay-$start.out 2>&1 < /dev/null &"
log "replay started on $REF_HOST"
until $SSH $REF_HOST "grep -q 'vm stopped' $BENCH_ROOT/replay/replay-$start.status" 2>/dev/null; do sleep 60; done
log "replay done: $($SSH $REF_HOST "tr '\n' ' ' < $BENCH_ROOT/replay/replay-$start.status")"
rm -rf $BENCH_ROOT/replay-ref-$start; mkdir -p $BENCH_ROOT/replay-ref-$start
$SSH $REF_HOST "tar -C $BENCH_ROOT/replay -cf - vm-data-$start" | tar -C $BENCH_ROOT/replay-ref-$start -xf -
log "copied: $(du -sh $BENCH_ROOT/replay-ref-$start/vm-data-$start | cut -f1)"
until grep -q "^end=" $ASAPQ_BENCH/runs/$TAG.meta 2>/dev/null && ! pgrep -f "^/bin/bash ./scripts/run_vm_demo.sh" >/dev/null; do sleep 60; done
log "run and post-run ended; re-scoring against the replay"
cd $CAMPAIGN_DIR && REF_START=$start REF_DIR=$BENCH_ROOT/replay-ref-$start/vm-data-$start ./redo_accuracy.sh $TAG > redo-r2.out 2>&1
log "re-scoring done"
