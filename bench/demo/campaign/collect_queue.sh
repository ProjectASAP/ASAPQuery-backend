#!/bin/bash
# collect_queue.sh: pull every campaign host's queue status/state and job outputs (small files)
# into $ASAPQ_BENCH/campaign-queue/<host>/. Safe to run any time (cron or by hand).
OUT=$ASAPQ_BENCH/campaign-queue
for h in ${BENCH_HOSTS:?set BENCH_HOSTS}; do
  R=$h; mkdir -p $OUT/$h
  timeout 120 rsync -a $R:$CAMPAIGN_DIR/queue/ $OUT/$h/queue/ 2>/dev/null
  timeout 600 rsync -a --prune-empty-dirs --exclude 'vmdata/' --exclude 'views/' --exclude 'victoria-metrics-data/' \
    --exclude 'data_plane-data/' --include='*/' --include='*.json' --include='*.jsonl' --include='*.log' --include='*.txt' \
    --include='*.meta' --exclude='*' $R:$CAMPAIGN_DIR/jobs-out/ $OUT/$h/jobs-out/ 2>/dev/null
done
date -u +%FT%TZ > $OUT/last_collect
