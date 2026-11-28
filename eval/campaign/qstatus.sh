#!/bin/bash
# qstatus.sh: one line per job on every campaign host (live, over ssh).
for h in ${BENCH_HOSTS:?set BENCH_HOSTS}; do
  timeout 20 ssh -o BatchMode=yes $h 'cat $CAMPAIGN_DIR/queue/status 2>/dev/null' | sed "s/^/$h  /"
done
