#!/bin/bash
# status_loop.sh <run_dir> <log>: append live_status every 5 min until the run's meta has end=.
run=$1 log=$2 meta=${run%.demo.demo48.s2800}.meta
while ! grep -q "^end=" "$meta" 2>/dev/null; do
  { echo "#### $(date -u +%FT%TZ)"; python3 $CAMPAIGN_DIR/live_status.py "$run" 2>&1; } >> "$log"
  sleep 300
done
echo "#### $(date -u +%FT%TZ) run ended: $(tail -1 $meta)" >> "$log"
