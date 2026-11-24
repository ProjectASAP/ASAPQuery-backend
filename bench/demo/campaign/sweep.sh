#!/bin/bash
# sweep.sh <commit> <run_s> <series:users>...: VictoriaMetrics-alone scale sweep steps, one after
# another (demo_job.sh, ARMS=victoriametrics); detached from the terminal. Log: sweep.log.
commit=$1 run_s=$2; shift 2
cd $CAMPAIGN_DIR
for step in "$@"; do
  s=${step%%:*} u=${step##*:}
  tag=sweep-vm-s$s-u$u
  echo "$(date -u +%FT%TZ) start $tag" >> sweep.log
  KEEP_VM_DATA=0 ./demo_job.sh $tag $commit $s $u $run_s victoriametrics >> sweep.log 2>&1
  echo "$(date -u +%FT%TZ) end $tag rc=$?" >> sweep.log
done
echo "$(date -u +%FT%TZ) SWEEP_DONE" >> sweep.log
