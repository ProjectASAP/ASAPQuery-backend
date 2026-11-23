#!/bin/bash
# qrunner.sh: host-side queue runner, independent of any agent session or ssh connection.
#
# Queue:  $CAMPAIGN_DIR/queue/jobs.queue, one job per line:  <job-id>|<shell command>
#         (# comments and blank lines ignored; the file may be appended to while running).
# State:  $CAMPAIGN_DIR/queue/state/<job-id>.{running,done,failed}  and  <job-id>.log
# Status: $CAMPAIGN_DIR/queue/status  (rewritten after every state change)
#
# Jobs run one after another, in file order. A job with .done or .failed is skipped.
# A job found .running at start (runner or host died mid-job) is rerun from scratch:
# each job's command must clean its own output first.
# Single instance via flock; cron (@reboot and every 5 min) restarts it if it is not running:
#   @reboot      $CAMPAIGN_DIR/qrunner.sh --daemon
#   */5 * * * *  $CAMPAIGN_DIR/qrunner.sh --daemon
# Pause: touch $CAMPAIGN_DIR/queue/PAUSE (the runner exits before the next job).
Q=$CAMPAIGN_DIR/queue
mkdir -p $Q/state
if [ "$1" = "--daemon" ]; then
  setsid nohup "$0" --run >> $Q/runner.log 2>&1 < /dev/null &
  exit 0
fi
exec 9> $Q/runner.lock
flock -n 9 || exit 0
source $BENCH_ENV
status() {
  { echo "host=$(hostname -s) updated=$(date -u +%FT%TZ) runner_pid=$$"
    grep -v '^#' $Q/jobs.queue 2>/dev/null | grep . | while IFS='|' read -r id cmd; do
      st=queued
      for s in done failed running; do [ -f $Q/state/$id.$s ] && { st=$s; break; }; done
      echo "$st $id $( [ -f $Q/state/$id.$st ] && cat $Q/state/$id.$st)"
    done; } > $Q/status.tmp && mv $Q/status.tmp $Q/status
}
echo "$(date -u +%FT%TZ) runner start pid $$"
while true; do
  [ -f $Q/PAUSE ] && { echo "$(date -u +%FT%TZ) paused"; status; exit 0; }
  next=$(grep -v '^#' $Q/jobs.queue 2>/dev/null | grep . | while IFS='|' read -r id cmd; do
           [ -f $Q/state/$id.done ] || [ -f $Q/state/$id.failed ] || { echo "$id"; break; }; done)
  if [ -z "$next" ]; then status; echo "$(date -u +%FT%TZ) queue empty"; exit 0; fi
  cmd=$(grep -v '^#' $Q/jobs.queue | grep "^$next|" | head -1 | cut -d'|' -f2-)
  echo "start=$(date -u +%FT%TZ)" > $Q/state/$next.running
  status
  echo "$(date -u +%FT%TZ) job $next: $cmd"
  bash -c "$cmd" > $Q/state/$next.log 2>&1
  rc=$?
  if [ $rc -eq 0 ]; then
    echo "$(cat $Q/state/$next.running) end=$(date -u +%FT%TZ) rc=0" > $Q/state/$next.done
  else
    echo "$(cat $Q/state/$next.running) end=$(date -u +%FT%TZ) rc=$rc" > $Q/state/$next.failed
  fi
  rm -f $Q/state/$next.running
  echo "$(date -u +%FT%TZ) job $next rc=$rc"
  status
done
