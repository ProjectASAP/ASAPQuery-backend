#!/bin/bash
# QUERY_USERS=N arm_rerun.sh <commit> [--dry-run]: parity gate, then append the rerun jobs to each host's queue
# and start the host runners. Run from the local machine; nothing after it needs the local machine.
#  1. build_backend.sh <commit> on the build host (own dirs) + parity test; STOP unless "parity rc=0"
#     and every host has $BENCH_ROOT/campaign-bin-<commit>.
#  2. jobs from rerun_plan.txt (cfg|est_hours|SERIES|MODES|RANGE|WORKLOADS), 3 rounds each,
#     packed longest-first onto the 8 hosts; written to $CAMPAIGN_DIR/queue/jobs.queue.
#  3. qrunner.sh --daemon on every host (cron also restarts it every 5 min).
set -e
commit=$1; dry=${2:-}
T=$ASAPQ_BENCH/campaign-2026-11-20/tools
HOSTS="${BENCH_HOSTS:?set BENCH_HOSTS}"
if [ "$dry" != "--dry-run" ]; then
  $T/build_backend.sh $commit | tee $BENCH_ROOT/tmp/arm-rerun-build-$commit.log
  grep -q "parity rc=0" $BENCH_ROOT/tmp/arm-rerun-build-$commit.log || { echo "PARITY GATE FAILED for $commit; nothing armed"; exit 3; }
  for h in $HOSTS; do
    ssh $h "test -x $BENCH_ROOT/campaign-bin-$commit/data_plane" || { echo "$h lacks bin $commit"; exit 4; }
  done
fi
QU=${QUERY_USERS:-1}
python3 - "$commit" "$T/rerun_plan.txt" "$QU" $HOSTS > $BENCH_ROOT/tmp/rerun-assignment-$commit.txt <<'EOF'
import sys
commit, plan, users, hosts = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]
jobs = []
for line in open(plan):
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    parts = line.split("|")
    cfg, est, series, modes, rng, workloads = parts[:6]
    u = parts[6] if len(parts) > 6 and parts[6] else users  # per-config QUERY_USERS override
    for r in (1, 2, 3):
        jobs.append((float(est), cfg, r, series, modes, rng or "-", workloads, u))
load = {h: 0.0 for h in hosts}
assign = {h: [] for h in hosts}
for job in sorted(jobs, key=lambda j: -j[0]):
    h = min(hosts, key=lambda x: load[x])
    load[h] += job[0]
    assign[h].append(job)
for h in hosts:
    print(f"#HOST {h} {load[h]:.1f}h")
    for est, cfg, r, series, modes, rng, wl, u in assign[h]:
        print(f"{h}|rerun-{cfg}-r{r}|QUERY_USERS={u} $CAMPAIGN_DIR/rerun_job.sh {commit} {cfg} {r} {series} {modes} {rng} {wl}")
EOF
cat $BENCH_ROOT/tmp/rerun-assignment-$commit.txt | grep "^#HOST"
[ "$dry" = "--dry-run" ] && { echo "dry run: $BENCH_ROOT/tmp/rerun-assignment-$commit.txt"; exit 0; }
for h in $HOSTS; do
  rsync -a $T/rerun_job.sh $T/qrunner.sh $h:$CAMPAIGN_DIR/
  grep "^$h|" $BENCH_ROOT/tmp/rerun-assignment-$commit.txt | cut -d'|' -f2- | \
    ssh $h "cat >> $CAMPAIGN_DIR/queue/jobs.queue; rm -f $CAMPAIGN_DIR/queue/PAUSE; $CAMPAIGN_DIR/qrunner.sh --daemon"
done
echo "armed $commit"
