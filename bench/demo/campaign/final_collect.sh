#!/bin/bash
# final_collect.sh [loop]: pull the small result files of every final1121-* run from the
# campaign hosts into $ASAPQ_BENCH/final-2026-11-21/<cfg>/ (cfg = tag without final1121- and -rN).
# With "loop": repeat every 10 min until every host printed FINAL_HOST_DONE.
OUT=$ASAPQ_BENCH/final-2026-11-21
HOSTS="${BENCH_HOSTS:?set BENCH_HOSTS}"
once() {
  local alldone=1
  for h in $HOSTS; do
    R=$h
    tags=$(timeout 60 ssh -o BatchMode=yes $R 'ls $ASAPQ_BENCH/runs/ 2>/dev/null | grep -o "^final1121-[^.]*" | sort -u')
    for t in $tags; do
      cfg=${t#final1121-}; cfg=${cfg%-r[0-9]*}
      mkdir -p $OUT/$cfg
      timeout 600 rsync -a --prune-empty-dirs --include='*/' \
        --include='*.demo_table.txt' --include='*.summary.txt' --include='*.prepare.log' --include='*.log' \
        --include='*.meta' --include='*.done' --include='*.accuracy_vs_vm.txt' \
        --include='resources.json' --include='engine_metrics_end.txt' --include='vm_flags.json' --include='report.json' \
        --include='accuracy_vs_vm.json' --include='feeder0-stats.json' --include='host_meta.json' \
        --exclude='*' "$R:$ASAPQ_BENCH/runs/$t*" $OUT/$cfg/ 2>/dev/null
    done
    for t in $tags; do
      cfg=${t#final1121-}; cfg=${cfg%-r[0-9]*}
      for p in $(timeout 60 ssh -o BatchMode=yes $R "cd $ASAPQ_BENCH/runs; ls -d $t.*/ 2>/dev/null | sed 's#^$t\\.##; s#/\$##'"); do
        timeout 120 rsync -a --prune-empty-dirs --include='*/' --include='queries.json' --include='manifest.json' \
          --include='plan_joint_summary.json' --include='plan_alone.json' --include='plan_failures.json' \
          --include='plan_joint.report.json' --exclude='*' "$R:$ASAPQ_BENCH/prep/$p/" $OUT/$cfg/prep/$p/ 2>/dev/null
      done
    done
    timeout 60 ssh -o BatchMode=yes $R 'grep -q FINAL_HOST_DONE $CAMPAIGN_DIR/final.out 2>/dev/null' || alldone=0
  done
  return $((1 - alldone))
}
if [ "$1" = loop ]; then
  until once; do sleep 600; done
  echo "all hosts done $(date -u +%FT%TZ)"
else
  once
fi
