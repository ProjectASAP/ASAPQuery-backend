#!/bin/bash
# collect.sh <host> <cfg>: small result files of one config into campaign-2026-11-20/<cfg>/
h=$1; cfg=$2; D=$ASAPQ_BENCH/campaign-2026-11-20/$cfg; mkdir -p $D
R=$h
rsync -a $R:$CAMPAIGN_DIR/$cfg.meta $R:$CAMPAIGN_DIR/$cfg.log $D/ 2>/dev/null
rsync -a --prune-empty-dirs --include='*/' \
  --include='*.demo_table.txt' --include='*.summary.txt' --include='*.accuracy_vs_vm.txt' --include='*.prepare.log' --include='camp*.log' \
  --include='resources.json' --include='engine_metrics_end.txt' --include='vm_flags.json' --include='report.json' \
  --include='data_plane.log' --include='feeder0-stats.json' --include='accuracy_vs_vm.json' --exclude='*' "$R:$ASAPQ_BENCH/runs/camp1120-$cfg*" $D/
# planner predictions (prep dirs used by this config's runs)
rsync -a --prune-empty-dirs --include='*/' --include='plan_joint_summary.json' --include='plan_alone.json' \
  --include='plan_failures.json' --include='queries.json' --exclude='*' $R:$ASAPQ_BENCH/prep/ $D/prep/
