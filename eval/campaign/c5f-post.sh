#!/bin/bash
$CAMPAIGN_DIR/postrun.sh c5f-seed862-s10000-instant
cd $ASAPQ_BENCH/harness
r=../runs/camp1120-c5f-seed862-s10000-instant.seed-862.workload-01.s10000; p=../prep/seed-862.workload-01.s10000
python3 report.py --prep $p --run $r --reference prometheus > $r.summary.txt 2>&1
python3 demo_table.py --prep $p --run $r > $r.demo_table.txt 2>&1
echo POSTRUN_DONE
