#!/bin/bash
$CAMPAIGN_DIR/postrun.sh c6f60-typical60s-s10000-instant
cd $ASAPQ_BENCH/harness
r=../runs/camp1120-c6f60-typical60s-s10000-instant.demo.typical-dashboard-60s.s10000; p=../prep/demo.typical-dashboard-60s.s10000
python3 report.py --prep $p --run $r --reference prometheus > $r.summary.txt 2>&1
python3 demo_table.py --prep $p --run $r > $r.demo_table.txt 2>&1
echo POSTRUN_DONE
