#!/bin/bash
source $BENCH_ENV; cd $CAMPAIGN_DIR
while pgrep -f "[i]ngest_steps_v2.py" >/dev/null; do sleep 20; done
rm -rf $ASAPQ_BENCH/prep/ingest.*
export BIN_DIR=$BENCH_ROOT/campaign-bin-dae1ffef
STEP_S=240 python3 ingest_steps_v3.py $CAMPAIGN_DIR/c7-ingest-v3-dae1ffef dashboard-heavy,typical-dashboard-10hz 25000,50000,75000,100000,200000,400000 asap > c7-ingest-v3a.log 2>&1
NO_VIEW_PERSISTENCE=1 STEP_S=240 python3 ingest_steps_v3.py $CAMPAIGN_DIR/c7-ingest-v3-dae1ffef-nopersist dashboard-heavy,typical-dashboard-10hz 50000,75000,100000,200000,400000 asap > c7-ingest-v3b.log 2>&1
echo CHAIN_DONE >> c7-ingest-v3b.log
