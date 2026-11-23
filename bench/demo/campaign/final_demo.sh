#!/bin/bash
# final_demo.sh <commit> <users> <engine_public_ip>: the final 48-core demo run on a DRIVER host
# (feeders, harness, query client, post-run reference) against an ENGINE host:
# SERIES 2800 (demo48, windows capped at 3h), ASAP then VictoriaMetrics on the same cpuset,
# each 4 h from zero (QUERY_PHASE_S=14400; results at 3 h and 4 h), full answers every 10th
# evaluation. Start it by hand.
#   driver A: final_demo.sh <commit> 1 <engine A address>
#   driver B: final_demo.sh <commit> 4 <engine B address>
commit=$1 users=$2 eip=$3 tag=${4:-final-demo48-s2800-u$2}
cd $CAMPAIGN_DIR
export ENGINE_SSH=$eip ENGINE_ADDR=$eip NET_IFACE=${NET_IFACE:?set NET_IFACE} ENGINE_SSH_OPTS="-o BatchMode=yes"
export ANSWER_RESULTS_EVERY=10 KEEP_VM_DATA=1
# Open-loop client (2026-11-23): requests go out at their scheduled time; lateness is recorded.
export QUERY_MAX_INFLIGHT=${QUERY_MAX_INFLIGHT:-4096}
./demo_job.sh $tag $commit 2800 $users 14400 asap,victoriametrics demo/demo48 > $tag.out 2>&1
