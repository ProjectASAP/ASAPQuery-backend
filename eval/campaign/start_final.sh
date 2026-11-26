#!/bin/bash
# start_final.sh <commit>: wait until no campaign run_vm_demo.sh runs on this host, then final_run.sh.
while pgrep -f "[r]un_vm_demo.sh" >/dev/null; do sleep 30; done
exec $CAMPAIGN_DIR/final_run.sh "$1" $CAMPAIGN_DIR/jobs-final.txt
