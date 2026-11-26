#!/bin/bash
# queue.sh <log-to-wait-for> <cfg> : after <log> shows VM_DEMO_DONE, launch.sh <cfg> with the caller's env.
until grep -q VM_DEMO_DONE "$1" 2>/dev/null; do sleep 30; done
sleep 15
$CAMPAIGN_DIR/launch.sh "$2"
