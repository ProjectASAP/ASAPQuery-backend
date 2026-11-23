#!/bin/bash
# confirm_vm.sh: the 3.5 h VictoriaMetrics-alone confirmation run at SERIES 2800 (scale ~0.07x),
# 1 user, from zero (3 h window fill + 0.5 h steady state), on the engine host with feeders here.
# Start only after the coordinator confirms. Detached: nohup setsid ./confirm_vm.sh &
cd $CAMPAIGN_DIR
ANSWER_RESULTS_EVERY=10 KEEP_VM_DATA=0 ./demo_job.sh confirm-vm-s2800-u1 a7b4e8f0 2800 1 12600 victoriametrics \
  > confirm-vm-s2800-u1.out 2>&1
