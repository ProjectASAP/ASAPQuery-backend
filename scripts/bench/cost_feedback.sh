#!/usr/bin/env bash
# Stage E: measure the data plane's operation costs on this machine, feed
# them to the Planner, and re-plan.
#
# usage: cost_feedback.sh <settings.json> <out_dir> <workload.json> [plan.json ...]
#   settings.json  the deployment's PlanSelectionSettings (its
#                  measured_operation_costs are the base table)
#   workload.json  the workload to re-plan
#   plan.json ...  installed plans whose summary families to measure; the
#                  first is also evaluated end to end for raw-window costs
#
# Writes to out_dir: measured_operation_costs.json, report.json (costs the
# table cannot hold), plan_selection_settings.json (settings with the
# measured table), and before/ and after/ with each plan and Stage 3's
# prediction. Extra control-plane flags (e.g. --deployment-capabilities)
# go in CONTROL_PLANE_FLAGS. Pin with e.g. MEASURE_CORE=45.
set -euo pipefail
settings=$1 out=$2 workload=$3; shift 3
mkdir -p "$out"/before "$out"/after
bin=$(dirname "$0")/../../target/release
python3 -c 'import json,sys; json.dump(json.load(open(sys.argv[1])).get("measured_operation_costs", []), open(sys.argv[2], "w"))' \
  "$settings" "$out/base_table.json"
plans=(); for p in "$@"; do plans+=(--plan "$p"); done
taskset -c "${MEASURE_CORE:-0}" "$bin/measure_costs" "${plans[@]}" --raw-plan "$1" \
  --base-table "$out/base_table.json" --out "$out/measured_operation_costs.json" --report "$out/report.json"
python3 -c 'import json,sys; s=json.load(open(sys.argv[1])); s["measured_operation_costs"]=json.load(open(sys.argv[2])); json.dump(s, open(sys.argv[3], "w"), indent=1)' \
  "$settings" "$out/measured_operation_costs.json" "$out/plan_selection_settings.json"
for when in before after; do
  s=$settings; [ "$when" = after ] && s=$out/plan_selection_settings.json
  "$bin/control_plane" --workload "$workload" --plan-selection-settings "$s" ${CONTROL_PLANE_FLAGS:-} \
    --out "$out/$when/plan.json" --prediction "$out/$when/prediction.json"
done
