# ERP Figure 1 runner

`run.py` is the experiment boundary for the five Figure 1 arms:

1. `autosketch_per_query`
2. `planner_analytical`
3. `planner_erp`
4. `asap_no_sharing`
5. `exact`

The manifest pins a dataset SHA-256 and one candidate space, memory budget,
accuracy requirement, and window workload. Each arm runs in a separate process.
The runner measures wall time, user/system CPU, and peak RSS with GNU `time`;
the arm reports its selected plan, retained state bytes, measured error, and any
additional metrics. A successful arm must echo the exact contract from
`ASAP_FIGURE1_CONTRACT_JSON`; the dataset path is supplied in
`ASAP_FIGURE1_DATASET`. Contract drift aborts the experiment rather than
producing a comparison.

```sh
python3 tools/erp-figure1/run.py \
  --manifest /path/to/immutable-figure1-manifest.json \
  --output /path/to/new-results.json
python3 -m unittest tools/erp-figure1/test_run.py -v
```

Each command must print one JSON object:

```json
{
  "contract": {"the exact object supplied in ASAP_FIGURE1_CONTRACT_JSON": true},
  "selected_plan": {"family": "cms", "width": 1024, "depth": 5},
  "metrics": {"state_bytes": 40960, "max_error": 0.007},
  "provenance": {"revision": "..."}
}
```

The runner does not contain sketch, Planner, or AutoSketch implementations. The
arm commands must invoke those production/reference implementations. Failed
arms remain in the output and make the command fail. Do not publish a Figure 1
from smoke adapters or from arms that merely relabel a fixed CLI configuration.
