# Alibaba dashboard: partial real-data results (stopped)

Audience: experiment reviewers. Execution stopped at the user’s request. This is **not** the completed 54-run experiment or a three-trial aggregate. No smoke-test measurements are included.

Completed runs: **15/54**, all service workload. AutoSketch service trial 2 was interrupted and has no completed result; ERP-NoSharing and ERP trial 2 were not started. Edge and latency catalogs exist, but their held-out replays were not started. Automatic execution/publication is stopped.

## Data and queries

The first 240 complete three-minute Alibaba microservices-v2022 CallGraph archives contain **2,646,269,187** retained observations after full-row deduplication and removal of missing downstream IDs. These are call observations, not unique logical requests. Source URLs, SHA-256 hashes, malformed-row counts and exclusions are retained in `dataset-manifest.json`.

Calibration scans hours 0–6 and retains a deterministic 10,000,000-observation reservoir. Held-out replay is unsampled. Each completed service run ingests hours 5–6 for warmup and hours 6–12 for evaluation: **1,753,353,646 observations**. Native event timestamps are used, without a synthetic scrape schedule.

The dashboard has six panels: downstream-service counts and Top-3 services by count, each over 1m, 10m and 60m half-open windows. Refresh is every minute: 360 dashboard refreshes and 2160 panel queries per completed run. Minute panes are composed before TopK readout. Raw samples include each window’s event count and cardinality.

## First complete trial: measured performance

This table uses trial 0 consistently across all six baselines. CPU columns are total process-CPU seconds in timed primitive regions. Dashboard time is the mean summed wall time of composition plus all six readouts; shared merges are counted once. It is not network/client latency. Memory is retained logical payload, excluding allocator overhead and query scratch.

| Baseline | Planning s | Update CPU s | Merge CPU s | Readout CPU s | Dashboard mean ms | Retained MiB | Violations / 2160 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Exact-pane | 1.626e-06 | 66.95 | 10.35 | 4.07 | 40.06 | 15.18 | 0 |
| Exact-scan | 8.72e-07 | 7.90 | 0.00 | 2270.60 | 6307.72 | 2239.36 | 0 |
| ASAP analytical sizing | 2.1814e-05 | 1076.88 | 10.39 | 9.68 | 55.75 | 8.80 | 1102 |
| AutoSketch CPU extension | 1383.9 | 3287.68 | 10.72 | 1.15 | 32.98 | 18.43 | 378 |
| ERP no-sharing (custom) | 0.0609535 | 3289.36 | 10.69 | 1.15 | 32.89 | 18.43 | 378 |
| ERP shared-or-local (custom) → Exact fallback | 0.0565849 | 67.03 | 10.23 | 3.93 | 39.33 | 15.18 | 0 |

Important interpretation:

- AutoSketch and ERP-NoSharing maintain six independent summaries, causing six physical updates per input observation. Shared deployments update one store.
- Exact-scan updates only retain required raw keys. Its raw scan/group-by is charged to query readout; zero merge time is not zero query work.
- ERP shared-or-local selected **shared Exact fallback**, because no shared sketch met calibration accuracy. Its zero violations do not demonstrate an accurate approximate-sketch win.
- Approximate accuracy failures remain visible: query latency alone cannot establish superiority. Count targets are L1/total ≤2%; Top-3 requires tie-aware recall ≥80%.
- Update includes one hour of warmup and six held-out hours. Query/merge cover only held-out refreshes. Input decoding, oracle IO/validation, planning and eviction are excluded from these three CPU columns; separate wall/CPU/eviction values remain in raw JSON.

## Completed-run inventory

| Baseline | Completed trials |
|---|---|
| Exact-pane | 0, 1, 2 |
| Exact-scan | 0, 1, 2 |
| ASAP analytical sizing | 0, 1, 2 |
| AutoSketch CPU extension | 0, 1 |
| ERP no-sharing (custom) | 0, 1 |
| ERP shared-or-local (custom) | 0, 1 |

All completed `*-run.json` files and their plans are preserved, including trials not used in the first-trial table. The interrupted run has only a log/plan and is excluded. Do not pool the unbalanced repetition counts as a three-trial comparison.

## Offline cost and scope

| Catalog | Construction wall seconds |
|---|---:|
| service | 1712.446 |
| edge | 1370.072 |
| latency | 418.559 |

Shared calibration preparation: 119.762 wall seconds. Catalog construction is separate from online planning and is not free.

The Rust harness invokes actual ASAPPlanner ERP selection with a **custom calibration catalog** and a memory objective. It does not demonstrate nearest-shape matching against a pre-existing synthetic catalog, production backend throughput, live drift fallback, or arbitrary pane-width optimization. Analytical sizing calls planner sizing functions, not a full analytical CPU-cost optimizer. AutoSketch is a CPU adaptation with independent per-query LHS/neighbor search and explicit exact fallback; KLL/DDSketch are extensions, not paper support claims.

## Evidence and verification

Artifact validation passed for all 15 complete runs: manifest event counts, endpoint coverage, plan correspondence, finite timers, violation accounting and single charging of shared merges. Completed exact references have zero accuracy violations. Correctness fixtures are documented separately and are not performance evidence.

`partial-figure.svg` and `partial-figure.png` show the same first-trial results, not the unfinished final figure. `partial-summary.json` contains their numerical inputs. Full source provenance and executed commands are in `manifest.json`. Original archives, compact binary data and oracle caches remain outside Git; source hashes and preparation scripts make them reproducible.

Executable source revision: `c4ded012581b2b3ea6d4dae530f2569601b4c144`. Binary SHA-256: `ee7b21cfc6fb96ef9cbfe50c9c6ed91d96dd73c4fce1e9e3d2bd50caeee73d01`. No experimental source was changed during the completed measurements.
