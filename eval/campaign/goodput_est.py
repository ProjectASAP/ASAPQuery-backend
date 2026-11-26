#!/usr/bin/env python3
"""goodput_est.py <run_dir>: per arm, from report.json: on-time answers (200 within the interval:
answered_ok + inaccurate + unverified-because-not-kept), the accuracy pass rate over the checked
(kept) answers, and goodput estimated as on-time x pass rate per second (results are kept for every
10th evaluation, so report.py's answered_ok counts only the checked tenth)."""
import json, sys
from pathlib import Path
run = Path(sys.argv[1]); rep = json.load(open(run / "report.json"))
for arm, r in rep["arms"].items():
    c = r["classes"]; qs = r.get("query_phase_s") or 14400
    checked = c["answered_ok"] + c["inaccurate_missing_samples"] + c["inaccurate_approximation"]
    on_time = checked + c["unverified"]
    rate = c["answered_ok"] / checked if checked else None
    offered = sum(c.values())
    est = on_time * rate / qs if rate is not None else None
    print(f"{arm}: offered {offered}; on time (200 within interval) {on_time} ({100 * on_time / offered:.1f}%); "
          f"checked {checked}, within target {c['answered_ok']} ({'-' if rate is None else f'{100 * rate:.1f}%'}); "
          f"slow {c['slow']}, rejected {c['rejected']}, timeout {c['timeout']}, partial {c['partial']}; "
          f"goodput est {'-' if est is None else f'{est:.2f}'}/s ({'-' if est is None else f'{100 * est * qs / offered:.1f}%'} of offered)")
