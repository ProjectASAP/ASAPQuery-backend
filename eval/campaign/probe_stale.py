#!/usr/bin/env python3
"""probe_stale.py <run_dir>: stale answers from the visibility probes (user 0's send of every evaluation).
victoriametrics: sum(count_over_time(<metric>[window])) at T when asked, against the replayed reference's
count (reference.jsonl ref_count; reference rows exist for the kept evaluations): stale when < 99.9%.
asap: the data plane's watermark when asked: stale when it had not reached T."""
import collections, json, sys
from pathlib import Path
run = Path(sys.argv[1])
ref = {}
rp = run / "victoriametrics" / "reference.jsonl"
if rp.exists():
    for l in open(rp):
        r = json.loads(l)
        if r.get("ref_count") is not None:
            ref[(r["query"], r["offset_ms"])] = r["ref_count"]
for arm in ("asap", "victoriametrics"):
    pp = run / arm / "probes.jsonl"
    if not pp.exists():
        continue
    n, stale, checked, frac = collections.Counter(), collections.Counter(), collections.Counter(), collections.defaultdict(list)
    for l in open(pp):
        p = json.loads(l)
        q, off = p["query"], p["offset_ms"]
        n[q] += 1
        if arm == "asap":
            vu = p.get("visible_until_offset_ms")
            if vu is None:
                continue
            checked[q] += 1
            if vu < off:
                stale[q] += 1
                frac[q].append((off - vu) / 1e3)
        else:
            rc = ref.get((q, off))
            if rc is None or p.get("count") is None:
                continue
            checked[q] += 1
            if rc > 0 and p["count"] / rc < 0.999:
                stale[q] += 1
                frac[q].append(p["count"] / rc)
    print(f"== {arm}: probes {sum(n.values())}, checked {sum(checked.values())}, stale {sum(stale.values())}")
    for q in sorted(n):
        if stale[q]:
            v = sorted(frac[q])
            what = "s behind" if arm == "asap" else "visible fraction"
            print(f"  {q}: stale {stale[q]} of {checked[q]} checked; {what} median {v[len(v) // 2]:.3f}, worst "
                  f"{(v[-1] if arm == 'asap' else v[0]):.3f}")
