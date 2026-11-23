#!/usr/bin/env python3
"""plan_sharing.py <plan.json>...: per plan, queries (roots), plan nodes, and nodes shared by >= 2 queries
(a node serves a query when the query's root reaches it through producer edges), split by when the node
runs (ingest-time precompute vs query time), plus the most-shared nodes. --json writes one JSON line each."""
import collections
import json
import sys

def kind(n):
    p = n["payload"]
    if isinstance(p, dict):
        outer = next(iter(p)); inner = p[outer]
        return f"{outer}:{next(iter(inner)) if isinstance(inner, dict) else inner}"
    return str(p)

out_json = "--json" in sys.argv
for path in [a for a in sys.argv[1:] if a != "--json"]:
    p = json.load(open(path)); d = p["dag"]
    nodes = {n["id"]: n for n in d["nodes"]}
    producers = collections.defaultdict(list)
    for e in d["edges"]:
        producers[e["consumer"]].append(e["producer"])
    serves = collections.defaultdict(set)
    for qi, root in enumerate(d["roots"]):
        stack, seen = [root], set()
        while stack:
            x = stack.pop()
            if x in seen:
                continue
            seen.add(x); serves[x].add(qi)
            stack += producers[x]
    shared = {n: q for n, q in serves.items() if len(q) >= 2}
    by_timing = collections.Counter((nodes[n].get("output_state") or {}).get("timing", "?") for n in shared)
    top = sorted(shared.items(), key=lambda kv: -len(kv[1]))[:6]
    rec = {"plan": path, "queries": len(d["roots"]), "nodes": len(nodes), "shared_nodes": len(shared),
           "shared_by_timing": dict(by_timing), "max_sharing": max((len(q) for q in shared.values()), default=0),
           "mean_queries_per_node": round(sum(len(q) for q in serves.values()) / max(1, len(serves)), 2),
           "top": [{"node": n, "kind": kind(nodes[n]), "timing": (nodes[n].get("output_state") or {}).get("timing"),
                    "queries": len(q)} for n, q in top]}
    if out_json:
        print(json.dumps(rec))
    else:
        print(f"{path}: {rec['queries']} queries, {rec['nodes']} nodes, {rec['shared_nodes']} shared by >=2 queries "
              f"{rec['shared_by_timing']}, max {rec['max_sharing']} queries on one node")
        for t in rec["top"]:
            print(f"    node {t['node']} {t['kind']} ({t['timing']}): {t['queries']} queries")
