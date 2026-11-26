#!/usr/bin/env python3
"""ASAP and VictoriaMetrics answers against Prometheus (exact), on the
plan's grid: for each planned query, one range request [end - span, end] at
the plan's step with end on the step grid `--behind-s` behind now, sent to
all three. Per query: steps and series compared, the largest relative
difference from Prometheus, the share of (series, step) points within the
5% target, and for topk the recall of Prometheus' series."""
import argparse
import json
import time
import urllib.parse
import urllib.request


def matrix(base, query, start, end, step):
    url = f"{base}/api/v1/query_range?" + urllib.parse.urlencode(
        {"query": query, "start": start, "end": end, "step": step})
    started = time.time()
    try:
        body = json.load(urllib.request.urlopen(url, timeout=600))
    except urllib.error.HTTPError as e:
        return None, time.time() - started, e.read()[:200].decode(errors="replace")
    out = {}
    for s in body["data"]["result"]:
        labels = tuple(sorted((k, v) for k, v in s["metric"].items() if k != "__name__"))
        for ts, v in s["values"]:
            out[(labels, float(ts))] = float(v)
    return out, time.time() - started, None


def diff(actual, exact):
    common = actual.keys() & exact.keys()
    rels = [abs(actual[k] - exact[k]) / max(abs(exact[k]), 1e-12) for k in common]
    within = sum(1 for r in rels if r <= 0.05)
    return (max(rels, default=0.0), within / len(exact) if exact else 1.0,
            len(common) / len(exact) if exact else 1.0)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asap", required=True)
    ap.add_argument("--vm", required=True)
    ap.add_argument("--exact", required=True)
    ap.add_argument("--queries", required=True, help="the live prep's queries.json")
    ap.add_argument("--behind-s", type=float, default=60)
    args = ap.parse_args()
    queries = json.load(open(args.queries))["queries"]
    print(f"{'query':58} {'arm':16} {'latency':>8} {'points':>7} {'max rel diff':>12} {'within 5%':>9} {'recall':>6}")
    for q in queries:
        step = q["range"]["step_ms"] / 1000
        span = q["range"]["span_ms"] / 1000
        end = (time.time() - args.behind_s) // step * step
        start = end - span
        exact, t_exact, err = matrix(args.exact, q["promql"], start, end, step)
        if exact is None:
            print(f"{q['promql'][:58]:58} prometheus      failed: {err}")
            continue
        print(f"{q['promql'][:58]:58} {'prometheus':16} {t_exact:7.2f}s {len(exact):7}")
        for name, base in (("asap", args.asap), ("victoriametrics", args.vm)):
            got, t, err = matrix(base, q["promql"], start, end, step)
            if got is None:
                print(f"{'':58} {name:16} {t:7.2f}s refused/failed: {err}")
                continue
            worst, within, recall = diff(got, exact)
            print(f"{'':58} {name:16} {t:7.2f}s {len(got):7} {worst:12.4%} {within:9.1%} {recall:6.1%}")


if __name__ == "__main__":
    main()
