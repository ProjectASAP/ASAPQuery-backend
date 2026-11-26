#!/usr/bin/env python3
"""Live accuracy for the presentation dashboard, against a reference
VictoriaMetrics that ingests the same feed (its own feeder, identical
samples) and gets no query load except these untimed reference queries, so
it keeps up and its answers are the truth in real time.

Every --every-s seconds, for every query, at the newest evaluation time T on
the query's interval grid (queries.json interval_ms) that is --behind-s behind
now: the reference's
answer (the query unchanged, rate/increase as rate_prometheus /
increase_prometheus), ASAP's and the loaded VictoriaMetrics' answers (the
query unchanged, as instant queries at T; a query with `engines` is asked only
of those, e.g. a VictoriaMetrics-only query is not sent to ASAP). Written for node-exporter's
textfile collector (--textfile):

  asap_demo_accuracy_max_rel_error{engine,query}       largest relative error over the reference's points;
                                                       a point the engine lacks counts as error 1 (100%)
  asap_demo_accuracy_within_target_ratio{engine,query} share of the reference's points within --target
  asap_demo_accuracy_unverified_total{query}           checks whose reference failed (an empty reference
                                                       answer is graded: empty is exact, any series 100%)
  (a query is checked once the reference has its whole window, its answers below are written from the
  start; an engine that gives no answer, e.g. ASAP
  warming up, is not graded for that check)
  asap_demo_accuracy_time_seconds                      T of the latest check
  asap_demo_answer{engine,query,series}                the answers at T (reference, asap, victoriametrics):
                                                       every series of a query with at most --max-series
                                                       outputs, else the reference's --top largest

It also converts the final experiment's report_data.json into the
`asap_demo_experiment_*` metrics (experiment_to_textfile.py) each round.
"""
import argparse
import concurrent.futures
import json
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

PROMETHEUS_RATE = re.compile(r"\b(rate|increase)\s*\(")


def instant(base, query, t, timeout=120):
    url = f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": query, "time": f"{t:.3f}"})
    try:
        body = json.load(urllib.request.urlopen(url, timeout=timeout))
    except Exception:  # noqa: BLE001 - no answer
        return None
    if body.get("status") != "success":
        return None
    return {tuple(sorted((k, v) for k, v in s["metric"].items() if k != "__name__")): float(s["value"][1])
            for s in body["data"]["result"]}


def esc(v):
    return str(v).replace("\\", "\\\\").replace('"', '\\"')


def series_name(labels):
    return ",".join(f"{k}={v}" for k, v in labels) or "{}"


def errors(got, ref, target):
    """(max relative error, share within target) over the reference's points. An empty reference
    answer (e.g. a filter that matches nothing) is the truth too: an empty answer is exact, any
    series is an error of 1 (100%)."""
    if not ref:
        return (0.0, 1.0) if not got else (1.0, 0.0)
    errs = []
    for k, v in ref.items():
        if got is None or k not in got:
            errs.append(1.0)
        else:
            errs.append(0.0 if got[k] == v else abs(got[k] - v) / max(abs(v), 1e-12))
    return max(errs), sum(1 for e in errs if e <= target) / len(errs)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asap", required=True)
    ap.add_argument("--vm", required=True)
    ap.add_argument("--reference", required=True, help="the reference VictoriaMetrics (no query load)")
    ap.add_argument("--queries", required=True, help="the live prep's queries.json")
    ap.add_argument("--textfile", required=True)
    ap.add_argument("--report-data", help="the final experiment's report_data.json")
    ap.add_argument("--reference-start-ms", type=int, default=0,
                    help="when the reference's feed started: a query is checked once its window has data there")
    ap.add_argument("--every-s", type=float, default=15)
    ap.add_argument("--behind-s", type=float, default=6)
    ap.add_argument("--target", type=float, default=0.05)
    ap.add_argument("--max-series", type=int, default=20)
    ap.add_argument("--top", type=int, default=5)
    args = ap.parse_args()
    queries = json.load(open(args.queries))["queries"]
    out = Path(args.textfile)
    here = Path(__file__).resolve().parent
    unverified = {q["id"]: 0 for q in queries}
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=8)
    while True:
        started = time.time()
        jobs, at = {}, {}
        for q in queries:
            every = q["interval_ms"] / 1000
            t = at[q["id"]] = (time.time() - args.behind_s) // every * every
            runs = q.get("engines", ("asap", "victoriametrics"))
            ref_q = PROMETHEUS_RATE.sub(lambda m: f"{m.group(1)}_prometheus(", q["promql"])
            jobs[q["id"]] = (pool.submit(instant, args.reference, ref_q, t),
                             pool.submit(instant, args.asap, q["promql"], t) if "asap" in runs else None,
                             pool.submit(instant, args.vm, q["promql"], t) if "victoriametrics" in runs else None)
        lines = [f"asap_demo_accuracy_time_seconds {max(at.values())}"]
        for q in queries:
            qid = esc(q["id"])
            t = at[q["id"]]
            ref, asap, vm = (f.result() if f is not None else None for f in jobs[q["id"]])
            # Graded only once the reference holds the whole window; the answers are shown from the start.
            warming = t * 1000 - q.get("window_ms", q["lookback_ms"]) < args.reference_start_ms
            if not warming:
                if ref is None:   # the reference failed (an empty answer is graded)
                    unverified[q["id"]] += 1
                lines.append(f'asap_demo_accuracy_unverified_total{{query="{qid}"}} {unverified[q["id"]]}')
            if ref is None:
                continue
            answers = {"asap": asap, "victoriametrics": vm}
            for engine, got in answers.items() if not warming else ():
                if got is None:  # refused (warming up) or failed: no answer to grade
                    continue
                worst, within = errors(got, ref, args.target)
                lines.append(f'asap_demo_accuracy_max_rel_error{{engine="{engine}",query="{qid}"}} {worst}')
                lines.append(f'asap_demo_accuracy_within_target_ratio{{engine="{engine}",query="{qid}"}} {within}')
            shown = (sorted(ref) if len(ref) <= args.max_series else sorted(ref, key=lambda k: -ref[k])[:args.top])
            for labels in shown:
                for engine, got in (("reference", ref), *answers.items()):
                    if got is not None and labels in got:
                        lines.append(f'asap_demo_answer{{engine="{engine}",query="{qid}",'
                                     f'series="{esc(q["id"] + " · " + series_name(labels))}"}} {got[labels]}')
        tmp = out.with_name("." + out.name + ".tmp")
        tmp.write_text("\n".join(lines) + "\n")
        tmp.rename(out)
        if args.report_data:
            subprocess.run([sys.executable, str(here / "experiment_to_textfile.py"), "--report-data",
                            args.report_data, "--textfile", str(out.with_name("asap_demo_experiment.prom"))],
                           check=False)
        time.sleep(max(1.0, args.every_s - (time.time() - started)))


if __name__ == "__main__":
    main()
