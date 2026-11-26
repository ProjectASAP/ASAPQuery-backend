#!/usr/bin/env python3
"""ASAP accuracy against VictoriaMetrics as ground truth, for one finished run.

Adopted from the 2026-11-20 campaign's tools/accuracy_vs_vm.py (same method
and table); additions: it writes the per-evaluation reference answers that
report.py reads, handles range runs, and maps evaluation times between arms
that ran one after another (each arm has its own feed start).

usage: accuracy_vs_vm.py <run dir> <prep dir> [vm binary]

Ground truth per (query, evaluation): asked AFTER the run, untimed, of a
VictoriaMetrics started on the run's stored data (run.py keeps it:
KEEP_VM_DATA=1, the default), at VictoriaMetrics' own evaluation time: the
query unchanged, except rate/increase -> rate_prometheus/increase_prometheus
(PromQL's extrapolation). VictoriaMetrics' in-run answers are never the
truth (under query load its ingestion can fall behind, so an in-run answer
can miss recent samples); they are compared with the truth like ASAP's. A
reference that fails is "unverified".
Series are matched on labels without __name__ (range runs: per series and step).

Writes <run>/accuracy_vs_vm.json (the table's rows), and
<run>/victoriametrics/reference.jsonl (one line per evaluation: query,
offset_ms, promql, status, result), which report.py uses as the ground truth.
"""
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

run, prep = Path(sys.argv[1]), Path(sys.argv[2])
VM = sys.argv[3] if len(sys.argv) > 3 else os.environ["BIN_DIR"] + "/victoria-metrics-v1.156.0"
PORT = 19777
TARGET = 0.05
RATE = re.compile(r"\b(rate|increase)\(")
# rate(m[w]) / increase(m[w]) with a plain selector and a literal window (no offset).
RATE_CALL = re.compile(r"\b(rate|increase)\(\s*([a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[^}]*\})?)\[(\d+)([smhd])\]\s*\)")
UNIT_S = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def extrapolated(func, sel, w):
    """PromQL semantics (Prometheus `extrapolatedRate`, the semantics the Planner's
    exact plans implement) as a MetricsQL expression VictoriaMetrics evaluates on the
    kept raw samples (2026-11-21): the window's increase (last - first, counter resets
    corrected: increase_prometheus) extrapolated toward the window edges by at most
    1.1x the average sample spacing (else half of it), and toward zero for a counter;
    divided by the window for rate. VictoriaMetrics' rate_prometheus does not
    extrapolate. Series with fewer than 2 samples give no result, as in Prometheus."""
    r = f"increase_prometheus({sel}[{w}s])"
    f, tf = f"first_over_time({sel}[{w}s])", f"tfirst_over_time({sel}[{w}s])"
    tl, n = f"tlast_over_time({sel}[{w}s])", f"count_over_time({sel}[{w}s])"
    sampled = f"({tl} - {tf})"
    avg = f"({sampled} / ({n} - 1))"
    d_start0 = f"({tf} - (time() - {w}))"
    d_zero = f"(({sampled} * {f} / {r}) if ({r} > 0))"
    d_start = f"((({d_start0} <= {d_zero}) default {d_zero}) default {d_start0})"
    d_end = f"(time() - {tl})"
    thr = f"({avg} * 1.1)"
    ext = (f"({sampled} + (({d_start} < {thr}) default ({avg} / 2))"
           f" + (({d_end} < {thr}) default ({avg} / 2)))")
    inc = f"(({r} * {ext} / {sampled}) if ({n} >= 2))"
    return inc if func == "increase" else f"({inc} / {w})"


def promql_truth(promql):
    """The query with every rate/increase replaced by its PromQL-extrapolated form."""
    return RATE_CALL.sub(lambda m: extrapolated(m.group(1), m.group(2), int(m.group(3)) * UNIT_S[m.group(4)]), promql)
# The first range-selected metric of the query (2026-11-23: any metric, not only data/data_total),
# whose sample count the visibility probe compares; same expression as run.py's probe.
METRIC = re.compile(r"(?<![\w:])([a-zA-Z_:][a-zA-Z0-9_:]*)(?=\s*(?:\{[^}]*\})?\s*\[)")


def answers(arm):
    out = {}
    p = run / arm / "answers.jsonl"
    if not p.exists():
        return out
    for line in open(p):
        a = json.loads(line)
        # QUERY_USERS: one answer per evaluation is scored, user 0's (report.py scores every user's).
        # Answers whose result was not kept (run.py ANSWER_RESULTS_EVERY) are not checked.
        if a.get("user", 0) == 0 and not (a.get("result") or {}).get("omitted"):
            out[(a["query"], a["offset_ms"])] = a
    return out


def start_ms(arm):
    try:
        return json.load(open(run / arm / "resources.json"))["start_ms"]
    except (OSError, KeyError, ValueError):
        return None


def points(result, origin_ms):
    """{key: value}: (labels) for a vector, (labels, step offset ms) for a matrix."""
    res = (result or {}).get("data", {})
    out = {}
    for r in res.get("result", []):
        labels = tuple(sorted((k, v) for k, v in r["metric"].items() if k != "__name__"))
        if res.get("resultType") == "matrix":
            for ts, v in r["values"]:
                out[(labels, round(float(ts) * 1000) - origin_ms)] = float(v)
        else:
            out[labels] = float(r["value"][1])
    return out


def answered(a, origin_ms):
    if not a or a.get("status") != 200 or (a.get("result") or {}).get("status") != "success":
        return None
    return points(a["result"], origin_ms)


def compare(asap, ref):
    """max relative error over the reference's points; points ASAP lacks or adds count as misses."""
    errs, missing = [], 0
    for k, v in ref.items():
        if k not in asap:
            missing += 1
            continue
        a = asap[k]
        errs.append(0.0 if a == v else abs(a - v) / max(abs(v), 1e-12))
    extra = sum(1 for k in asap if k not in ref)
    return (max(errs) if errs else 0.0), missing, extra


def main():
    queries = {q["id"]: q for q in json.load(open(prep / "queries.json"))["queries"]}
    A, V = answers("asap"), answers("victoriametrics")
    a0, v0 = start_ms("asap"), start_ms("victoriametrics")
    # The asapquery arm (ASAPQuery's engine), when the run has one, is checked
    # against the same references (accuracy_vs_vm.asapquery.json, second table).
    Q, q0 = answers("asapquery"), start_ms("asapquery")
    # The vmcluster arm (2026-11-22): its evaluations get reference rows too (report.py classifies it).
    C = answers("vmcluster")
    # With an independent reference (REFERENCE_DATA) the victoriametrics arm alone is scored too.
    if v0 is None or (a0 is None and q0 is None and not os.environ.get("REFERENCE_DATA")):
        print("needs the victoriametrics arm and the asap or asapquery arm")
        return
    rate_queries = {qid for qid, q in queries.items() if RATE.search(q["promql"])}
    # Safety net: the newest sample time the VictoriaMetrics arm's feeders
    # actually sent (start_ms + 1 s per completed tick, the earliest over its
    # feeders). An evaluation after it has an incomplete reference (a feeder
    # that fell behind and was stopped): it is unverified, not compared.
    sent_until_ms = None
    for f in sorted((run / "victoriametrics").glob("feeder*-stats.json")):
        try:
            st = json.load(open(f))
            until = st["start_ms"] + 1000 * len(st.get("ticks") or [])
            sent_until_ms = until if sent_until_ms is None else min(sent_until_ms, until)
        except Exception:  # noqa: BLE001 - no stats: no safety net
            pass
    # And what VictoriaMetrics itself had at the end (run.py finish_reference_feed):
    # its newest sample per metric; none (e.g. it was OOM-killed) leaves every
    # evaluation unverified.
    try:
        rf = json.load(open(run / "victoriametrics" / "resources.json")).get("reference_feed") or {}
    except (OSError, ValueError):
        rf = {}
    rf_until = None
    if rf:
        newest = list((rf.get("newest_sample_ms") or {}).values())
        rf_until = min(newest) if newest and all(isinstance(v, int) for v in newest) else None
    data = run / "victoriametrics" / "victoria-metrics-data"
    # REFERENCE_DATA (2026-11-23): an independent reference store, the same seeded feed
    # replayed offline (no query load) from the victoriametrics arm's start; its feed is
    # complete (REFERENCE_SENT_UNTIL_MS, from the replay's feeder stats), so the arm's own
    # safety nets do not apply.
    if os.environ.get("REFERENCE_DATA"):
        data = Path(os.environ["REFERENCE_DATA"])
        sent_until_ms = int(os.environ["REFERENCE_SENT_UNTIL_MS"]) if os.environ.get("REFERENCE_SENT_UNTIL_MS") else None
        rf_until = None
        print(f"reference: replayed store {data}, fed up to {sent_until_ms}")
    vm = None
    if not data.exists():
        print(f"{data} was not kept (KEEP_VM_DATA=0?): every point is unverified")
    else:
        vm = subprocess.Popen([VM, f"-storageDataPath={data}", f"-httpListenAddr=127.0.0.1:{PORT}",
                               "-retentionPeriod=100y", "-search.maxQueryDuration=1h", "-search.latencyOffset=0s",
                               "-search.maxSamplesPerQuery=100000000000", "-search.maxSamplesPerSeries=1000000000",
                               "-search.disableCache", "-loggerLevel=ERROR"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(240):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{PORT}/health", timeout=2)
                break
            except Exception:  # noqa: BLE001
                time.sleep(0.5)
        # The reference's own newest data: the earliest last sample over every series
        # of every metric (any names), from the kept data itself; it caps verification
        # together with the feeder's ticks (run.py's reference_feed is used if this
        # fails). None (no data) leaves every evaluation unverified.
        try:
            # At the end of the feed (the data are hours old when this runs later).
            feed_end = max([sent_until_ms or 0] + [v0 + off for (_, off) in V] + [v0 + off for (_, off) in A]) + 120_000
            # Per metric name (2026-11-23): one query over every series exceeds the
            # unique-series limit at demo scale (HTTP 422), which left every point unverified.
            try:
                names = sorted(json.load(open(prep / "feeder_table.json"))["workloads"][0]["e2e"]["metrics"])
            except Exception:  # noqa: BLE001
                names = []
            lasts = []
            for name in names or [None]:
                sel = f'{{__name__="{name}"}}' if name else '{__name__!=""}'
                url = f"http://127.0.0.1:{PORT}/api/v1/query?" + urllib.parse.urlencode(
                    {"query": f"min(tlast_over_time({sel}[1h]))", "time": f"{feed_end / 1000:.3f}"})
                res = json.load(urllib.request.urlopen(url, timeout=600))["data"]["result"]
                lasts.append(int(float(res[0]["value"][1]) * 1000) if res else v0)
            own_until = min(lasts)
        except Exception:  # noqa: BLE001
            own_until = rf_until if rf_until is not None else v0
        sent_until_ms = own_until if sent_until_ms is None else min(sent_until_ms, own_until)

    def ref_query(q, offset_ms):
        """_ref_query, plus (fairness, 2026-11-21) the reference's sample count over
        the query's window at T (`ref_count`, against which a loaded engine's
        visible fraction is computed) and, for rate/increase instant queries, the
        answer in VictoriaMetrics' own MetricsQL (`result_vm`), against which the
        victoriametrics arm's answers are classified."""
        row = _ref_query(q, offset_ms)
        if row.get("status") != 200:
            return row
        t = (v0 + offset_ms) / 1000
        m = METRIC.search(q["promql"])
        try:
            if m:
                url = f"http://127.0.0.1:{PORT}/api/v1/query?" + urllib.parse.urlencode(
                    {"query": f"sum(count_over_time({m.group(1)}[{q['window_ms'] // 1000}s]))", "time": f"{t:.3f}"})
                res = json.load(urllib.request.urlopen(url, timeout=3600))["data"]["result"]
                row["ref_count"] = float(res[0]["value"][1]) if res else 0.0
            if RATE.search(q["promql"]) and not q.get("range"):
                url = f"http://127.0.0.1:{PORT}/api/v1/query?" + urllib.parse.urlencode(
                    {"query": q["promql"], "time": f"{t:.3f}"})
                row["result_vm"] = json.load(urllib.request.urlopen(url, timeout=3600))
        except Exception as e:  # noqa: BLE001 - recorded
            row["extra_error"] = str(e)[:200]
        return row

    def _ref_query(q, offset_ms):
        """The *_prometheus answer at VictoriaMetrics' own evaluation time for this offset."""
        # Ground truth for ASAP (PromQL semantics): rate/increase extrapolated as
        # Prometheus does (promql_truth); every other function as VictoriaMetrics
        # computes it. Recorded per row as `truth`.
        promql = promql_truth(q["promql"]) if RATE_CALL.search(q["promql"]) else q["promql"]
        t = (v0 + offset_ms) / 1000
        row = {"query": q["id"], "offset_ms": offset_ms, "promql": promql,
               "truth": ("promql_extrapolated_rate (Prometheus extrapolatedRate in MetricsQL over the kept raw samples)"
                         if RATE_CALL.search(q["promql"]) else "victoriametrics")}
        if q.get("range"):
            # Sampled steps: a full query_range reference (241 steps of 1h windows)
            # takes minutes per request; verify the first, middle and last step of
            # each range request as untimed instant queries at those step times.
            rng = q["range"]
            n = round(rng["span_ms"] / rng["step_ms"])
            first = t - rng["span_ms"] / 1000
            steps = [first, first + (n // 2) * rng["step_ms"] / 1000, t]
            if sent_until_ms is not None and t * 1000 > sent_until_ms:
                row.update(status=None, result={"error": f"unverified: the VictoriaMetrics feeder sent samples "
                                                         f"only up to {sent_until_ms} ms, before T"})
                return row
            by_series = {}
            try:
                for ts in steps:
                    url = f"http://127.0.0.1:{PORT}/api/v1/query?" + urllib.parse.urlencode(
                        {"query": promql, "time": f"{ts:.3f}"})
                    body = json.load(urllib.request.urlopen(url, timeout=3600))
                    for r in body["data"]["result"]:
                        key = json.dumps(r["metric"], sort_keys=True)
                        by_series.setdefault(key, {"metric": r["metric"], "values": []})["values"].append(
                            [ts, r["value"][1]])
                row.update(status=200, sampled_steps=[round(ts * 1000) for ts in steps],
                           result={"status": "success", "data": {"resultType": "matrix",
                                                                 "result": list(by_series.values())}})
            except Exception as e:  # noqa: BLE001
                row.update(status=None, result={"error": str(e)[:300]})
            return row
        params, path = {"query": promql, "time": f"{t:.3f}"}, "query"
        url = f"http://127.0.0.1:{PORT}/api/v1/{path}?" + urllib.parse.urlencode(params)
        if sent_until_ms is not None and t * 1000 > sent_until_ms:
            row.update(status=None, result={"error": f"unverified: the VictoriaMetrics feeder sent samples "
                                                     f"only up to {sent_until_ms} ms, before T"})
            return row
        try:
            row.update(status=200, result=json.load(urllib.request.urlopen(url, timeout=3600)))
        except Exception as e:  # noqa: BLE001
            row.update(status=None, result={"error": str(e)[:300]})
        return row

    rows_by_arm = {"asap": {}, "asapquery": {}}
    try:
        refs = {}
        with open(run / "victoriametrics" / "reference.jsonl", "w") as out:
            # Every evaluation either arm made.
            for (qid, off) in sorted(set(V) | set(A) | set(Q) | set(C)):
                refs[(qid, off)] = ref_query(queries[qid], off)
                out.write(json.dumps(refs[(qid, off)]) + "\n")
        for arm, arm_answers, arm0 in (("asap", A, a0), ("asapquery", Q, q0)):
            rows = rows_by_arm[arm]
            for (qid, off), a in sorted(arm_answers.items()):
                r = rows.setdefault(qid, {"n": 0, "asap_failed": 0, "unverified": 0, "within": 0, "max_err_vm": 0.0,
                                          "max_err_ref": None, "missing": 0, "extra": 0,
                                          "ref": "PromQL extrapolated (raw samples)" if qid in rate_queries else "vm post-run"})
                r["n"] += 1
                asap = answered(a, arm0)
                if asap is None:
                    r["asap_failed"] += 1
                    continue
                vv = answered(V.get((qid, off)), v0)
                smp = (refs.get((qid, off)) or {}).get("sampled_steps")
                if vv is not None and smp:
                    vv = {k: v for k, v in vv.items() if k[1] in {ms - v0 for ms in smp}}
                if vv is not None:
                    err_vm, _, _ = compare(asap, vv)
                    r["max_err_vm"] = max(r["max_err_vm"], err_vm)
                ref = refs.get((qid, off))
                ref = answered(ref, v0) if ref else None
                if not ref:  # no answer, or no series (e.g. data not kept): unverified
                    r["unverified"] += 1
                    if sent_until_ms is not None and (v0 + off) > sent_until_ms:
                        r["unverified_feed"] = r.get("unverified_feed", 0) + 1
                    continue
                sampled = refs.get((qid, off), {}).get("sampled_steps")
                if sampled:  # range: compare only at the reference's sampled steps
                    offs = {ms - v0 for ms in sampled}
                    asap = {k: v for k, v in asap.items() if k[1] in offs}
                    r["sampled_steps"] = r.get("sampled_steps", 0) + len(offs)
                    r["ref"] = r["ref"].replace("vm post-run", "vm post-run, sampled steps", 1) \
                        if "sampled" not in r["ref"] else r["ref"]
                err, miss, extra = compare(asap, ref)
                r["max_err_ref"] = max(r["max_err_ref"] or 0.0, err)
                r["missing"] += miss
                r["extra"] += extra
                r["within"] += int(err <= TARGET and miss == 0)
    finally:
        if vm:
            vm.terminate()
            vm.wait()
    (run / "accuracy_vs_vm.json").write_text(json.dumps(rows_by_arm["asap"], indent=1))
    if q0 is not None:
        (run / "accuracy_vs_vm.asapquery.json").write_text(json.dumps(rows_by_arm["asapquery"], indent=1))
    feed_unv = sum(r.get("unverified_feed", 0) for r in rows_by_arm["asap"].values())
    print(f"VictoriaMetrics feeder sent up to {sent_until_ms} ms; evaluations after it (unverified, "
          f"incomplete reference): {feed_unv} of the asap arm's")
    for arm in ("asap", "asapquery"):
        rows = rows_by_arm[arm]
        if not rows:
            continue
        print(f"\n{arm} arm (\"asap failed\" = this arm's failed requests)")
        print("| query | ground truth | n | asap failed | unverified | within 5% | max err vs ground truth | "
              "max err vs VM in-run answer | points missing/extra |")
        print("|---|---|---|---|---|---|---|---|---|")
        for qid, r in rows.items():
            gt = r["max_err_ref"]
            if r.get("sampled_steps"):
                r["ref"] += f" ({r['sampled_steps']} steps)"
            print(f"| {qid} | {r['ref']} | {r['n']} | {r['asap_failed']} | {r['unverified']} | {r['within']} | "
                  f"{'-' if gt is None else f'{gt:.2%}'} | {r['max_err_vm']:.2%} | {r['missing']}/{r['extra']} |")


if __name__ == "__main__":
    main()
