#!/usr/bin/env python3
"""The live demo's query load: N simulated users, as in the campaign's harness
(run.py QUERY_USERS): every query is sent at its own interval (queries.json
interval_ms), to every engine that runs it (a query with `engines` goes only to
those), at T the newest evaluation time on the query's interval grid that is
--delay-s behind now; user u sends its copy u/N of the interval later than
user 0. A range query (queries.json `range`) is a range request [T - span, T]
at its step, any other an instant request at T. Requests
are not throttled: a refresh does not wait for the previous one, so several
requests per engine are in flight at once. A request that takes longer than
--timeout-s counts as a timeout.

Every request is classified as in the campaign's userload2.py (goodput):
  answered_ok                 200 within the SLO (latency <= the query's interval) and the engine's
                              latest accuracy check for that query (live/accuracy.py, against the
                              reference) within 5%
  inaccurate_missing_samples  200 within the SLO, latest check outside 5%, and the engine's visible
                              fraction of the window's samples (live/ingest_exporter.py) below 0.999
  inaccurate_approximation    the same with every sample visible
  slow                        200 but over the SLO
  rejected                    HTTP 4xx/5xx (VictoriaMetrics 503/429, ASAP refusals)
  timeout                     no answer within --timeout-s
  partial                     VictoriaMetrics answered with isPartial=true
A request with no accuracy check yet counts as answered_ok when it is within the SLO.
Goodput = answered_ok per second.

GET /metrics on --listen:
  live_load_users                                   N
  live_load_requests_total{engine,query,outcome}    the classes above (query = the query's id)
  live_load_in_flight{engine}
  live_load_last_latency_seconds{engine,query}      the latest answered request
  live_load_latency_seconds_bucket / _sum / _count{engine,query,le}   answered requests (a histogram,
                                                     for p50/p99 with histogram_quantile)
Every request is also logged to --log (JSON lines: time, user, engine,
query, T, outcome, latency).
"""
import argparse
import http.server
import json
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--engine", action="append", required=True, metavar="NAME=URL")
    ap.add_argument("--queries", required=True, help="the live prep's queries.json")
    ap.add_argument("--users", type=int, default=8)
    ap.add_argument("--delay-s", type=float, default=6)
    ap.add_argument("--timeout-s", type=float, default=120)
    ap.add_argument("--listen", default="127.0.0.1:19725")
    ap.add_argument("--log", required=True)
    ap.add_argument("--accuracy-textfile", help="live/accuracy.py's output (asap_demo_accuracy_*)")
    ap.add_argument("--ingest-metrics", help="live/ingest_exporter.py's /metrics URL (live_window_fraction)")
    ap.add_argument("--target", type=float, default=0.05)
    args = ap.parse_args()
    engines = dict(e.split("=", 1) for e in args.engine)
    queries = json.load(open(args.queries))["queries"]
    lock = threading.Lock()
    counts, last, sums, in_flight = {}, {}, {}, {e: 0 for e in engines}
    BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 30, 60, 120, 300, 600)
    hist = {}  # (engine, query) -> [count per bucket]
    log = open(args.log, "a", buffering=1)
    verdict = {}   # (engine, query id) -> max relative error of the latest accuracy check
    visible = {}   # engine -> visible fraction of the window's samples

    def refresh_inputs():
        import re
        while True:
            if args.accuracy_textfile:
                try:
                    for line in open(args.accuracy_textfile):
                        m = re.match(r'asap_demo_accuracy_max_rel_error\{engine="([^"]+)",query="([^"]+)"\} (\S+)', line)
                        if m:
                            verdict[(m.group(1), m.group(2))] = float(m.group(3))
                except OSError:
                    pass
            if args.ingest_metrics:
                try:
                    text = urllib.request.urlopen(args.ingest_metrics, timeout=5).read().decode()
                    for m in re.finditer(r'live_window_fraction\{engine="([^"]+)",[^}]*\} (\S+)', text):
                        visible[m.group(1)] = float(m.group(2))
                except Exception:  # noqa: BLE001
                    pass
            time.sleep(5)

    threading.Thread(target=refresh_inputs, daemon=True).start()

    def classify(engine, q, status, latency, partial):
        if status == "timeout":
            return "timeout"
        if status != 200:
            return "rejected"
        if partial:
            return "partial"
        if latency > q["interval_ms"] / 1000:
            return "slow"
        err = verdict.get((engine, q["id"]))
        if err is None or err <= args.target:
            return "answered_ok"
        return ("inaccurate_missing_samples" if visible.get(engine, 1.0) < 0.999
                else "inaccurate_approximation")

    def send(user, engine, base, q, t):
        if q.get("range"):
            rng = q["range"]
            url = f"{base}/api/v1/query_range?" + urllib.parse.urlencode({
                "query": q["promql"], "start": f"{t - rng['span_ms'] / 1000:.3f}", "end": f"{t:.3f}",
                "step": f"{rng['step_ms'] / 1000:g}"})
        else:
            url = f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": q["promql"], "time": f"{t:.3f}"})
        with lock:
            in_flight[engine] += 1
        started = time.time()
        partial = False
        try:
            with urllib.request.urlopen(url, timeout=args.timeout_s) as r:
                head = r.read(4096)
                partial = b'"isPartial":true' in head
                while r.read(1 << 20):
                    pass
            status = 200
        except (socket.timeout, TimeoutError):
            status = "timeout"
        except urllib.error.HTTPError as e:
            status = e.code
        except urllib.error.URLError as e:
            status = "timeout" if isinstance(e.reason, (socket.timeout, TimeoutError)) else 599
        except Exception:  # noqa: BLE001 - counted as rejected
            status = 599
        latency = time.time() - started
        outcome = classify(engine, q, status, latency, partial)
        key = (engine, q["id"])  # by id: two queries may share a PromQL text (e.g. instant and range)
        with lock:
            in_flight[engine] -= 1
            counts[key + (outcome,)] = counts.get(key + (outcome,), 0) + 1
            if status == 200:
                last[key] = latency
                s_, c_ = sums.get(key, (0.0, 0))
                sums[key] = (s_ + latency, c_ + 1)
                h = hist.setdefault(key, [0] * len(BUCKETS))
                for i, b in enumerate(BUCKETS):
                    if latency <= b:
                        h[i] += 1
        log.write(json.dumps({"t": round(started, 3), "user": user, "engine": engine, "query": q["id"],
                              "eval_time": t, "status": status, "outcome": outcome,
                              "latency_s": round(latency, 4)}) + "\n")

    def user_loop(user, q):
        every = q["interval_ms"] / 1000
        offset = user * every / args.users
        targets = [(e, b) for e, b in engines.items() if e in q.get("engines", engines)]
        nxt = (time.time() // every + 1) * every + offset
        while True:
            time.sleep(max(0.0, nxt - time.time()))
            t = (time.time() - offset - args.delay_s) // every * every
            for engine, base in targets:
                threading.Thread(target=send, args=(user, engine, base, q, t), daemon=True).start()
            nxt += every

    for u in range(args.users):
        for q in queries:
            threading.Thread(target=user_loop, args=(u, q), daemon=True).start()

    def esc(v):
        return str(v).replace("\\", "\\\\").replace('"', '\\"')

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            with lock:
                lines = [f"live_load_users {args.users}"]
                lines += [f'live_load_requests_total{{engine="{e}",query="{esc(q)}",outcome="{o}"}} {v}'
                          for (e, q, o), v in sorted(counts.items())]
                lines += [f'live_load_in_flight{{engine="{e}"}} {v}' for e, v in sorted(in_flight.items())]
                lines += [f'live_load_last_latency_seconds{{engine="{e}",query="{esc(q)}"}} {v}'
                          for (e, q), v in sorted(last.items())]
                for (e, q), (sm, c) in sorted(sums.items()):
                    lbl = f'engine="{e}",query="{esc(q)}"'
                    for b, n in zip(BUCKETS, hist.get((e, q), [0] * len(BUCKETS))):
                        lines.append(f'live_load_latency_seconds_bucket{{{lbl},le="{b}"}} {n}')
                    lines.append(f'live_load_latency_seconds_bucket{{{lbl},le="+Inf"}} {c}')
                    lines.append(f'live_load_latency_seconds_sum{{{lbl}}} {sm}')
                    lines.append(f'live_load_latency_seconds_count{{{lbl}}} {c}')
            body = ("\n".join(lines) + "\n").encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    host, port = args.listen.rsplit(":", 1)
    http.server.ThreadingHTTPServer((host, int(port)), Handler).serve_forever()


if __name__ == "__main__":
    main()
