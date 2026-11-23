#!/usr/bin/env python3
"""Live ingestion metrics of the demo engines, for the monitoring Prometheus
(GET /metrics on --listen):

  (the live_feeder_* series carry {engine,feeder}: an engine fed by several feeder shards, one
  --feeder-log per shard, has one series per shard; max by (engine) is the engine's lag)
  live_feeder_start_lag_seconds{engine}       how late the feeder started its latest logged tick
                                              (the paced feeder logs every 10th tick and every
                                              tick whose sending took over 1 s)
  live_feeder_overrun_ticks_total{engine}     ticks whose sending took over 1 s (the engine did not
                                              take one second of samples within a second)
  live_feeder_tick{engine}                    the latest logged tick
  live_feeder_offered_samples_per_second{engine}  what the feeder sends per second
  live_answer_freshness_seconds{engine}       now - the newest sample an answer can see:
                                              VictoriaMetrics `max(timestamp(<gauge>))` asked now with
                                              latency_offset=1ms (its minimum); ASAP its watermark lag (its /metrics)
  live_window_fraction{engine,query}           a planned sample count over a window (e.g.
                                              sum(count_over_time(lat[5m]))) at the newest grid time,
                                              divided by the count the feeder sent into that window
                                              (--expected): 1.0 = every sample visible
  live_window_count{engine,query}             that count itself
  live_series_value{engine,query,series}      one series of a planned per-series query at the
                                              newest grid time, from both engines (to show one
                                              lagging); the series is the reference's largest
"""
import argparse
import http.server
import json
import re
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

TICK = re.compile(r"^tick (\d+): (\d+) rows in (\d+) ms \(start lag (\d+) ms\)")
HEADER = re.compile(r"= (\d+) rows/s")


def get_json(url, timeout=60):
    return json.load(urllib.request.urlopen(url, timeout=timeout))


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--listen", default="127.0.0.1:19724")
    ap.add_argument("--feeder-log", action="append", default=[], metavar="ENGINE=PATH")
    ap.add_argument("--asap", required=True)
    ap.add_argument("--vm", required=True)
    ap.add_argument("--gauge", default="lat")
    ap.add_argument("--series-query", default="max_over_time(lat[30m])",
                    help="a planned per-series query whose one series both engines are asked for")
    ap.add_argument("--count-query", default="sum(count_over_time(lat[5m]))",
                    help="a planned query counting the samples in a window")
    ap.add_argument("--expected", type=float, default=0,
                    help="samples the feeder sends into that window (series x samples/s x seconds); 0: off")
    ap.add_argument("--step-s", type=float, default=15)
    ap.add_argument("--delay-s", type=float, default=6)
    args = ap.parse_args()
    logs = [tuple(x.split("=", 1)) for x in args.feeder_log]   # (engine, path), several per engine
    values, lock = {}, threading.Lock()
    state = {path: {"pos": 0, "overruns": 0} for _, path in logs}
    chosen = {"series": None}

    def put(name, labels, v):
        key = (name, tuple(sorted(labels.items())))
        with lock:
            values[key] = v

    def poll_feeders():
        for engine, path in logs:
            p = Path(path)
            if not p.exists():
                continue
            st = state[path]
            lbl = {"engine": engine, "feeder": p.stem}
            with open(p, "rb") as f:
                if p.stat().st_size < st["pos"]:
                    st["pos"] = 0
                f.seek(st["pos"])
                chunk = f.read()
                st["pos"] += len(chunk)
            for line in chunk.decode(errors="replace").splitlines():
                m = HEADER.search(line)
                if m and line.startswith("paced_feeder"):
                    put("live_feeder_offered_samples_per_second", lbl, float(m.group(1)))
                m = TICK.match(line)
                if m:
                    tick, _, elapsed, lag = map(int, m.groups())
                    if elapsed > 1000:
                        st["overruns"] += 1
                    put("live_feeder_start_lag_seconds", lbl, lag / 1000)
                    put("live_feeder_tick", lbl, tick)
            put("live_feeder_overrun_ticks_total", lbl, st["overruns"])

    def poll_engines():
        now = time.time()
        try:
            q = urllib.parse.urlencode({"query": f"max(timestamp({args.gauge}))", "time": f"{now:.3f}",
                                        "latency_offset": "1ms", "nocache": "1"})
            r = get_json(f"{args.vm}/api/v1/query?{q}")["data"]["result"]
            if r:
                put("live_answer_freshness_seconds", {"engine": "victoriametrics"}, now - float(r[0]["value"][1]))
        except Exception:  # noqa: BLE001 - a missed sample
            pass
        try:
            text = urllib.request.urlopen(f"{args.asap}/metrics", timeout=10).read().decode()
            m = re.search(r"^asap_watermark_lag_seconds (\S+)", text, re.M)
            if m:
                put("live_answer_freshness_seconds", {"engine": "asap", "source": "watermark lag"}, float(m.group(1)))
        except Exception:  # noqa: BLE001
            pass

    def poll_series():
        t = (time.time() - args.delay_s) // args.step_s * args.step_s
        answers = {}
        for engine, base in (("victoriametrics", args.vm), ("asap", args.asap)):
            try:
                q = urllib.parse.urlencode({"query": args.series_query, "time": f"{t:.3f}"})
                body = get_json(f"{base}/api/v1/query?{q}", timeout=120)
                answers[engine] = {tuple(sorted(s["metric"].items())): float(s["value"][1])
                                   for s in body["data"]["result"]}
            except Exception:  # noqa: BLE001
                answers[engine] = None
        if chosen["series"] is None:
            ref = answers.get("asap") or answers.get("victoriametrics")
            if ref:
                chosen["series"] = max(ref, key=ref.get)
        if chosen["series"] is None:
            return
        name = ",".join(f"{k}={v}" for k, v in chosen["series"] if k != "__name__")
        for engine, got in answers.items():
            if got:
                key = next((k for k in got if tuple(x for x in k if x[0] != "__name__")
                            == tuple(x for x in chosen["series"] if x[0] != "__name__")), None)
                if key is not None:
                    put("live_series_value", {"engine": engine, "query": args.series_query, "series": name},
                        got[key])

    def loop(fn, every):
        while True:
            try:
                fn()
            except Exception:  # noqa: BLE001
                pass
            time.sleep(every)

    def poll_counts():
        if not args.expected:
            return
        t = (time.time() - args.delay_s) // args.step_s * args.step_s
        for engine, base in (("victoriametrics", args.vm), ("asap", args.asap)):
            try:
                q = urllib.parse.urlencode({"query": args.count_query, "time": f"{t:.3f}"})
                r = get_json(f"{base}/api/v1/query?{q}", timeout=60)["data"]["result"]
            except Exception:  # noqa: BLE001 - no point this time
                continue
            if r:
                v = float(r[0]["value"][1])
                put("live_window_count", {"engine": engine, "query": args.count_query}, v)
                put("live_window_fraction", {"engine": engine, "query": args.count_query}, v / args.expected)

    for fn, every in ((poll_feeders, 1), (poll_engines, 5), (poll_series, args.step_s), (poll_counts, args.step_s)):
        threading.Thread(target=loop, args=(fn, every), daemon=True).start()

    def esc(v):
        return str(v).replace("\\", "\\\\").replace('"', '\\"')

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            with lock:
                items = sorted(values.items())
            body = "".join(f"{n}{{{','.join(f'{k}={chr(34)}{esc(v)}{chr(34)}' for k, v in lbl)}}} {v}\n"
                           for (n, lbl), v in items).encode()
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
