#!/usr/bin/env python3
"""A logging pass-through in front of one Prometheus-API data source, so the
live demo sees every request Grafana sends exactly as sent.

    proxy.py --name ASAP --listen 127.0.0.1:19721 --upstream http://127.0.0.1:19711 --log demo/live/logs/proxy-asap.jsonl

Every request is forwarded unchanged (method, path, query string, body,
headers except hop-by-hop ones) and the response returned unchanged. For
/api/v1/query and /api/v1/query_range it appends one JSON line: wall time,
path, the query and its start/end/step (or time), HTTP status, the data
plane's x-asap-execution header, latency, and the error text of a non-200.
GET /proxy/metrics serves counters for the monitoring Prometheus:
  live_proxy_requests_total{datasource,path,query,status}
  live_proxy_request_seconds_sum / _count{datasource,path,query}   (answered requests, HTTP 200)
  live_proxy_last_request_seconds{datasource,path,query}            (the latest answered request)
  live_proxy_last_range_end_lag_seconds{datasource,query}   (wall time - request end)
  live_proxy_last_range_steps{datasource,query}
"""
import argparse
import http.client
import http.server
import json
import threading
import time
import urllib.parse

HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers",
       "transfer-encoding", "upgrade", "host", "content-length"}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--name", required=True)
    ap.add_argument("--listen", required=True)
    ap.add_argument("--upstream", required=True)
    ap.add_argument("--log", required=True)
    args = ap.parse_args()
    up = urllib.parse.urlsplit(args.upstream)
    lock = threading.Lock()
    counters, sums, counts, last = {}, {}, {}, {}
    log = open(args.log, "a", buffering=1)

    def esc(v):
        return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")

    def render():
        out = []
        with lock:
            for (path, query, status), v in sorted(counters.items()):
                out.append(f'live_proxy_requests_total{{datasource="{args.name}",path="{path}",query="{esc(query)}",'
                           f'status="{status}"}} {v}')
            for (path, query), v in sorted(sums.items()):
                lbl = f'datasource="{args.name}",path="{path}",query="{esc(query)}"'
                out.append(f"live_proxy_request_seconds_sum{{{lbl}}} {v}")
                out.append(f"live_proxy_request_seconds_count{{{lbl}}} {counts[(path, query)]}")
            for (name, path, query), v in sorted(last.items()):
                out.append(f'{name}{{datasource="{args.name}",path="{path}",query="{esc(query)}"}} {v}')
        return ("\n".join(out) + "\n").encode()

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def handle_any(self):
            if self.path.startswith("/proxy/metrics"):
                body = render()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            length = int(self.headers.get("Content-Length") or 0)
            req_body = self.rfile.read(length) if length else b""
            headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
            started = time.time()
            conn = http.client.HTTPConnection(up.hostname, up.port, timeout=300)
            try:
                conn.request(self.command, self.path, body=req_body or None, headers=headers)
                resp = conn.getresponse()
                body = resp.read()
                status, resp_headers = resp.status, resp.getheaders()
            except Exception as e:  # noqa: BLE001
                status, resp_headers, body = 502, [("Content-Type", "text/plain")], f"proxy: {e}".encode()
            finally:
                conn.close()
            latency = time.time() - started
            # Log before answering: a client that gave up (Grafana's data
            # source timeout) makes the write fail, and the request must
            # still be recorded, as "client_gone".
            client_gone = False
            try:
                self.send_response(status)
                for k, v in resp_headers:
                    if k.lower() not in HOP:
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except OSError:
                client_gone = True
            path = urllib.parse.urlsplit(self.path).path
            if path not in ("/api/v1/query", "/api/v1/query_range"):
                return
            params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(self.path).query))
            if self.command == "POST" and req_body:
                params.update(urllib.parse.parse_qsl(req_body.decode(errors="replace")))
            query = params.get("query", "")
            execution = dict((k.lower(), v) for k, v in resp_headers).get("x-asap-execution")
            row = {"t": round(started, 3), "ds": args.name, "path": path, "query": query,
                   "start": params.get("start"), "end": params.get("end"), "step": params.get("step"),
                   "time": params.get("time"), "status": status, "execution": execution,
                   "latency_ms": round(latency * 1e3, 2)}
            if status != 200:
                row["error"] = body[:300].decode(errors="replace")
            if client_gone:
                row["client_gone"] = True
            log.write(json.dumps(row) + "\n")
            with lock:
                key = (path, query)
                key_status = "client_gone" if client_gone else status
                counters[(path, query, key_status)] = counters.get((path, query, key_status), 0) + 1
                # Latency of answered requests only (a refusal takes ~1 ms).
                if status == 200 and not client_gone:
                    sums[key] = sums.get(key, 0.0) + latency
                    counts[key] = counts.get(key, 0) + 1
                    last[("live_proxy_last_request_seconds", path, query)] = latency
                if path == "/api/v1/query_range" and params.get("end") and params.get("step"):
                    try:
                        end, start = float(params["end"]), float(params.get("start", params["end"]))
                        step = float(params["step"].rstrip("s"))
                        last[("live_proxy_last_range_end_lag_seconds", path, query)] = started - end
                        last[("live_proxy_last_range_steps", path, query)] = (end - start) / step + 1
                    except ValueError:
                        pass

        do_GET = do_POST = handle_any

    host, port = args.listen.rsplit(":", 1)
    server = http.server.ThreadingHTTPServer((host, int(port)), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()
