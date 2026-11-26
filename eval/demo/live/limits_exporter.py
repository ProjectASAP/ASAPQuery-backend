#!/usr/bin/env python3
"""Enforced limits vs usage per engine (harness/cgroup_limits.sh), for the
monitoring Prometheus (GET /metrics on --listen). Memory uses the harness'
definitions (harness/run.py cgroup_memory, cgroup_peak_and_psi,
page_cache_bytes), so the dashboard and the reports agree:

  live_limit_cpu_cores{engine}                   cpu.max quota / period
  live_cgroup_cpu_seconds_total{engine}          cpu.stat usage_usec
  live_limit_memory_bytes{engine}                memory.max
  live_cgroup_memory_bytes{engine,kind}          current (memory.current); from memory.stat anon, file,
                                                 active_file, inactive_file, file_dirty, file_writeback,
                                                 shmem; kernel (memory.stat "kernel", else slab +
                                                 kernel_stack + pagetables); working_set = current -
                                                 inactive_file (floored at 0)
  live_cgroup_memory_peak_bytes{engine}          memory.peak
  live_cgroup_memory_events_total{engine,event}  oom, oom_kill, high, max (memory.events)
  live_cgroup_memory_psi_avg10{engine,kind}      memory.pressure some/full avg10 (percent)
  live_data_page_cache_bytes{engine}             the data dir's files resident in the page cache (fincore,
                                                 every --fincore-every-s)
  live_disk_size_bytes{engine}, live_disk_used_bytes{engine}   the engine's fixed-size filesystem
  live_enospc_total{engine}                      "No space left on device" lines in the engine's log
  live_data_fs_written_bytes_total{engine}       bytes the cgroup wrote to the engine's data filesystem (io.stat
                                                 wbytes of the filesystem's device); the cgroup is made anew
                                                 for each run, so this counts from the run's start

    limits_exporter.py --engine asap=/sys/fs/cgroup/asap-limits/asap,MOUNT,LOG [--engine ...]
"""
import argparse
import http.server
import os
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "harness"))
from run import cgroup_memory, cgroup_peak_and_psi, page_cache_bytes  # noqa: E402


def read(path, default=""):
    try:
        return Path(path).read_text()
    except OSError:
        return default


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--engine", action="append", required=True, metavar="NAME=CGROUP,MOUNT,LOG")
    ap.add_argument("--listen", default="127.0.0.1:19726")
    ap.add_argument("--fincore-every-s", type=float, default=60)
    args = ap.parse_args()
    engines = {}
    for e in args.engine:
        name, rest = e.split("=", 1)
        cg, mnt, log = rest.split(",")
        engines[name] = (Path(cg), mnt, Path(log))
    enospc = {n: [0, 0] for n in engines}  # count, log offset
    fincore = {}

    def fincore_loop():
        while True:
            for n, (_, mnt, _) in engines.items():
                try:
                    fincore[n] = page_cache_bytes(mnt)
                except Exception:  # noqa: BLE001 - a missed sample
                    pass
            time.sleep(args.fincore_every_s)

    threading.Thread(target=fincore_loop, daemon=True).start()

    def render():
        out = []
        for name, (cg, mnt, log) in engines.items():
            lbl = f'engine="{name}"'
            quota, _, period = read(cg / "cpu.max", "max 100000").strip().partition(" ")
            if quota != "max":
                out.append(f"live_limit_cpu_cores{{{lbl}}} {int(quota) / int(period)}")
            for line in read(cg / "cpu.stat").splitlines():
                if line.startswith("usage_usec "):
                    out.append(f"live_cgroup_cpu_seconds_total{{{lbl}}} {int(line.split()[1]) / 1e6}")
            mx = read(cg / "memory.max").strip()
            if mx and mx != "max":
                out.append(f"live_limit_memory_bytes{{{lbl}}} {mx}")
            for kind, v in (cgroup_memory(cg) or {}).items():
                if kind not in ("slab", "kernel_stack", "pagetables"):
                    out.append(f'live_cgroup_memory_bytes{{{lbl},kind="{kind}"}} {v}')
            extra = cgroup_peak_and_psi(cg)
            if "memory_peak_bytes" in extra:
                out.append(f"live_cgroup_memory_peak_bytes{{{lbl}}} {extra['memory_peak_bytes']}")
            for event, v in (extra.get("memory_events") or {}).items():
                if event in ("oom", "oom_kill", "high", "max"):
                    out.append(f'live_cgroup_memory_events_total{{{lbl},event="{event}"}} {v}')
            for kind in ("some", "full"):
                if f"psi_{kind}_avg10" in extra:
                    out.append(f'live_cgroup_memory_psi_avg10{{{lbl},kind="{kind}"}} {extra[f"psi_{kind}_avg10"]}')
            if fincore.get(name) is not None:
                out.append(f"live_data_page_cache_bytes{{{lbl}}} {fincore[name]}")
            try:
                st = os.statvfs(mnt)
                out.append(f"live_disk_size_bytes{{{lbl}}} {st.f_blocks * st.f_frsize}")
                out.append(f"live_disk_used_bytes{{{lbl}}} {(st.f_blocks - st.f_bfree) * st.f_frsize}")
            except OSError:
                pass
            try:
                dev = os.stat(mnt).st_dev
                key = f"{os.major(dev)}:{os.minor(dev)}"
                for line in read(cg / "io.stat").splitlines():
                    fields = line.split()
                    if fields and fields[0] == key:
                        wbytes = dict(f.split("=", 1) for f in fields[1:]).get("wbytes", "0")
                        out.append(f"live_data_fs_written_bytes_total{{{lbl}}} {wbytes}")
            except OSError:
                pass
            try:
                with open(log, "rb") as f:
                    f.seek(enospc[name][1])
                    chunk = f.read()
                enospc[name][1] += len(chunk)
                enospc[name][0] += chunk.count(b"No space left on device")
            except OSError:
                pass
            out.append(f"live_enospc_total{{{lbl}}} {enospc[name][0]}")
        return ("\n".join(out) + "\n").encode()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = render()
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
