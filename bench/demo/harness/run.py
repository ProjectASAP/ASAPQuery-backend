#!/usr/bin/env python3
"""Run one prepared workload on the arms and record what each did.

Arms:
  prometheus       Prometheus 3.5.0 alone, fed by Remote Write (the exact
                   reference every other arm's answers are compared with).
  asapquery        ASAPQuery's query engine with its own planner configs, and
                   the Prometheus it forwards unsupported queries to.
  backend          ASAPQuery-backend's data plane installing ASAPPlanner's
                   plan, and the Prometheus it forwards to and reads raw data from.
  asap             the data plane alone (`--no-forward`, no Prometheus beside
                   it): every answer comes from ASAPPlanner's plan.
  victoriametrics  VictoriaMetrics single-node, no stream aggregation, fed by
                   Remote Write and queried with /api/v1/query.

Every arm gets the same data: ASAPQuery's paced feeder (same table and seed,
one feeder per sink) sends one second of event time per wall-clock second,
starting at an aligned time S. Queries run at explicit evaluation times on
each query's own interval grid, from S + (longest lookback) for --rounds
rounds of the longest interval, each sent DELAY_S after its evaluation
time. Arms run one after another unless --concurrent.

Per arm it writes answers.jsonl (every evaluation: offset from S, latency,
status, who answered, result) and resources.json (CPU core-seconds and peak
RSS of every process, forwards, load average).
"""
import argparse
import concurrent.futures
import http.server
import json
import os
import shutil
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import resource
import statistics
import urllib.request
from pathlib import Path

from common import BIN, VICTORIAMETRICS_VERSION, read_json, write_json

CLOCK_TICKS = os.sysconf("SC_CLK_TCK")
ARM_PORTS = {"prometheus": 19100, "asapquery": 19200, "backend": 19300, "backend_alone": 19400,
             "victoriametrics": 19500, "asap": 19600, "vmcluster": 19700, "asap_forward": 19800}
# The VictoriaMetrics arms: single-node, and the cluster (vmstorage, vmselect,
# vminsert; 2026-11-22, scaling) under the same total budget.
VM_ARMS = ("victoriametrics", "vmcluster")
# The plan the `asap` arm installs (the workload planned jointly).
BACKEND_PLANS_ASAP = "plan.json"
# The plan each backend arm installs: the workload planned jointly, or each
# query planned alone and the plans merged.
BACKEND_PLANS = {"backend": "plan.json", "backend_alone": "plan_alone_merged.json"}
SEED = 862
# How long after an evaluation time its query is sent: each engine needs its
# panes or windows closed by then, and still kept. ASAPQuery: its pilot's
# freshness delay (10 s) over its 5 s allowed lateness, with as much
# retention slack. Backend: its pane-close lateness is set to 2 s (the
# feeder's ticks span 1 s) and it keeps one pane beyond the plan's
# retention, so a query must arrive within two panes of its closing.
# Every arm's query is sent QUERY_DELAY_S (default 6 s, decided for the
# comparison) after its evaluation time; ASAPQuery needs at least its 10 s
# freshness delay. VictoriaMetrics makes a sample searchable ~1-2 s after it
# is received; its latency offset is kept 1 s below the delay.
QUERY_DELAY = float(os.environ.get("QUERY_DELAY_S", "6"))
DELAY_S = {"prometheus": QUERY_DELAY, "asapquery": max(10.0, QUERY_DELAY), "backend": QUERY_DELAY,
           "backend_alone": QUERY_DELAY, "victoriametrics": QUERY_DELAY, "asap": QUERY_DELAY,
           "vmcluster": QUERY_DELAY, "asap_forward": QUERY_DELAY}
# asapquery arm (2026-11-20, env-gated): ASAPQUERY_DELAY_S overrides its
# 10 s floor; ASAPQUERY_FALLBACK=victoriametrics forwards to its own
# VictoriaMetrics (VM_FLAGS) instead of Prometheus; ASAPQUERY_ALLOWED_LATENESS_MS
# and ASAPQUERY_NUM_WORKERS set its precompute engine (num_workers defaults to
# the arm's cpuset size when the fallback is VictoriaMetrics);
# data_ingestion_interval_ms then follows the manifest's scrape interval.
if os.environ.get("ASAPQUERY_DELAY_S"):
    DELAY_S["asapquery"] = float(os.environ["ASAPQUERY_DELAY_S"])
ASAPQUERY_FALLBACK = os.environ.get("ASAPQUERY_FALLBACK", "prometheus")
# VictoriaMetrics serves an instant query at `time` within -search.latencyOffset
# of now at now - latencyOffset (default 30 s, which hides the newest 30 s).
# 2026-11-21 (fairness): it is set to the uniform query delay (6 s), so VM is
# not penalised for a default ASAP was not given.
VM_LATENCY_OFFSET_S = max(1, int(round(DELAY_S["victoriametrics"])))
# VictoriaMetrics flags: DEFAULTS for everything that protects it (the
# concurrency limit -search.maxConcurrentRequests, -search.maxQueueDuration,
# -search.maxQueryDuration, per-query sample/series/memory caps), so its
# self-protection is real and is measured (rejections, timeouts); only the
# latency offset (above), the retention and the log level are set. No
# -streamAggr.config. Caches at defaults; vm_flags.json records /flags.
# VM_MAX_QUERY_DURATION (2026-11-22, scaling; e.g. 600s): -search.maxQueryDuration
# (default 30 s), so latencies past 30 s are measured rather than cut off; set
# with QUERY_TIMEOUT_S to the same value. Unset: VictoriaMetrics' default.
VM_MAX_QUERY_DURATION = os.environ.get("VM_MAX_QUERY_DURATION")
VM_SEARCH_FLAGS = [f"-search.latencyOffset={VM_LATENCY_OFFSET_S}s"] + (
    [f"-search.maxQueryDuration={VM_MAX_QUERY_DURATION}"] if VM_MAX_QUERY_DURATION else [])
VM_FLAGS = ["-retentionPeriod=1d", *VM_SEARCH_FLAGS, "-loggerLevel=WARN"]
# The cluster's components: vmstorage keeps the data (retention), vmselect
# answers queries (the search flags above), vminsert takes Remote Write.
VMCLUSTER_FLAGS = {"vmstorage": ["-retentionPeriod=1d", "-loggerLevel=WARN"],
                   "vmselect": [*VM_SEARCH_FLAGS, "-loggerLevel=WARN"],
                   "vminsert": ["-loggerLevel=WARN"]}
# VMCLUSTER_LAYOUT: component:instances:cores per instance, comma-separated; the
# cores add up to the arm's cpuset, and each instance's cgroup gets memory (and
# vmstorage the disk) in proportion to its cores. Default: the 12-core split.
VMCLUSTER_LAYOUT = os.environ.get("VMCLUSTER_LAYOUT", "vmstorage:1:6,vmselect:1:4,vminsert:1:2")
# The client's timeout per request (seconds; default 60).
QUERY_TIMEOUT_S = float(os.environ.get("QUERY_TIMEOUT_S", "60"))
# Engines on another host (2026-11-22, scaling): ENGINE_SSH is the ssh target
# of the engine host (ENGINE_SSH_OPTS its ssh options), ENGINE_ADDR the address
# its engines listen on. This harness, the feeders and the query client stay on
# this host and reach the engines over the network; an agent (run.py --agent)
# on the engine host starts the engines in their cpusets and cgroups, samples
# them, and stops them; the arm directories are copied back afterwards.
ENGINE_SSH = os.environ.get("ENGINE_SSH")
ENGINE_ADDR = os.environ.get("ENGINE_ADDR", "127.0.0.1")
# NET_IFACE: the engine host's interface whose bytes are sampled (link headroom).
NET_IFACE = os.environ.get("NET_IFACE")
# Prometheus' default --query.max-samples (50M) would refuse a 1h window over
# more than ~1,400 series at 10 samples/s; the exact arm must answer.
PROMETHEUS_QUERY_MAX_SAMPLES = 10_000_000_000
BACKEND_ALLOWED_LATENESS_MS = 2000
# QUERY_USERS=N (default 1): N simulated dashboard users. Every user sends every
# scheduled request (same evaluation time), user u's copy sent u/N of the
# query's interval later than user 0's, so the users' refreshes are spread over
# the interval. Requests are not awaited before the next is sent (several in
# flight) and none is skipped. Each answer row records its `user`.
QUERY_USERS = max(1, int(os.environ.get("QUERY_USERS", "1")))
ASAPQUERY_RETENTION_SLACK_MS = 10_000
# Every backend answer computed by ASAP; a query it cannot answer is refused.
BACKEND_NO_FORWARD = os.environ.get("BACKEND_FORWARD") is None
# The backend is told where the feed starts, so the first pane after its
# start is built (the feeder sends every sample from it).
BACKEND_STREAM_START = os.environ.get("BACKEND_NO_STREAM_START") is None
SAMPLE_INTERVAL_S = 2.0


class LiveMetrics:
    """The harness' own Prometheus /metrics (run.py --metrics-port), scraped
    by the monitoring Prometheus: per-query latency of every evaluation, the
    arm's phase, and per-component CPU by phase and RSS from the Sampler.
    Every series carries `run` (the run directory's name) and `arm`."""

    def __init__(self, run):
        self.run, self.lock, self.values = run, threading.Lock(), {}
        self.help = {
            "asap_bench_query_latency_seconds": ("gauge", "Latency of the query's latest evaluation"),
            "asap_bench_query_evaluations_total": ("counter", "Evaluations sent, by HTTP status"),
            "asap_bench_query_evaluation_time_seconds": ("gauge", "Evaluation time of the query's latest evaluation"),
            "asap_bench_arm_phase": ("gauge", "1 for the arm's current phase (ingest, query, done)"),
            "asap_bench_component_cpu_seconds": ("gauge", "CPU core-seconds of an arm's process in a phase so far"),
            "asap_bench_component_rss_bytes": ("gauge", "Current RSS of an arm's process"),
            "asap_bench_component_peak_rss_bytes": ("gauge", "Peak sampled RSS of an arm's process so far"),
            "asap_bench_feed_target_rows_per_second": ("gauge", "Samples per second the feeder sends to each sink"),
        }

    def set(self, name, labels, value):
        key = (name, tuple(sorted({"run": self.run, **labels}.items())))
        with self.lock:
            self.values[key] = value

    def inc(self, name, labels, by=1.0):
        key = (name, tuple(sorted({"run": self.run, **labels}.items())))
        with self.lock:
            self.values[key] = self.values.get(key, 0.0) + by

    def set_phase(self, arm, phase):
        for ph in ("ingest", "query", "done"):
            self.set("asap_bench_arm_phase", {"arm": arm, "phase": ph}, 1.0 if ph == phase else 0.0)

    def render(self):
        with self.lock:
            items = sorted(self.values.items())
        out, seen = [], set()
        for (name, labels), value in items:
            if name not in seen:
                seen.add(name)
                kind, text = self.help.get(name, ("gauge", name))
                out += [f"# HELP {name} {text}", f"# TYPE {name} {kind}"]
            lbl = ",".join(f'{k}="{v}"' for k, v in labels)
            out.append(f"{name}{{{lbl}}} {value}")
        return ("\n".join(out) + "\n").encode()

    def serve(self, port):
        metrics = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                body = metrics.render()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = http.server.ThreadingHTTPServer(("0.0.0.0", port), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return server


LIVE = None  # LiveMetrics when --metrics-port is set

# CPU placement (--arm-cores N): each arm's engine processes (its data plane,
# VictoriaMetrics, Prometheus, ASAPQuery engine) run on that arm's own cpuset
# of N cores; the feeders, this harness (scheduler + query client) and the
# monitoring stack run on the remaining cores (AUX_CPUS). Empty: no pinning.
ARM_CPUS = {}   # arm -> set of cpus
AUX_CPUS = set()


def parse_cpus(text):
    """'0-3,8' -> {0, 1, 2, 3, 8}."""
    out = set()
    for part in filter(None, text.split(",")):
        lo, _, hi = part.partition("-")
        out.update(range(int(lo), int(hi or lo) + 1))
    return out


def fmt_cpus(cpus):
    """{0, 1, 2, 3, 8} -> '0-3,8'."""
    out, run = [], []
    for c in sorted(cpus):
        if run and c != run[-1] + 1:
            out.append(f"{run[0]}-{run[-1]}" if len(run) > 1 else str(run[0]))
            run = []
        run.append(c)
    if run:
        out.append(f"{run[0]}-{run[-1]}" if len(run) > 1 else str(run[0]))
    return ",".join(out)


# The data plane's flag for the directory it persists materialized views
# in: ASAP_PERSIST_FLAG, else the first of these its --help lists. The
# directory is inside the arm's run directory, like VictoriaMetrics'
# -storageDataPath, so both are on the same disk.
PERSIST_FLAG_CANDIDATES = ("--data-dir", "--view-dir", "--views-dir", "--storage-dir", "--persist-dir",
                           "--state-dir")


def precompute_threads_args():
    """PRECOMPUTE_THREADS=N (2026-11-21): the data plane's --precompute-threads,
    when its build has the flag (the default caps at 4)."""
    n = os.environ.get("PRECOMPUTE_THREADS")
    if not n:
        return []
    try:
        text = subprocess.run([str(BIN["data_plane"]), "--help"], capture_output=True, text=True).stdout
    except OSError:
        text = ""
    return ["--precompute-threads", n] if "--precompute-threads" in text else []


def data_plane_persist_args(directory):
    flag = os.environ.get("ASAP_PERSIST_FLAG")
    # ASAP_PERSIST_FLAG=off: views in memory only (--no-view-persistence).
    if flag == "off":
        return ["--no-view-persistence"]
    if not flag:
        try:
            text = subprocess.run([str(BIN["data_plane"]), "--help"], capture_output=True, text=True).stdout
        except OSError:
            text = ""
        flag = next((f for f in PERSIST_FLAG_CANDIDATES if f in text), None)
    if not flag:
        return []
    directory.mkdir(parents=True, exist_ok=True)
    return [flag, directory]


def plan_cpus(arms, cores, spec):
    """Arm cpusets: --arm-cpus 'arm=0-15,arm2=16-31' if given, else
    consecutive blocks of `cores` from cpu 0 in arm order; the rest is AUX."""
    allowed = sorted(os.sched_getaffinity(0))
    if spec:
        sets = {a: parse_cpus(c) for a, c in (x.split("=", 1) for x in spec.split(";") if x)}
    else:
        if cores * len(arms) >= len(allowed):
            raise SystemExit(f"--arm-cores {cores} x {len(arms)} arms leaves no core for feeders/harness "
                             f"({len(allowed)} cores)")
        sets = {a: set(allowed[i * cores:(i + 1) * cores]) for i, a in enumerate(arms)}
    used = set().union(*sets.values())
    return sets, set(allowed) - used


def http_get(url, timeout=60):
    started = time.monotonic()
    with urllib.request.urlopen(url, timeout=timeout) as response:
        body = response.read()
        headers = dict(response.headers)
        status = response.status
    return status, headers, body, (time.monotonic() - started) * 1e3


def wait_ready(url, deadline_s=120):
    end = time.time() + deadline_s
    while time.time() < end:
        try:
            http_get(url, timeout=2)
            return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError(f"{url} not ready")


def proc_usage(pid):
    """(CPU seconds, peak RSS bytes) of a live process."""
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    cpu = (int(fields[11]) + int(fields[12])) / CLOCK_TICKS
    peak = 0
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmHWM:"):
                peak = int(line.split()[1]) * 1024
    return cpu, peak


def prometheus_query_requests(base):
    """Instant and range queries Prometheus at `base` has answered."""
    _, _, body, _ = http_get(f"{base}/metrics")
    total = 0
    for line in body.decode().splitlines():
        if line.startswith("prometheus_http_requests_total{") and (
            'handler="/api/v1/query"' in line or 'handler="/api/v1/query_range"' in line
        ):
            total += int(float(line.rsplit(" ", 1)[1]))
        # VictoriaMetrics as the asapquery arm's fallback (ASAPQUERY_FALLBACK).
        elif line.startswith("vm_http_requests_total{") and (
            'path="/api/v1/query"' in line or 'path="/api/v1/query_range"' in line
        ):
            total += int(float(line.rsplit(" ", 1)[1]))
    return total


def thread_cpu(pid):
    """CPU seconds per thread name of a live process."""
    out = {}
    try:
        tids = os.listdir(f"/proc/{pid}/task")
    except OSError:
        return out
    for tid in tids:
        try:
            with open(f"/proc/{pid}/task/{tid}/stat") as f:
                text = f.read()
        except OSError:
            continue
        name = text[text.index("(") + 1:text.rindex(")")]
        fields = text.rsplit(")", 1)[1].split()
        # Group numbered workers (tokio-runtime-w, rayon-12, ...) by name.
        name = name.rstrip("0123456789-_ ") or name
        out[name] = out.get(name, 0.0) + (int(fields[11]) + int(fields[12])) / CLOCK_TICKS
    return out


def io_bytes(pid):
    """(read_bytes, write_bytes) the process caused to be fetched from or
    sent to the storage layer (/proc/<pid>/io), so disk I/O, not page cache."""
    out = {}
    try:
        with open(f"/proc/{pid}/io") as f:
            for line in f:
                k, _, v = line.partition(":")
                out[k] = int(v)
    except OSError:
        pass
    return out.get("read_bytes", 0), out.get("write_bytes", 0)


def rss(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    return 0


def harness_usage():
    """The harness (this process: the scheduler and its query threads)."""
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru.ru_utime + ru.ru_stime, rss(os.getpid())


# Memory beyond RSS (2026-11-21): each arm's engine processes run in their own
# cgroup v2 (/sys/fs/cgroup/asapbench/<run>-<arm>, created with sudo -n), so
# memory.current and memory.stat (anon, file = page cache charged to the
# engine, shmem) are recorded beside RSS: VictoriaMetrics reads its compressed
# data through the page cache, which RSS does not count. ENGINE_CGROUP=0 turns
# it off; without sudo it is off. The data directory's page-cache residency is
# measured with fincore at the end of the query phase.
CGROUP_BASE = Path("/sys/fs/cgroup/asapbench")


def _find_limits(obj):
    if isinstance(obj, dict):
        if "memory_bytes" in obj and "disk_bytes" in obj:
            return obj
        for v in obj.values():
            found = _find_limits(v)
            if found:
                return found
    return None


def engine_limits():
    """The resource limits every engine is held to, identical for ASAP and
    VictoriaMetrics: the planner's cost model 3 ClusterLimits from
    PLANNER_COST_INPUTS (memory_bytes, disk_bytes, cpu_cores), else
    ENGINE_LIMITS (a JSON object or file with the same keys, plus optional
    io_max, e.g. "rbps=… wbps=…"); None: no limits (accounting only)."""
    src = None
    if os.environ.get("PLANNER_COST_INPUTS"):
        src = _find_limits(json.loads(Path(os.environ["PLANNER_COST_INPUTS"]).read_text()))
    elif os.environ.get("ENGINE_LIMITS"):
        text = os.environ["ENGINE_LIMITS"]
        src = json.loads(Path(text).read_text() if not text.lstrip().startswith("{") else text)
    if not src:
        return None
    return {k: src.get(k) for k in ("memory_bytes", "disk_bytes", "cpu_cores", "io_max")}


LIMITS = engine_limits()


def cgroup_create(name, limits=None):
    """`limits` overrides LIMITS for this cgroup (a vmcluster component's share)."""
    if os.environ.get("ENGINE_CGROUP", "1") != "1":
        return None
    LIMITS = limits if limits is not None else globals()["LIMITS"]  # noqa: N806
    path = CGROUP_BASE / name
    script = ("echo '+memory +cpu +io' > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null; "
              f"mkdir -p {CGROUP_BASE} && echo '+memory +cpu +io' > {CGROUP_BASE}/cgroup.subtree_control; "
              f"mkdir -p {path}")
    if LIMITS:
        if LIMITS.get("cpu_cores"):
            script += f"; echo '{int(float(LIMITS['cpu_cores']) * 100000)} 100000' > {path}/cpu.max"
        if LIMITS.get("memory_bytes"):
            script += f"; echo {int(LIMITS['memory_bytes'])} > {path}/memory.max"
    ok = subprocess.run(["sudo", "-n", "sh", "-c", script], capture_output=True).returncode == 0
    return path if ok and (path / "memory.current").exists() else None


def cgroup_io_max(path, data_dir):
    """io.max on the device holding data_dir (LIMITS io_max, e.g. 'rbps=… wbps=…')."""
    if not (path and LIMITS and LIMITS.get("io_max")):
        return
    dev = os.stat(data_dir).st_dev
    subprocess.run(["sudo", "-n", "sh", "-c", f"echo '{os.major(dev)}:{os.minor(dev)} {LIMITS['io_max']}' > {path}/io.max"],
                   capture_output=True)


def cgroup_events(path):
    """memory.events, memory.pressure, io.stat and cpu.stat of a cgroup at the end."""
    if not path:
        return None
    out = {}
    for name in ("memory.events", "memory.pressure", "io.stat", "cpu.stat", "memory.max", "cpu.max"):
        try:
            out[name] = (path / name).read_text().strip()
        except OSError:
            pass
    return out


def disk_limited_dir(directory, size_bytes=None):
    """With a disk limit: `directory` becomes the mount point of its own ext4
    filesystem of exactly LIMITS["disk_bytes"] (a loop-mounted image
    `<directory>.img` beside it), so the engine's data can never exceed it
    (ENOSPC instead). The image stays after the run; run_vm_demo.sh unmounts."""
    size_bytes = size_bytes or (LIMITS or {}).get("disk_bytes")
    if not size_bytes:
        return None
    directory.mkdir(parents=True, exist_ok=True)
    img = directory.with_name(directory.name + ".img")
    script = (f"truncate -s {int(size_bytes)} {img} && mkfs.ext4 -q -F -m 0 {img} && "
              f"mount -o loop {img} {directory} && chown {os.getuid()}:{os.getgid()} {directory}")
    r = subprocess.run(["sudo", "-n", "sh", "-c", script], capture_output=True, text=True)
    return {"image": str(img), "mounted": r.returncode == 0, "error": r.stderr[-300:] if r.returncode else None}


def disk_usage(directory):
    try:
        st = os.statvfs(directory)
        return {"size_bytes": st.f_blocks * st.f_frsize, "used_bytes": (st.f_blocks - st.f_bfree) * st.f_frsize}
    except OSError:
        return None


def cgroup_add(path, pid):
    if path:
        subprocess.run(["sudo", "-n", "sh", "-c", f"echo {pid} > {path}/cgroup.procs"], capture_output=True)


MEM_STAT_KEYS = ("anon", "file", "active_file", "inactive_file", "file_dirty", "file_writeback",
                 "shmem", "kernel", "slab", "kernel_stack", "pagetables")


def cgroup_memory(path):
    """A cgroup's memory, bytes: current, memory.stat's anon / file (active_file,
    inactive_file, file_dirty, file_writeback) / shmem / kernel (or slab +
    kernel_stack + pagetables on kernels without `kernel`), and working_set =
    current - inactive_file (the standard container metric: page cache the
    kernel would reclaim first is not counted). None without a cgroup."""
    if not path:
        return None
    try:
        out = {"current": int((path / "memory.current").read_text())}
        for line in (path / "memory.stat").read_text().splitlines():
            k, v = line.split()
            if k in MEM_STAT_KEYS:
                out[k] = int(v)
        if "kernel" not in out:
            out["kernel"] = sum(out.get(k, 0) for k in ("slab", "kernel_stack", "pagetables"))
        out["working_set"] = max(0, out["current"] - out.get("inactive_file", 0))
        # CPU used so far (cpu.stat usage_usec; 2026-11-22: cores over time).
        try:
            for line in (path / "cpu.stat").read_text().splitlines():
                k, v = line.split()
                if k == "usage_usec":
                    out["cpu_usage_usec"] = int(v)
        except (OSError, ValueError):
            pass
        # Bytes the cgroup has written/read to block devices so far (io.stat).
        try:
            for line in (path / "io.stat").read_text().splitlines():
                for kv in line.split()[1:]:
                    k, _, v = kv.partition("=")
                    if k in ("wbytes", "rbytes"):
                        out["io_" + k] = out.get("io_" + k, 0) + int(v)
        except (OSError, ValueError):
            pass
        return out
    except (OSError, ValueError):
        return None


def arm_cgroups(arm):
    """{role: cgroup} of an arm: its one engine cgroup (role ""), or one per
    vmcluster component."""
    many = getattr(arm, "cgroups", None)
    if many:
        return dict(many)
    one = getattr(arm, "cgroup", None)
    return {"": one} if one else {}


def arm_cgroup_memory(arm):
    """cgroup_memory of the arm: summed over its cgroups (each component's
    under `components` when there are several)."""
    groups = arm_cgroups(arm)
    if len(groups) <= 1:
        return cgroup_memory(next(iter(groups.values()), None))
    parts = {r: cgroup_memory(p) for r, p in groups.items()}
    parts = {r: m for r, m in parts.items() if m}
    if not parts:
        return None
    total = {}
    for m in parts.values():
        for k, v in m.items():
            total[k] = total.get(k, 0) + v
    total["components"] = parts
    return total


def arm_cgroup_peak_and_psi(arm):
    """cgroup_peak_and_psi of the arm; with several cgroups: the sum of their
    peaks (an upper bound of the joint peak), the max of their pressure, the
    sum of their memory.events, and each component's own."""
    groups = arm_cgroups(arm)
    if len(groups) <= 1:
        return cgroup_peak_and_psi(next(iter(groups.values()), None))
    parts = {r: cgroup_peak_and_psi(p) for r, p in groups.items()}
    out = {"components": parts}
    if all("memory_peak_bytes" in v for v in parts.values()):
        out["memory_peak_bytes"] = sum(v["memory_peak_bytes"] for v in parts.values())
        out["memory_peak_is_sum_of_components"] = True
    for k in ("psi_some_avg10", "psi_full_avg10", "psi_some_total_us", "psi_full_total_us"):
        vals = [v[k] for v in parts.values() if k in v]
        if vals:
            out[k] = max(vals)
    events = {}
    for v in parts.values():
        for k, n in (v.get("memory_events") or {}).items():
            events[k] = events.get(k, 0) + int(n)
    out["memory_events"] = {k: str(n) for k, n in events.items()}
    return out


def multi_cgroup_events(groups):
    """cgroup_events of several cgroups: each under `components`, and their
    memory.events summed (same text format) at the top."""
    parts = {r: cgroup_events(g) or {} for r, g in groups.items()}
    total = {}
    for v in parts.values():
        for line in v.get("memory.events", "").splitlines():
            k, n = line.split()
            total[k] = total.get(k, 0) + int(n)
    return {"components": parts, "memory.events": "\n".join(f"{k} {n}" for k, n in total.items())}


def ht_order(cpus):
    """cpus ordered so hyperthread siblings are adjacent (by their core's first
    sibling): a slice of an even length is whole cores."""
    def first_sibling(c):
        try:
            return min(parse_cpus(Path(f"/sys/devices/system/cpu/cpu{c}/topology/thread_siblings_list")
                                  .read_text().strip()))
        except (OSError, ValueError):
            return c
    return sorted(cpus, key=lambda c: (first_sibling(c), c))


def net_bytes():
    """(rx, tx) bytes of NET_IFACE so far, or None."""
    if not NET_IFACE:
        return None
    try:
        base = Path(f"/sys/class/net/{NET_IFACE}/statistics")
        return int((base / "rx_bytes").read_text()), int((base / "tx_bytes").read_text())
    except (OSError, ValueError):
        return None


def cgroup_peak_and_psi(path):
    """memory.peak (bytes, if the kernel has it) and memory.pressure some/full avg10."""
    out = {}
    if not path:
        return out
    try:
        out["memory_peak_bytes"] = int((path / "memory.peak").read_text())
    except (OSError, ValueError):
        pass
    try:
        for line in (path / "memory.pressure").read_text().splitlines():
            kind, *fields = line.split()
            kv = dict(f.split("=") for f in fields)
            out[f"psi_{kind}_avg10"] = float(kv.get("avg10", "nan"))
            out[f"psi_{kind}_total_us"] = int(kv.get("total", 0))
    except (OSError, ValueError):
        pass
    try:
        out["memory_events"] = dict(l.split() for l in (path / "memory.events").read_text().splitlines())
    except (OSError, ValueError):
        pass
    return out


# Self-protection and ingest counters sampled from each engine's /metrics every
# SAMPLE_INTERVAL_S (2026-11-21, fairness): VictoriaMetrics' query concurrency
# limit, queueing and rejections, slow queries, HTTP errors, rollup-cache
# hits/misses, insert concurrency and rows; ASAP's refusals, samples by outcome
# (late/future drops), windows, backlog, watermark lag and Remote Write.
import re as _re
ENGINE_KEEP = {
    "victoriametrics": _re.compile(
        r"^(vm_concurrent_select_|vm_concurrent_insert_|vm_slow_queries_total|vm_http_request_errors_total"
        r"|vm_http_requests_total\{path=\"/api/v1/(query|query_range|write)|vm_rows_inserted_total|vm_rows_ignored_total"
        r"|vm_too_long_lines|vm_rollup_result_cache_|vm_cache_requests_total\{type=\"promql/rollupResult"
        r"|vm_cache_misses_total\{type=\"promql/rollupResult|vm_memory_intensive_queries_total"
        r"|vm_search_query_duration_seconds_(sum|count)|vm_pending_rows)"),
    "asap": _re.compile(r"^(asap_view_persisted_bytes_total|asap_view_sync_seconds_total|asap_view_bytes|asap_persist_"
                        r"|asap_view_windows_|asap_samples_total|asap_windows_total|asap_precompute_backlog_windows|asap_samples_queued"
                        r"|asap_watermark_lag_seconds|asap_query_refus|asap_http|asap_remote_write"
                        r"|asap_query_latency_seconds_(sum|count)|asap_range_query_latency_seconds_(sum|count)"
                        r"|asap_raw_forward_)"),
}


def engine_counters(url, arm_name):
    keep = ENGINE_KEEP.get(arm_name)
    if keep is None:
        return {}
    body = http_get(url, timeout=10)[2].decode(errors="replace")
    out = {}
    for line in body.splitlines():
        if line and not line.startswith("#") and keep.search(line):
            k, _, v = line.rpartition(" ")
            try:
                out[k] = float(v)
            except ValueError:
                pass
    return out


def page_cache_bytes(directory):
    """Bytes of `directory`'s files resident in the page cache (fincore), or None."""
    files = [str(f) for f in Path(directory).rglob("*") if f.is_file()]
    if not files:
        return 0
    total = 0
    try:
        for i in range(0, len(files), 500):
            out = subprocess.run(["fincore", "-b", "-n", "-o", "RES", *files[i:i + 500]],
                                 capture_output=True, text=True).stdout
            total += sum(int(x) for x in out.split() if x.isdigit())
        return total
    except OSError:
        return None


class Sampler(threading.Thread):
    """Samples every process of an arm and the harness every
    SAMPLE_INTERVAL_S: CPU (per process and per thread name) and RSS. The
    ingest phase runs from the arm's start to the first query, the query
    phase from it to the end. Steady-state RSS is the median over the query
    phase, after the ingest phase's lookback has filled every window."""

    def __init__(self, arm):
        super().__init__(daemon=True)
        self.arm, self.stop_event = arm, threading.Event()
        self.samples = []  # (t, phase, {role: (cpu, rss)}, harness (cpu, rss))
        self.cg_samples = []  # (t, phase, cgroup_memory())
        self.engine_samples = []  # (t, phase, engine_counters())
        self.net_samples = []  # (t, phase, (rx, tx) bytes of NET_IFACE)
        self.phase = "ingest"
        self.marks = {}

    def snapshot(self):
        procs = {}
        for role, proc in list(self.arm.procs.items()):
            if proc.poll() is None:
                try:
                    cpu, _ = proc_usage(proc.pid)
                    rb, wb = io_bytes(proc.pid)
                    procs[role] = {"cpu": cpu, "rss": rss(proc.pid), "threads": thread_cpu(proc.pid),
                                   "disk_read": rb, "disk_write": wb}
                except OSError:
                    pass
        h_cpu, h_rss = harness_usage()
        procs["harness"] = {"cpu": h_cpu, "rss": h_rss, "threads": {}}
        return procs

    def mark_query_phase(self):
        self.marks["query_start"] = self.snapshot()
        self.phase = "query"

    def publish(self, snap):
        """Per component: CPU so far in each phase, current and peak RSS."""
        if LIVE is None:
            return
        start = self.marks.get("start", {})
        mid = self.marks.get("query_start")
        for role, v in snap.items():
            labels = {"arm": self.arm.name, "component": role, "bar": f"{self.arm.name}/{role}"}
            begin = start.get(role, {}).get("cpu", 0.0)
            if mid is None:
                LIVE.set("asap_bench_component_cpu_seconds", {**labels, "phase": "ingest"}, v["cpu"] - begin)
            else:
                at_mid = mid.get(role, {}).get("cpu", 0.0)
                LIVE.set("asap_bench_component_cpu_seconds", {**labels, "phase": "ingest"}, at_mid - begin)
                LIVE.set("asap_bench_component_cpu_seconds", {**labels, "phase": "query"}, v["cpu"] - at_mid)
            LIVE.set("asap_bench_component_rss_bytes", labels, v["rss"])
            peak = max([s[2][role] for s in self.samples if role in s[2]] + [v["rss"]])
            LIVE.set("asap_bench_component_peak_rss_bytes", labels, peak)

    def run(self):
        self.marks["start"] = self.snapshot()
        while not self.stop_event.wait(SAMPLE_INTERVAL_S):
            try:
                snap = self.snapshot()
            except Exception:  # noqa: BLE001 - one missed sample is not fatal
                continue
            self.samples.append((time.time(), self.phase, {r: v["rss"] for r, v in snap.items()}))
            cg = arm_cgroup_memory(self.arm)
            if cg:
                self.cg_samples.append((time.time(), self.phase, cg))
            nb = net_bytes()
            if nb:
                self.net_samples.append((time.time(), self.phase, nb))
            # One /metrics per engine; a vmcluster's per component (keys "<component>:<metric>").
            urls = getattr(self.arm, "metrics_urls", None) or (
                {"": self.arm.metrics_url} if getattr(self.arm, "metrics_url", None) else {})
            if urls:
                kind = "victoriametrics" if self.arm.name in VM_ARMS else self.arm.name
                merged = {}
                kinds = getattr(self.arm, "metrics_kinds", {})
                for comp, url in urls.items():
                    try:
                        merged.update({(f"{comp}:{k}" if comp else k): v
                                       for k, v in engine_counters(url, kinds.get(comp, kind)).items()})
                    except Exception:  # noqa: BLE001
                        pass
                if merged:
                    self.engine_samples.append((time.time(), self.phase, merged))
            self.publish(snap)

    def engine_counter_summary(self):
        """Per phase (ingest, query): counters (*_total) as the difference over the
        phase, gauges as the max in it; plus every sample (engine_counters.jsonl)."""
        out = {}
        for phase in ("ingest", "query"):
            ss = [m for _, ph, m in self.engine_samples if ph == phase]
            if len(ss) < 1:
                continue
            first, last, d = ss[0], ss[-1], {}
            for k in set(first) | set(last):
                if "_total" in k or "_count" in k or "_sum" in k:
                    d[k] = last.get(k, 0.0) - first.get(k, 0.0)
                else:
                    d[k + " (max)"] = max(m.get(k, 0.0) for m in ss)
            out[phase] = d
        return out

    def net_summary(self):
        """NET_IFACE bytes/s per phase (mean and max between samples), rx and tx."""
        out = {}
        for phase in ("ingest", "query"):
            ss = [(t, nb) for t, ph, nb in self.net_samples if ph == phase]
            if len(ss) < 2:
                continue
            for i, d in enumerate(("rx", "tx")):
                rates = [(b[1][i] - a[1][i]) / (b[0] - a[0]) for a, b in zip(ss, ss[1:]) if b[0] > a[0]]
                out[f"{phase}_{d}_bytes_per_s_mean"] = (ss[-1][1][i] - ss[0][1][i]) / (ss[-1][0] - ss[0][0])
                out[f"{phase}_{d}_bytes_per_s_max"] = max(rates, default=None)
        return {"iface": NET_IFACE, **out} if out else None

    def cgroup_summary(self):
        """Peak and steady (median over the query phase) of the arm's cgroup memory."""
        if not self.cg_samples:
            return None
        out = {}
        for k in ("working_set", "anon", "current", "file", "active_file", "inactive_file", "file_dirty",
                  "file_writeback", "shmem", "kernel"):
            allv = [c[k] for _, _, c in self.cg_samples if k in c]
            q = [c[k] for _, ph, c in self.cg_samples if ph == "query" and k in c]
            out[f"{k}_peak_bytes"] = max(allv, default=None)
            out[f"{k}_steady_bytes"] = statistics.median(q) if q else None
        return out

    def stop(self):
        end = self.snapshot()
        self.stop_event.set()
        start = self.marks.get("start", {})
        mid = self.marks.get("query_start", end)
        out = {}
        for role in end:
            def cpu(snap, r=role):
                return snap.get(r, {}).get("cpu", 0.0)
            query_rss = [s[2][role] for s in self.samples if s[1] == "query" and role in s[2]]
            all_rss = [s[2][role] for s in self.samples if role in s[2]]
            threads = {}
            for name in end[role]["threads"]:
                t = lambda snap: snap.get(role, {}).get("threads", {}).get(name, 0.0)  # noqa: E731
                threads[name] = {"ingest_phase_cpu_s": t(mid) - t(start), "query_phase_cpu_s": t(end) - t(mid)}
            out[role] = {
                "ingest_phase_cpu_s": cpu(mid) - cpu(start),
                "query_phase_cpu_s": cpu(end) - cpu(mid),
                # Disk bytes written/read (process io counters), per phase.
                **{f"{phase}_phase_disk_{d}_bytes": snap_b.get(role, {}).get(f"disk_{d}", 0)
                   - snap_a.get(role, {}).get(f"disk_{d}", 0)
                   for phase, snap_a, snap_b in (("ingest", start, mid), ("query", mid, end))
                   for d in ("write", "read")},
                "peak_rss_bytes": max(all_rss, default=end[role]["rss"]),
                "steady_rss_bytes": statistics.median(query_rss) if query_rss else None,
                "threads": threads,
            }
        return out


class Arm:
    def __init__(self, name, prep, run_dir):
        self.name, self.prep, self.dir = name, prep, run_dir / name
        if self.dir.exists():
            shutil.rmtree(self.dir)
        self.dir.mkdir(parents=True)
        self.port = ARM_PORTS[name]
        self.procs = {}
        self.cpus = {}  # role -> cpuset it runs on

    def spawn(self, role, cmd, env=None, cpus=None, limits=None):
        """Start a process: a feeder on the auxiliary cores, anything else
        (the arm's engine) on the arm's own cpuset (when placement is on).
        `cpus` and `limits` (a vmcluster component): its own cpuset, and its
        own cgroup with these limits instead of the arm's shared one."""
        log = open(self.dir / f"{role}.log", "w")
        if cpus is None:
            cpus = AUX_CPUS if role.startswith("feeder") else ARM_CPUS.get(self.name)
        self.cpus[role] = fmt_cpus(cpus) if cpus else None
        argv = [str(c) for c in cmd]
        engine = not role.startswith("feeder")
        group = None
        if engine and limits is not None:
            if not hasattr(self, "cgroups"):
                self.cgroups = {}
            group = self.cgroups[role] = cgroup_create(
                f"{os.getpid()}-{int(time.time())}-{self.name}-{role}", limits)
            if not hasattr(self, "limits"):
                self.limits = {}
            self.limits[role] = limits
        elif engine:
            if not hasattr(self, "cgroup"):
                self.cgroup = cgroup_create(f"{os.getpid()}-{int(time.time())}-{self.name}")
            group = self.cgroup
        if engine and group:
            # The engine starts INSIDE its cgroup (2026-11-21): a shell waits until it
            # has been moved, then execs the engine with the same pid, so every byte
            # is charged to the cgroup and an engine that reads its own memory.max
            # (e.g. the data plane's view cache default) sees the limit.
            argv = ["sh", "-c", f'until grep -q "^0::/{CGROUP_BASE.name}/{group.name}$" /proc/self/cgroup; '
                                'do sleep 0.01; done; exec "$@"', role, *argv]
        self.procs[role] = subprocess.Popen(
            argv, stdout=log, stderr=subprocess.STDOUT,
            env={**os.environ, **(env or {})}, cwd=self.dir,
            preexec_fn=(lambda c=cpus: os.sched_setaffinity(0, c)) if cpus else None,
        )
        if engine and group:
            cgroup_add(group, self.procs[role].pid)
            if limits is not None:
                cgroup_io_max(group, self.dir)
            elif not getattr(self, "io_max_set", False):
                cgroup_io_max(group, self.dir)
                self.io_max_set = True

    def prometheus(self, role, port):
        data = self.dir / f"{role}-data"
        config = self.dir / f"{role}.yml"
        config.write_text("global:\n  scrape_interval: 1h\n")
        self.spawn(role, [
            BIN["prometheus"], f"--config.file={config}", f"--storage.tsdb.path={data}",
            f"--web.listen-address=127.0.0.1:{port}", "--web.enable-remote-write-receiver",
            "--storage.tsdb.retention.time=1d", "--log.level=warn",
            f"--query.max-samples={PROMETHEUS_QUERY_MAX_SAMPLES}",
        ])
        wait_ready(f"http://127.0.0.1:{port}/-/ready")
        return f"http://127.0.0.1:{port}"

    def start(self, start_ms):
        """Start the arm's processes; return (query URL, sink URLs, forward-counting Prometheus).
        `start_ms` is where the feed starts, which the backend is told
        (`--stream-start-ms`), as a producer declares its stream's start."""
        p = self.port
        data_name = {"prometheus": "prometheus-data", "victoriametrics": "victoria-metrics-data",
                     "asap": "data_plane-data", "asap_forward": "store"}.get(self.name)
        self.disk_limit = disk_limited_dir(self.dir / data_name) if data_name else None
        if self.name == "prometheus":
            prom = self.prometheus("prometheus", p)
            return prom, [f"{prom}/api/v1/write"], None
        if self.name == "victoriametrics":
            base = f"http://{ENGINE_ADDR}:{p}"
            self.spawn("victoria-metrics", [
                BIN["victoriametrics"], f"-storageDataPath={self.dir / 'victoria-metrics-data'}",
                f"-httpListenAddr={ENGINE_ADDR}:{p}", *VM_FLAGS,
            ])
            wait_ready(f"{base}/health")
            write_json(self.dir / "vm_flags.json", {
                **vm_flags_record("victoriametrics", base, VM_FLAGS),
                "stream_aggregation": False, "query_timeout_s": QUERY_TIMEOUT_S,
            })
            return base, [f"{base}/api/v1/write"], None
        if self.name == "vmcluster":
            return self.start_vmcluster(p)
        if self.name == "asap":
            persist = data_plane_persist_args(self.dir / "data_plane-data")
            # The data plane alone: no Prometheus beside it, so nothing is
            # forwarded (--no-forward refuses a plan with a query it cannot run).
            self.spawn("data_plane", [
                BIN["data_plane"], "--plan", self.prep / "backend" / BACKEND_PLANS_ASAP,
                "--prometheus-url", "http://127.0.0.1:9", "--listen", f"{ENGINE_ADDR}:{p + 1}",
                "--allowed-lateness-ms", BACKEND_ALLOWED_LATENESS_MS,
                *(["--stream-start-ms", start_ms] if BACKEND_STREAM_START else []),
                "--no-forward", *persist, *precompute_threads_args(),
                # DATA_PLANE_EXTRA_ARGS (2026-11-21, diagnostics): extra data_plane flags,
                # e.g. "--view-cache-max-bytes 0"; recorded in the arm's command line.
                *os.environ.get("DATA_PLANE_EXTRA_ARGS", "").split(),
            ], env={"RUST_LOG": "warn"})
            wait_ready(f"http://{ENGINE_ADDR}:{p + 1}/-/ready")
            return f"http://{ENGINE_ADDR}:{p + 1}", [f"http://{ENGINE_ADDR}:{p + 1}/api/v1/write"], None
        if self.name == "asap_forward":
            # Forward mode (2026-11-22, docs/design/demo-plan.md §10.1): the data plane
            # delivers every accepted Remote Write request to its own VictoriaMetrics
            # single-node (the raw store), which answers what the plan does not. Both
            # run in this arm's one cgroup under the arm's limits, so the arm's
            # resources are ASAP + VictoriaMetrics; each is also reported per component.
            store = self.dir / "store"
            store.mkdir(exist_ok=True)
            vm = f"http://{ENGINE_ADDR}:{p}"
            self.spawn("victoria-metrics", [
                BIN["victoriametrics"], f"-storageDataPath={store / 'victoria-metrics-data'}",
                f"-httpListenAddr={ENGINE_ADDR}:{p}", *VM_FLAGS,
            ])
            wait_ready(f"{vm}/health")
            persist = data_plane_persist_args(store / "data_plane-data")
            self.spawn("data_plane", [
                BIN["data_plane"], "--plan", self.prep / "backend" / BACKEND_PLANS_ASAP,
                "--raw-store-url", vm, "--listen", f"{ENGINE_ADDR}:{p + 1}",
                "--allowed-lateness-ms", BACKEND_ALLOWED_LATENESS_MS,
                *(["--stream-start-ms", start_ms] if BACKEND_STREAM_START else []),
                *persist, *precompute_threads_args(),
                *os.environ.get("DATA_PLANE_EXTRA_ARGS", "").split(),
            ], env={"RUST_LOG": "warn"})
            wait_ready(f"http://{ENGINE_ADDR}:{p + 1}/-/ready")
            self.metrics_urls = {"asap": f"http://{ENGINE_ADDR}:{p + 1}/metrics", "vm": f"{vm}/metrics"}
            self.metrics_kinds = {"asap": "asap", "vm": "victoriametrics"}
            return f"http://{ENGINE_ADDR}:{p + 1}", [f"http://{ENGINE_ADDR}:{p + 1}/api/v1/write"], None
        if self.name == "asapquery":
            if ASAPQUERY_FALLBACK == "victoriametrics":
                # Its own VictoriaMetrics (same flags as the VM arm), counted in this arm.
                prom = f"http://127.0.0.1:{p}"
                self.spawn("victoria-metrics", [
                    BIN["victoriametrics"], f"-storageDataPath={self.dir / 'victoria-metrics-data'}",
                    f"-httpListenAddr=127.0.0.1:{p}", *VM_FLAGS,
                ])
                wait_ready(f"{prom}/health")
                health = "/health"
            else:
                prom = self.prometheus("prometheus", p)
                health = "/-/ready"
            config = {
                "output_dir": str(self.dir / "engine-output"),
                # INFO logs each answer's destination (asap or prometheus).
                "log_level": "INFO",
                "data_ingestion_interval_ms": 100,
                "streaming_engine": "precompute",
                "http_server": {"port": p + 1},
                "backend": {"type": "prometheus", "server": prom,
                            "health_endpoint": health, "forward_unsupported_queries": True},
                "store": {"lock_strategy": "per-key", "retention_slack_ms": ASAPQUERY_RETENTION_SLACK_MS},
                "ingest": {"type": "http_remote_write", "port": p + 2},
                "inference_config": str(self.prep / "asapquery/inference_config.yaml"),
                "streaming_config": str(self.prep / "asapquery/streaming_config.yaml"),
            }
            if ASAPQUERY_FALLBACK == "victoriametrics":
                manifest = read_json(self.prep / "manifest.json")
                config["data_ingestion_interval_ms"] = manifest["data_facts"]["scrape_interval_ms"]
                cpus = ARM_CPUS.get(self.name)
                config["precompute_engine"] = {
                    "num_workers": int(os.environ.get("ASAPQUERY_NUM_WORKERS") or (len(cpus) if cpus else 4))}
                if os.environ.get("ASAPQUERY_ALLOWED_LATENESS_MS"):
                    config["precompute_engine"]["allowed_lateness_ms"] = int(os.environ["ASAPQUERY_ALLOWED_LATENESS_MS"])
            (self.dir / "engine_config.json").write_text(json.dumps(config, indent=2))
            # YAML is a superset of JSON.
            self.spawn("asapquery", [BIN["asapquery"], "--config-file", self.dir / "engine_config.json"],
                       env={"TZ": "UTC"})
            wait_ready(f"http://127.0.0.1:{p + 1}/api/v1/query?query=1")
            return f"http://127.0.0.1:{p + 1}", [f"http://127.0.0.1:{p + 2}/api/v1/write",
                                                f"{prom}/api/v1/write"], prom
        prom = self.prometheus("prometheus", p)
        self.spawn("data_plane", [
            BIN["data_plane"], "--plan", self.prep / "backend" / BACKEND_PLANS[self.name],
            "--prometheus-url", prom, "--listen", f"127.0.0.1:{p + 1}",
            "--allowed-lateness-ms", BACKEND_ALLOWED_LATENESS_MS,
            *(["--stream-start-ms", start_ms] if BACKEND_STREAM_START else []),
            *(["--no-forward"] if BACKEND_NO_FORWARD else []),
        ], env={"RUST_LOG": "warn"})
        wait_ready(f"http://127.0.0.1:{p + 1}/-/ready")
        return f"http://127.0.0.1:{p + 1}", [f"http://127.0.0.1:{p + 1}/api/v1/write",
                                            f"{prom}/api/v1/write"], prom

    def start_vmcluster(self, p):
        """VictoriaMetrics cluster within the arm's budget (VMCLUSTER_LAYOUT):
        each instance on its own slice of the arm's cpuset (whole cores) and in
        its own cgroup with cpu.max = its cores and memory.max = the arm's
        memory x its share of the cores; each vmstorage on its own share of the
        disk limit. vmstorage listens on 127.0.0.1 (same host), vminsert and
        vmselect on ENGINE_ADDR. Replication factor 1 (the default)."""
        layout = []
        for item in VMCLUSTER_LAYOUT.split(","):
            comp, n, cores = item.split(":")
            layout += [(f"{comp}{i}", comp, int(cores)) for i in range(int(n))]
        total = sum(c for _, _, c in layout)
        cpus = ht_order(ARM_CPUS.get(self.name) or [])
        if cpus and len(cpus) != total:
            raise SystemExit(f"VMCLUSTER_LAYOUT {VMCLUSTER_LAYOUT} has {total} cores; the arm's cpuset has {len(cpus)}")
        lim = LIMITS or {}
        n_storage = sum(1 for _, comp, _ in layout if comp == "vmstorage")
        storage_nodes, at, self.metrics_urls = [], 0, {}
        plan = {}
        for role, comp, cores in layout:
            share = cores / total
            plan[role] = {
                "cpus": set(cpus[at:at + cores]) if cpus else None,
                "limits": {"cpu_cores": cores if (lim.get("cpu_cores") or cpus) else None,
                           "memory_bytes": int(lim["memory_bytes"] * share) if lim.get("memory_bytes") else None,
                           "disk_bytes": int(lim["disk_bytes"] / n_storage)
                           if (lim.get("disk_bytes") and comp == "vmstorage") else None,
                           "io_max": lim.get("io_max")}}
            at += cores
        for i, (role, comp, _) in enumerate(x for x in layout if x[1] == "vmstorage"):
            http, ins, sel = p + 10 + 3 * i, p + 11 + 3 * i, p + 12 + 3 * i
            data = self.dir / f"{role}-data"
            plan[role]["disk_limit"] = disk_limited_dir(data, plan[role]["limits"]["disk_bytes"])
            self.spawn(role, [BIN["vmstorage"], f"-storageDataPath={data}", f"-httpListenAddr=127.0.0.1:{http}",
                              f"-vminsertAddr=127.0.0.1:{ins}", f"-vmselectAddr=127.0.0.1:{sel}",
                              *VMCLUSTER_FLAGS["vmstorage"]],
                       cpus=plan[role]["cpus"], limits=plan[role]["limits"])
            wait_ready(f"http://127.0.0.1:{http}/health")
            self.metrics_urls[role] = f"http://127.0.0.1:{http}/metrics"
            storage_nodes.append((ins, sel))
        ports = {"vminsert": p + 1, "vmselect": p + 2}
        for role, comp, _ in layout:
            if comp == "vmstorage":
                continue
            node_port = 0 if comp == "vminsert" else 1
            nodes = ",".join(f"127.0.0.1:{n[node_port]}" for n in storage_nodes)
            port = ports[comp] + 100 * int(role[len(comp):])
            self.spawn(role, [BIN[comp], f"-httpListenAddr={ENGINE_ADDR}:{port}", f"-storageNode={nodes}",
                              *VMCLUSTER_FLAGS[comp]],
                       cpus=plan[role]["cpus"], limits=plan[role]["limits"])
            wait_ready(f"http://{ENGINE_ADDR}:{port}/health")
            self.metrics_urls[role] = f"http://{ENGINE_ADDR}:{port}/metrics"
        self.disk_limit = {r: v.get("disk_limit") for r, v in plan.items() if v.get("disk_limit")} or None
        record = {"layout": VMCLUSTER_LAYOUT, "replication_factor": 1, "query_timeout_s": QUERY_TIMEOUT_S,
                  "components": {}}
        for role, comp, _ in layout:
            base = self.metrics_urls[role].rsplit("/metrics", 1)[0]
            record["components"][role] = {
                **vm_flags_record(comp, base, VMCLUSTER_FLAGS[comp]),
                "cpus": fmt_cpus(plan[role]["cpus"]) if plan[role]["cpus"] else None,
                "limits": plan[role]["limits"]}
        write_json(self.dir / "vm_flags.json", record)
        # Several vmselect/vminsert instances: the harness uses the first of each (one query and one
        # Remote Write endpoint); more than one would need a load balancer in front.
        select, insert = f"http://{ENGINE_ADDR}:{ports['vmselect']}", f"http://{ENGINE_ADDR}:{ports['vminsert']}"
        return f"{select}/select/0/prometheus", [f"{insert}/insert/0/prometheus/api/v1/write"], None

    def write_state(self, phase, query_url):
        """<arm>/state.json: the phase and every process' pid, for the
        profiling wrapper (profile/profile_query_phase.sh) to attach to."""
        write_json(self.dir / "state.json", {
            "arm": self.name, "phase": phase, "query_url": query_url, "time": time.time(),
            "pids": {role: proc.pid for role, proc in self.procs.items() if proc.poll() is None},
        })

    def feed(self, sinks, start_ms, duration_s):
        # feeder_table.<i>.json (prepare.py FEEDER_SHARDS): one feeder per shard per sink,
        # named feeder<k> in sink-major order; else one feeder per sink with the whole table.
        tables = sorted(self.prep.glob("feeder_table.*.json"), key=lambda t: int(t.name.split(".")[1])) \
            or [self.prep / "feeder_table.json"]
        jobs = [(sink, table) for sink in sinks for table in tables]
        for i, (sink, table) in enumerate(jobs):
            self.spawn(f"feeder{i}", [
                BIN["feeder"], "--sink", "remote-write", "--url", sink,
                "--workload-table", table, "--seed", SEED,
                "--duration-secs", duration_s, "--start-ms", start_ms,
                "--stats-out", self.dir / f"feeder{i}-stats.json",
            # REMOTE_WRITE_SHAPE: per-sample | per-series; unset keeps the
            # feeder's default (per-sample) and works with older feeders.
            ] + (["--remote-write-shape", os.environ["REMOTE_WRITE_SHAPE"]]
                 if os.environ.get("REMOTE_WRITE_SHAPE") else []))

    def usage(self):
        out = {}
        for role, proc in self.procs.items():
            if proc.poll() is None:
                cpu, peak = proc_usage(proc.pid)
                out[role] = {"cpu_core_seconds": cpu, "peak_rss_bytes": peak}
            else:
                out[role] = {"exited": proc.returncode}
        return out

    def storage(self):
        """Bytes on disk under each process' data directory."""
        out = {}
        for data in list(self.dir.glob("*-data")) + list(self.dir.glob("engine-output")):
            total = 0
            for root, _, files in os.walk(data):
                for f in files:
                    try:
                        total += os.path.getsize(os.path.join(root, f))
                    except OSError:
                        pass
            out[data.name] = total
        return out

    def status(self, query_url):
        """The backend data plane's own counters, if it has them."""
        try:
            return json.loads(http_get(f"{query_url}/api/v1/status/asap")[2])
        except Exception:  # noqa: BLE001 - only the backend has it
            return None

    def stop(self):
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
        for proc in self.procs.values():
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
        for group in arm_cgroups(self).values():
            if group:
                subprocess.run(["sudo", "-n", "rmdir", str(group)], capture_output=True)
        for data in self.dir.glob("*-data"):
            # KEEP_VM_DATA (default 1): keep VictoriaMetrics' data for the
            # post-run *_prometheus reference queries (accuracy ground truth,
            # accuracy_vs_vm.py).
            if data.name == "victoria-metrics-data" and os.environ.get("KEEP_VM_DATA", "1") == "1":
                continue
            shutil.rmtree(data, ignore_errors=True)


def vm_flags_record(component, base, explicit):
    """Version, binary and flags (explicit and effective, from /flags) of a VictoriaMetrics process."""
    binary = BIN[component]
    flags = {}
    try:
        for line in http_get(f"{base}/flags")[2].decode().splitlines():
            if "=" in line:
                k, v = line.lstrip("-").split("=", 1)
                flags[k] = v.strip('"')
    except Exception:  # noqa: BLE001 - recorded when available
        pass
    return {"version": VICTORIAMETRICS_VERSION, "binary": str(binary),
            # What the binary says it is (e.g. victoria-metrics-20261105-...-tags-v1.156.0-0-g...).
            "binary_version": subprocess.run([str(binary), "-version"], capture_output=True, text=True).stdout.strip(),
            "explicit": explicit, "effective_flags": flags}


def finish_reference_feed(arm, query_url, last_eval_ms):
    """The reference (victoriametrics) arm, after its query phase is recorded:
    let its feeders finish their duration instead of stopping them (a feeder
    that fell behind under query load still sends the samples it owes), at
    most REFERENCE_FEED_WAIT_S (300) seconds; then check that VictoriaMetrics'
    newest sample of every metric is at or after the last evaluation time, so
    the post-run reference queries (accuracy_vs_vm.py) see complete data. The
    wait is recorded apart (`reference_feed` in resources.json) and is in no
    phase's CPU: the sampler has stopped."""
    limit = float(os.environ.get("REFERENCE_FEED_WAIT_S", "300"))
    started = time.time()
    feeders = {r: p for r, p in arm.procs.items() if r.startswith("feeder")}
    while time.time() - started < limit and any(p.poll() is None for p in feeders.values()):
        time.sleep(1)
    waited = time.time() - started
    unfinished = sorted(r for r, p in feeders.items() if p.poll() is None)
    newest = {}
    # VictoriaMetrics makes a received sample searchable within ~1-2 s.
    time.sleep(3)
    # Every metric the feed has (not only data/data_total: workloads rename and add
    # metrics): the earliest last sample over all series.
    # Per metric name (2026-11-22): one query over every series exceeds VictoriaMetrics'
    # unique-series limit at demo scale (HTTP 422).
    try:
        names = sorted(read_json(arm.prep / "feeder_table.json")["workloads"][0]["e2e"]["metrics"])
    except Exception:  # noqa: BLE001
        names = []
    for name in names or [None]:
        sel = f'{{__name__="{name}"}}' if name else '{__name__!=""}'
        url = f"{query_url}/api/v1/query?" + urllib.parse.urlencode(
            {"query": f"min(tlast_over_time({sel}[1h]))", "time": f"{time.time():.3f}"})
        key = name or "all_series"
        try:
            body = json.loads(http_get(url, timeout=120)[2])
            res = body["data"]["result"]
            newest[key] = int(float(res[0]["value"][1]) * 1000) if res else None
        except Exception as e:  # noqa: BLE001 - recorded
            newest[key] = f"error: {e}"
    ok = all(isinstance(v, int) and v >= last_eval_ms for v in newest.values())
    return {"wait_s": round(waited, 1), "wait_limit_s": limit, "feeders_unfinished": unfinished,
            "newest_sample_ms": newest, "last_evaluation_ms": last_eval_ms, "complete": ok}


def vm_reference_queries(arm, query_url, events, start_ms):
    """VM_REFERENCE_QUERIES=1 (asapquery arm comparison, 2026-11-20): after the
    victoriametrics arm's query phase (its resources already recorded), ask the
    same VictoriaMetrics, untimed, every rate/increase query rewritten to
    MetricsQL's rate_prometheus/increase_prometheus (PromQL's extrapolation) at
    every evaluation time, into <arm>/reference_answers.jsonl, an accuracy
    reference for those queries."""
    import re
    with open(arm.dir / "reference_answers.jsonl", "w") as out:
        for _, t_ms, q in events:
            ref = re.sub(r"\b(rate|increase)\(", r"\1_prometheus(", q["promql"])
            if ref == q["promql"]:
                continue
            url = f"{query_url}/api/v1/query?" + urllib.parse.urlencode({"query": ref, "time": f"{t_ms / 1e3:.3f}"})
            try:
                status, _, body, latency = http_get(url, timeout=600)
                result = json.loads(body)
            except Exception as e:  # noqa: BLE001 - recorded
                status, latency, result = None, None, {"error": str(e)}
            out.write(json.dumps({"query": q["id"], "offset_ms": t_ms - start_ms, "promql": ref,
                                  "status": status, "latency_ms": latency, "result": result}) + "\n")


def query_phase_ms(queries, rounds, query_phase_s):
    """How long queries run: --query-phase-s, else rounds of the longest interval."""
    if query_phase_s:
        return int(query_phase_s * 1000)
    return rounds * max(q["interval_ms"] for q in queries)


# QUERIES_FROM_START=1 (2026-11-22, demo run model): queries run from the feed's start
# (first evaluation one interval after it, windows filling), not after the longest
# lookback; the run lasts --query-phase-s from the start. No backfill.
QUERIES_FROM_START = os.environ.get("QUERIES_FROM_START") == "1"
ANSWER_RESULTS_EVERY = int(os.environ.get("ANSWER_RESULTS_EVERY", "1"))


def schedule(queries, start_ms, rounds, delay_s, query_phase_s=None):
    """(send wall time s, evaluation time ms, query) in send order."""
    # From the start: the first evaluation is one interval after it (not at it, an empty window).
    lookback = 1 if QUERIES_FROM_START else max(q["lookback_ms"] for q in queries)
    first = start_ms + lookback
    last = first + query_phase_ms(queries, rounds, query_phase_s)
    events = []
    for q in queries:
        t = -(-first // q["interval_ms"]) * q["interval_ms"]
        while t <= last:
            events.append((t / 1e3 + delay_s, t, q))
            t += q["interval_ms"]
    return sorted(events, key=lambda e: e[0]), last


def request_url(base, promql, q, t_ms):
    """The request for `promql` at evaluation time `t_ms`: a range request
    [t - span, t] at the query's step (as Grafana sends one) for a range
    query, else an instant query at t."""
    if q.get("range"):
        rng = q["range"]
        return f"{base}/api/v1/query_range?" + urllib.parse.urlencode({
            "query": promql, "start": f"{(t_ms - rng['span_ms']) / 1e3:.3f}",
            "end": f"{t_ms / 1e3:.3f}", "step": f"{rng['step_ms'] / 1e3:g}"})
    return f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": promql, "time": f"{t_ms / 1e3:.3f}"})


class LocalEngine:
    """The engine side of one arm on this host: its processes (in their cpusets
    and cgroups) and the sampler. Without ENGINE_SSH the feeders are spawned
    on the same Arm (`self.arm`), so they are sampled and stopped with it."""

    def __init__(self, name, prep, run_dir):
        self.name = name
        self.arm = Arm(name, prep, run_dir)

    def start(self, start_ms):
        query_url, sinks, forward_prom = self.arm.start(start_ms)
        if self.name == "asap" or self.name == "victoriametrics":
            self.arm.metrics_url = f"{query_url}/metrics"
        self.sampler = Sampler(self.arm)
        self.sampler.start()
        return query_url, sinks, forward_prom

    def mark_query(self):
        usage = self.arm.usage()
        self.sampler.mark_query_phase()
        return usage

    def write_state(self, phase, query_url):
        self.arm.write_state(phase, query_url)

    def finish(self):
        """After the query phase: stop sampling; write the samples; return the
        engine side's part of resources.json."""
        arm, sampler = self.arm, self.sampler
        usage = arm.usage()
        components = sampler.stop()
        # The cgroup's memory and IO every SAMPLE_INTERVAL_S (dirty/writeback over time, io wbytes).
        (arm.dir / "cgroup_samples.jsonl").write_text(
            "".join(json.dumps({"t": t_, "phase": ph_, **c_}) + "\n" for t_, ph_, c_ in sampler.cg_samples))
        (arm.dir / "engine_counters.jsonl").write_text(
            "".join(json.dumps({"t": t_, "phase": ph_, "m": m_}) + "\n" for t_, ph_, m_ in sampler.engine_samples))
        if sampler.net_samples:
            (arm.dir / "net_samples.jsonl").write_text(
                "".join(json.dumps({"t": t_, "phase": ph_, "rx": nb[0], "tx": nb[1]}) + "\n"
                        for t_, ph_, nb in sampler.net_samples))
        groups = arm_cgroups(arm)
        memory = {"cgroup": (str(groups.get("")) if "" in groups else {r: str(g) for r, g in groups.items()})
                  if groups else None,
                  **(sampler.cgroup_summary() or {}),
                  **arm_cgroup_peak_and_psi(arm),
                  "data_dir_page_cache_bytes": {d.name: page_cache_bytes(d) for d in arm.dir.glob("*-data")}}
        return {
            "usage": usage, "components": components, "memory": memory, "storage": arm.storage(),
            "engine_counters": sampler.engine_counter_summary(),
            "network": sampler.net_summary(),
            "cgroup_end": (cgroup_events(groups.get("")) if list(groups) == [""] else
                           multi_cgroup_events(groups)) if groups else None,
            "component_limits": getattr(arm, "limits", None),
            "disk_limit": getattr(arm, "disk_limit", None),
            "data_dir_usage": {d.name: disk_usage(d) for d in arm.dir.glob("*-data")},
            "enospc_in_logs": sum(f.read_text(errors="replace").lower().count("no space left")
                                  for f in arm.dir.glob("*.log")),
            "engine_exit_codes": {r: p.poll() for r, p in arm.procs.items() if not r.startswith("feeder")},
            "loadavg_at_end": os.getloadavg(), "cpus": os.cpu_count(),
            "cpusets": {**arm.cpus, "harness": fmt_cpus(os.sched_getaffinity(0))},
            "host": os.uname().nodename,
        }

    def stop(self):
        self.arm.stop()

    def fetch(self):
        pass


# Environment the engine agent gets from this harness (names or prefixes).
AGENT_ENV_PREFIXES = ("ASAPQ_BENCH", "BIN_DIR", "CARGO_TARGET_DIR", "VICTORIAMETRICS", "VM_", "VMCLUSTER",
                      "DATA_PLANE", "CONTROL_PLANE", "ASAP_PERSIST", "PRECOMPUTE", "ENGINE_", "KEEP_VM",
                      "QUERY_", "BACKEND_", "NET_IFACE", "RUST", "PROMETHEUS_BIN", "ASAPQUERY")


class RemoteEngine:
    """The engine side of one arm on ENGINE_SSH: an agent (run.py --agent over
    ssh, line-delimited JSON on its stdin/stdout) runs a LocalEngine there.
    `self.arm` is this host's Arm (same directory path): the feeders, the
    answers, the agent's log; the engine host's arm directory is copied into it
    at the end (fetch), and the engine host's copy is then removed."""

    def __init__(self, name, prep, run_dir):
        self.name = name
        self.arm = Arm(name, prep, run_dir)
        env = {k: v for k, v in os.environ.items() if k.startswith(AGENT_ENV_PREFIXES)}
        env.pop("PLANNER_COST_INPUTS", None)
        # The limits as computed here (the planner's cost inputs may be a file on this host only).
        env["ENGINE_LIMITS"] = json.dumps(LIMITS) if LIMITS else ""
        env.pop("ENGINE_SSH", None)
        env.pop("ENGINE_SSH_OPTS", None)
        argv = ["ssh", *os.environ.get("ENGINE_SSH_OPTS", "").split(), ENGINE_SSH, "env",
                *[f"{k}={shlex_quote(v)}" for k, v in sorted(env.items())],
                "python3", "-u", str(Path(__file__).resolve()), "--agent"]
        self.proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=open(self.arm.dir / "engine_agent.log", "w"), text=True)
        self.lock = threading.Lock()

    def call(self, op, **kw):
        with self.lock:
            self.proc.stdin.write(json.dumps({"op": op, "arm": self.name, **kw}, default=str) + "\n")
            self.proc.stdin.flush()
            line = self.proc.stdout.readline()
        if not line:
            raise RuntimeError(f"engine agent for {self.name} exited: see {self.arm.dir / 'engine_agent.log'}")
        r = json.loads(line)
        if not r["ok"]:
            raise RuntimeError(f"engine agent {op}: {r['error']}")
        return r["value"]

    def start(self, start_ms):
        spec = ";".join(f"{a}={fmt_cpus(c)}" for a, c in ARM_CPUS.items())
        return tuple(self.call("start", prep=str(self.arm.prep), run_dir=str(self.arm.dir.parent),
                               start_ms=start_ms, arm_cpus=spec))

    def mark_query(self):
        return self.call("mark_query")

    def write_state(self, phase, query_url):
        self.call("write_state", phase=phase, query_url=query_url)

    def finish(self):
        out = self.call("finish")
        # The feeders run here: their CPU and peak RSS beside the engine host's processes.
        out["feeder_usage"] = self.arm.usage()
        out["harness_host"] = os.uname().nodename
        return out

    def stop(self):
        try:
            self.call("stop")
        except Exception as e:  # noqa: BLE001 - the agent may already be gone
            print(f"engine agent stop: {e}", flush=True)

    def fetch(self):
        """Copy the engine host's arm directory here (logs, samples, kept data;
        not the loop images), then unmount and remove it there."""
        src = f"{ENGINE_SSH}:{self.arm.dir}/"
        ssh = " ".join(["ssh", *os.environ.get("ENGINE_SSH_OPTS", "").split()])
        r = subprocess.run(["rsync", "-a", "--exclude", "*.img", "--exclude", "lost+found", "-e", ssh, src,
                            f"{self.arm.dir}/"], capture_output=True, text=True)
        if r.returncode:
            # 23/24: some files not copied (recorded); anything else: keep the engine host's copy.
            print(f"fetch {self.name}: rsync exit {r.returncode}: {r.stderr[-500:]}", flush=True)
            if r.returncode not in (23, 24):
                return
        if os.environ.get("ENGINE_KEEP_REMOTE") != "1":
            try:
                self.call("cleanup")
            except Exception as e:  # noqa: BLE001
                print(f"engine agent cleanup: {e}", flush=True)
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=30)
        except Exception:  # noqa: BLE001
            self.proc.kill()


def shlex_quote(v):
    import shlex
    return shlex.quote(v)


def agent():
    """run.py --agent: the engine host's side of RemoteEngine. Reads one JSON
    request per line on stdin, answers one JSON line on stdout; everything else
    the harness prints goes to stderr (the driver's engine_agent.log)."""
    import sys
    import traceback
    out = os.fdopen(os.dup(1), "w", buffering=1)
    os.dup2(2, 1)
    engines = {}
    global ARM_CPUS, AUX_CPUS
    for line in sys.stdin:
        req = json.loads(line)
        op, name = req["op"], req["arm"]
        try:
            if op == "start":
                if req.get("arm_cpus"):
                    ARM_CPUS, AUX_CPUS = plan_cpus([name], 0, req["arm_cpus"])
                    os.sched_setaffinity(0, AUX_CPUS or set(range(os.cpu_count())))
                engines[name] = LocalEngine(name, Path(req["prep"]), Path(req["run_dir"]))
                value = list(engines[name].start(req["start_ms"]))
            elif op == "mark_query":
                value = engines[name].mark_query()
            elif op == "write_state":
                value = engines[name].write_state(req["phase"], req["query_url"])
            elif op == "finish":
                value = engines[name].finish()
            elif op == "stop":
                value = engines[name].stop()
            elif op == "cleanup":
                d = engines[name].arm.dir
                mounts = subprocess.run(["findmnt", "-rn", "-o", "TARGET"], capture_output=True, text=True).stdout
                for m in mounts.split():
                    if m.startswith(f"{d}/"):
                        subprocess.run(["sudo", "-n", "umount", m], capture_output=True)
                shutil.rmtree(d, ignore_errors=True)
                value = None
            else:
                raise ValueError(f"unknown op {op}")
            out.write(json.dumps({"ok": True, "value": value}, default=str) + "\n")
        except BaseException as e:  # noqa: BLE001 - reported to the driver
            print(traceback.format_exc(), file=sys.stderr, flush=True)
            out.write(json.dumps({"ok": False, "error": f"{type(e).__name__}: {e}"}) + "\n")


def run_arm(name, prep, run_dir, rounds, start_ms=None, query_phase_s=None):
    delay_s = DELAY_S[name]
    # A query with `engines` (2026-11-22, demo48) is sent only to those arms.
    queries = [q for q in read_json(prep / "queries.json")["queries"] if name in q.get("engines", [name])]
    engine = RemoteEngine(name, prep, run_dir) if ENGINE_SSH else LocalEngine(name, prep, run_dir)
    arm = engine.arm
    wall_started = time.time()
    try:
        if start_ms is None:
            start_ms = (int(time.time()) + 30) // 60 * 60 * 1000 + 60_000
        query_url, sinks, forward_prom = engine.start(start_ms)
        lookback = 0 if QUERIES_FROM_START else max(q["lookback_ms"] for q in queries)
        events, last = schedule(queries, start_ms, rounds, delay_s, query_phase_s)
        duration_s = (lookback + query_phase_ms(queries, rounds, query_phase_s)) // 1000 \
            + int(max(DELAY_S.values())) + 15
        arm.feed(sinks, start_ms, duration_s)
        engine.write_state("ingest", query_url)
        if LIVE is not None:
            LIVE.set_phase(name, "ingest")
            table = read_json(prep / "manifest.json")
            series = table["data_facts"]["series"]["leaf_count"]
            LIVE.set("asap_bench_feed_target_rows_per_second", {"arm": name},
                     series * table["samples_per_sec_per_series"] * 2)  # data and data_total
        answers = open(arm.dir / "answers.jsonl", "w")
        lock = threading.Lock()
        forwards_before = None

        probes = []
        # The first range-selected metric (any name; same expression as accuracy_vs_vm.METRIC).
        metric_re = __import__("re").compile(r"(?<![\w:])([a-zA-Z_:][a-zA-Z0-9_:]*)(?=\s*(?:\{[^}]*\})?\s*\[)")

        def probe(t_ms, q):
            """What the engine could see for (q, T) when it was asked (user 0's send):
            VictoriaMetrics: sum(count_over_time(<metric>[<window>])) at T (compared
            with the post-run reference's count, accuracy_vs_vm.py); ASAP: its
            watermark (the data plane does not answer count_over_time)."""
            row = {"query": q["id"], "offset_ms": t_ms - start_ms, "at": time.time()}
            try:
                if name in VM_ARMS:
                    m = metric_re.search(q["promql"]).group(1)
                    url = f"{query_url}/api/v1/query?" + urllib.parse.urlencode(
                        {"query": f"sum(count_over_time({m}[{q['window_ms'] // 1000}s]))", "time": f"{t_ms / 1e3:.3f}"})
                    res = json.loads(http_get(url, timeout=QUERY_TIMEOUT_S)[2])["data"]["result"]
                    row["count"] = float(res[0]["value"][1]) if res else 0.0
                elif name in ("asap", "asap_forward"):
                    lag = engine_counters(f"{query_url}/metrics", "asap").get("asap_watermark_lag_seconds")
                    row["watermark_lag_s"] = lag
                    row["visible_until_offset_ms"] = (row["at"] - lag) * 1e3 - start_ms if lag is not None else None
            except Exception as e:  # noqa: BLE001 - recorded
                row["error"] = str(e)[:200]
            with lock:
                probes.append(row)

        def ask(t_ms, q, user=0, send_at=None):
            url = request_url(query_url, q["promql"], q, t_ms)
            # How late the request actually went out (2026-11-23): its scheduled send time
            # vs now; with a bounded client pool, requests can queue in the client.
            lateness_ms = (time.time() - send_at) * 1e3 if send_at is not None else None
            started = time.monotonic()
            try:
                status, headers, body, latency = http_get(url, timeout=QUERY_TIMEOUT_S)
                result = json.loads(body)
            except urllib.error.HTTPError as e:
                latency = (time.monotonic() - started) * 1e3
                status, headers = e.code, dict(e.headers)
                try:
                    result = json.loads(e.read())
                except Exception:  # noqa: BLE001
                    result = {"error": str(e)}
            except Exception as e:  # noqa: BLE001 - recorded, not fatal
                status, headers, latency, result = None, {}, None, {"error": str(e)}
            # ANSWER_RESULTS_EVERY=N (2026-11-22, 6 h demo runs): keep the full result of user 0's
            # evaluations whose index counted from the feed start is a multiple of N (2026-11-23:
            # was the absolute grid index, which differs between arms started at different
            # times; now the same offsets in every arm, so they are checked against the reference); every
            # other answer keeps its status, partial flag and series count only (`omitted`).
            if ANSWER_RESULTS_EVERY > 1 and isinstance(result, dict) and \
                    not (user == 0 and ((t_ms - start_ms) // q["interval_ms"]) % ANSWER_RESULTS_EVERY == 0):
                data = result.get("data") if isinstance(result.get("data"), dict) else {}
                result = {k: v for k, v in result.items() if k != "data"}
                result["omitted"] = True
                result["result_series"] = len(data.get("result") or [])
            row = {"query": q["id"], "offset_ms": t_ms - start_ms, "user": user, "latency_ms": latency,
                   "send_lateness_ms": lateness_ms,
                   "status": status, "execution": headers.get("x-asap-execution"),
                   "forward_reason": headers.get("x-asap-forward-reason"), "result": result}
            with lock:
                answers.write(json.dumps(row) + "\n")
            if LIVE is not None:
                labels = {"arm": name, "query": q["id"]}
                if latency is not None:
                    LIVE.set("asap_bench_query_latency_seconds", labels, latency / 1e3)
                LIVE.set("asap_bench_query_evaluation_time_seconds", labels, t_ms / 1e3)
                ok = status == 200 and isinstance(result, dict) and result.get("status") == "success"
                LIVE.inc("asap_bench_query_evaluations_total", {**labels, "outcome": "ok" if ok else "error"})

        sends = sorted(((send_at + u * q["interval_ms"] / 1e3 / QUERY_USERS, t_ms, q, u)
                        for send_at, t_ms, q in events for u in range(QUERY_USERS)), key=lambda e: e[0])
        # QUERY_MAX_INFLIGHT (2026-11-23): requests in flight at once; default 16 per user.
        # Open loop: set it far above the offered concurrency (e.g. 4096) so every request
        # goes out at its scheduled time whatever the engine's latency.
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=int(os.environ.get("QUERY_MAX_INFLIGHT") or 16 * QUERY_USERS)) as pool:
            for send_at, t_ms, q, user in sends:
                wait = send_at - time.time()
                if wait > 0:
                    time.sleep(wait)
                if forwards_before is None:
                    forwards_before = prometheus_query_requests(forward_prom) if forward_prom else 0
                    usage_at_query_start = engine.mark_query()
                    engine.write_state("query", query_url)
                    if LIVE is not None:
                        LIVE.set_phase(name, "query")
                pool.submit(ask, t_ms, q, user, send_at)
                if user == 0 and name in ("asap", "asap_forward", *VM_ARMS):
                    pool.submit(probe, t_ms, q)
        answers.close()
        forwarded = (prometheus_query_requests(forward_prom) - forwards_before) if forward_prom else 0
        destinations = {}
        if name == "asapquery":
            import re
            log = re.sub(r"\x1b\[[0-9;]*m", "", (arm.dir / "asapquery.log").read_text(errors="replace"))
            for d in re.findall(r"destination=([a-z_]+)", log):
                destinations[d] = destinations.get(d, 0) + 1
        end = engine.finish()
        (arm.dir / "probes.jsonl").write_text("".join(json.dumps(p_) + "\n" for p_ in probes))
        status = arm.status(query_url) if name.startswith("backend") or name in ("asap", "asap_forward") else None
        # The engine's own /metrics at the end (data plane: query-time result
        # hits/misses/computes, backlog; VictoriaMetrics: its counters).
        if name in ("asap", "asap_forward", "victoriametrics", "backend", "backend_alone"):
            try:
                (arm.dir / "engine_metrics_end.txt").write_bytes(http_get(f"{query_url}/metrics")[2])
            except Exception as e:  # noqa: BLE001 - the data plane may not serve /metrics yet
                (arm.dir / "engine_metrics_end.txt").write_text(f"# unavailable: {e}\n")
        reference_feed = None
        if name == "victoriametrics":
            reference_feed = finish_reference_feed(arm, query_url, max(t for _, t, _ in events))
            print(f"victoriametrics reference feed: {reference_feed}", flush=True)
        if name == "victoriametrics" and os.environ.get("VM_REFERENCE_QUERIES"):
            vm_reference_queries(arm, query_url, events, start_ms)
        engine.write_state("done", query_url)
        if LIVE is not None:
            LIVE.set_phase(name, "done")
        write_json(arm.dir / "resources.json", {
            "storage_bytes": end["storage"], "data_plane_status": status, "components": end["components"],
            "arm": name, "start_ms": start_ms, "evaluations": len(events),
            "series": read_json(prep / "queries.json")["series"],
            "query_phase_s": query_phase_ms(queries, rounds, query_phase_s) / 1e3,
            "query_delay_s": delay_s,
            "query_users": QUERY_USERS,
            "query_timeout_s": QUERY_TIMEOUT_S,
            "query_max_inflight": int(os.environ.get("QUERY_MAX_INFLIGHT") or 16 * QUERY_USERS),
            "queries_from_start": QUERIES_FROM_START,
            "answer_results_every": ANSWER_RESULTS_EVERY,
            "memory": end["memory"],
            "engine_counters": end["engine_counters"],
            # Enforced limits (identical for every engine; from the planner's cost
            # inputs), what the cgroup saw (OOM, pressure, IO), and the data
            # filesystem's use; an OOM kill or ENOSPC under the limit is a result.
            "limits": LIMITS,
            "component_limits": end.get("component_limits"),
            "cgroup_end": end["cgroup_end"],
            "disk_limit": end["disk_limit"],
            "data_dir_usage": end["data_dir_usage"],
            "enospc_in_logs": end["enospc_in_logs"],
            "engine_exit_codes": end["engine_exit_codes"],
            "wall_seconds": time.time() - wall_started,
            # victoriametrics: the wait for its feeders after the query phase
            # (in wall_seconds, in no phase's CPU); see finish_reference_feed.
            "reference_feed": reference_feed,
            "prometheus_query_requests_during_queries": forwarded,
            "asapquery_answer_destinations": destinations,
            "usage_at_end": end["usage"], "usage_at_query_phase_start": usage_at_query_start,
            "loadavg_at_end": end["loadavg_at_end"], "cpus": end["cpus"],
            "cpusets": end["cpusets"],
            # ENGINE_SSH: where the engines ran, where the feeders and this harness ran,
            # the feeders' usage, and the engine host's network bytes (link headroom).
            "engine_host": end.get("host"), "harness_host": end.get("harness_host", end.get("host")),
            "feeder_usage_at_end": end.get("feeder_usage"),
            "network": end.get("network"),
        })
    finally:
        arm.stop()  # without ENGINE_SSH: the engines and the feeders; with it: the feeders
        if ENGINE_SSH:
            engine.stop()
            engine.fetch()


def main():
    import sys
    if "--agent" in sys.argv:
        agent()
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prep", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--arms", default="prometheus,asapquery,backend")
    parser.add_argument("--rounds", type=int, default=3,
                        help="rounds of the longest interval after the longest lookback")
    parser.add_argument("--query-phase-s", type=float,
                        help="seconds of queries after the longest lookback (instead of --rounds)")
    parser.add_argument("--concurrent", action="store_true")
    parser.add_argument("--metrics-port", type=int,
                        help="serve the harness' live /metrics here for the monitoring Prometheus")
    parser.add_argument("--arm-cores", type=int, default=0,
                        help="pin each arm's engine to its own cpuset of this many cores (same count per "
                             "arm); feeders and this harness run on the remaining cores. 0: no pinning")
    parser.add_argument("--arm-cpus", default="",
                        help="explicit cpusets instead, e.g. 'asap=0-15;victoriametrics=16-31;prometheus=32-47'")
    parser.add_argument("--metrics-linger-s", type=float, default=30,
                        help="keep serving /metrics this long after the last arm, for a last scrape")
    args = parser.parse_args()
    # Processes run in their arm's directory: paths must not be relative.
    args.prep, args.out = args.prep.resolve(), args.out.resolve()
    arms = args.arms.split(",")
    global LIVE, ARM_CPUS, AUX_CPUS
    if ENGINE_SSH:
        # The engine host gets the same paths: this harness and the prepared workload.
        ssh = " ".join(["ssh", *os.environ.get("ENGINE_SSH_OPTS", "").split()])
        here = Path(__file__).resolve().parent
        for src in (here, args.prep):
            subprocess.run(["ssh", *os.environ.get("ENGINE_SSH_OPTS", "").split(), ENGINE_SSH,
                            "mkdir", "-p", str(src), str(args.out)], check=True)
            subprocess.run(["rsync", "-a", "--delete", "-e", ssh, f"{src}/", f"{ENGINE_SSH}:{src}/"], check=True)
        if args.arm_cpus:
            # Cpusets on the engine host (the agent applies them); nothing is pinned here.
            ARM_CPUS = {a: parse_cpus(c) for a, c in (x.split("=", 1) for x in args.arm_cpus.split(";") if x)}
            print("engine host cpusets:", {a: fmt_cpus(c) for a, c in ARM_CPUS.items()}, flush=True)
        elif args.arm_cores:
            raise SystemExit("with ENGINE_SSH, give the engine host's cpusets with --arm-cpus")
    elif args.arm_cores or args.arm_cpus:
        ARM_CPUS, AUX_CPUS = plan_cpus(arms, args.arm_cores, args.arm_cpus)
        os.sched_setaffinity(0, AUX_CPUS)  # the harness: scheduler and query client
        print("cpusets:", {a: fmt_cpus(c) for a, c in ARM_CPUS.items()}, "aux:", fmt_cpus(AUX_CPUS), flush=True)
    if args.metrics_port:
        LIVE = LiveMetrics(args.out.name)
        LIVE.serve(args.metrics_port)
    if args.concurrent:
        start_ms = (int(time.time()) + 45) // 60 * 60 * 1000 + 60_000
        threads = [threading.Thread(target=run_arm, args=(a, args.prep, args.out, args.rounds, start_ms,
                                                          args.query_phase_s)) for a in arms]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    else:
        for a in arms:
            run_arm(a, args.prep, args.out, args.rounds, query_phase_s=args.query_phase_s)
    if LIVE is not None:
        time.sleep(args.metrics_linger_s)


if __name__ == "__main__":
    main()
