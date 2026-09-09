#!/usr/bin/env python3
"""Own fresh baseline/fallback services and retain repeated comparison evidence."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request

from compare import process_snapshot
from process_lifecycle import stop


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("prometheus", "metrics", "queries", "snapshot", "compiler", "data-plane", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--repetitions", type=int, default=20)
    parser.add_argument("--evaluation-step-ms", type=int, default=0)
    parser.add_argument("--batch-resources", action="store_true")
    parser.add_argument("--cpu-affinity", required=True)
    parser.add_argument("--base-port", type=int, default=19410)
    args = parser.parse_args()
    if args.trials < 1 or args.repetitions < 1:
        parser.error("trials and repetitions must be positive")
    if args.base_port < 1 or args.base_port + args.trials * 3 - 1 > 65535:
        parser.error("trial port range must fit TCP ports")
    cpus = {int(value) for value in args.cpu_affinity.split(",")}
    if not cpus or not cpus <= os.sched_getaffinity(0):
        parser.error("requested CPUs are unavailable")
    args.output.mkdir(parents=True, exist_ok=False)
    save(args.output / "manifest.json", {
        "configuration": {key: str(value) for key, value in vars(args).items()},
        "prometheus_sha256": hashlib.sha256(args.prometheus.read_bytes()).hexdigest(),
        "limitations": ["Fresh processes and empty TSDBs; OS caches are not evicted",
                        "CPU affinity is shared, not an aggregate quota or memory cap",
                        "Finite-input replay; repeated queries retain original evaluation times"]})
    for trial in range(1, args.trials + 1):
        # Separate ports avoid previous trial connections still in TIME_WAIT.
        base_port = args.base_port + (trial - 1) * 3
        folder = args.output / f"trial-{trial}"
        folder.mkdir()
        config = folder / "prometheus.yml"
        config.write_text("global:\n  scrape_interval: 1h\nscrape_configs: []\n")
        children, logs, evidence = {}, [], {}
        try:
            for index, name in enumerate(("baseline", "fallback")):
                port = base_port + index
                command = [str(args.prometheus.resolve()), f"--config.file={config.resolve()}",
                           f"--storage.tsdb.path={(folder / name).resolve()}",
                           f"--web.listen-address=127.0.0.1:{port}",
                           "--web.enable-remote-write-receiver", "--storage.tsdb.retention.time=1000000h"]
                log = (folder / f"{name}.log").open("w")
                logs.append(log)
                started = time.perf_counter_ns()
                child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                         preexec_fn=lambda: os.sched_setaffinity(0, cpus))
                children[name] = child
                for attempt in range(240):
                    if child.poll() is not None:
                        raise RuntimeError(f"{name} exited before readiness")
                    try:
                        with urllib.request.urlopen(f"http://127.0.0.1:{port}/-/ready", timeout=1) as response:
                            if response.status == 200:
                                break
                    except OSError:
                        pass
                    time.sleep(0.25)
                else:
                    raise RuntimeError(f"{name} readiness timeout")
                evidence[name] = {"command": command, "ready": process_snapshot(child.pid),
                                  "startup_wall_ns": time.perf_counter_ns() - started}
            command = [sys.executable, str(Path(__file__).with_name("replay.py"))]
            for name in ("metrics", "queries", "snapshot", "compiler", "data_plane"):
                command += ["--" + name.replace("_", "-"), str(getattr(args, name).resolve())]
            command += ["--compare", "--exact-url", f"http://127.0.0.1:{base_port}",
                        "--fallback-url", f"http://127.0.0.1:{base_port + 1}",
                        "--exact-pid", str(children["baseline"].pid),
                        "--fallback-pid", str(children["fallback"].pid),
                        "--exact-storage", str((folder / "baseline").resolve()),
                        "--fallback-storage", str((folder / "fallback").resolve()),
                        "--cpu-affinity", args.cpu_affinity, "--port", str(base_port + 2),
                        "--repetitions", str(args.repetitions), "--relative-tolerance", "1e-9",
                        "--absolute-tolerance", "1e-12", "--output", str((folder / "replay").resolve())]
            command += ["--evaluation-step-ms", str(args.evaluation_step_ms)]
            if args.batch_resources:
                command.append("--batch-resources")
            save(folder / "command.json", command)
            subprocess.run(command, check=True)
        finally:
            cleanup_errors = []
            for name, child in children.items():
                try:
                    evidence.setdefault(name, {})["termination"] = stop(child)
                except Exception as error:
                    # An error collecting one service must not orphan the other.
                    evidence.setdefault(name, {})["termination_error"] = repr(error)
                    cleanup_errors.append(f"{name}: {error}")
                    try:
                        child.kill()
                        child.wait(timeout=10)
                    except Exception as kill_error:
                        evidence[name]["cleanup_error"] = repr(kill_error)
            try:
                save(folder / "service-lifecycle.json", evidence)
            finally:
                for log in logs:
                    log.close()
            if cleanup_errors and sys.exc_info()[0] is None:
                raise RuntimeError("Service cleanup failed: " + "; ".join(cleanup_errors))
        print(f"completed trial {trial}: {folder}", flush=True)


if __name__ == "__main__":
    main()
