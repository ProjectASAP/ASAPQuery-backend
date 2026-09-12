"""Linux cgroup-v2 accounting; unavailable evidence remains null."""
from pathlib import Path
import argparse
import json
import os
import subprocess
import time


def validate(components):
    roots = [Path(c["cgroup"]).resolve() for c in components.values()]
    if len(set(roots)) != len(roots) or any(a in b.parents for a in roots for b in roots if a != b):
        raise ValueError("component cgroups must be disjoint, without parent/child overlap")
    namespaces = []
    for config in components.values():
        if config.get("network_namespace_pid"):
            identity = Path(f"/proc/{int(config['network_namespace_pid'])}/ns/net").stat().st_ino
            if identity == Path("/proc/1/ns/net").stat().st_ino or identity in namespaces:
                raise ValueError("network accounting requires distinct non-host namespaces")
            namespaces.append(identity)


def snapshot(components):
    result = {}
    for name, config in components.items():
        root = Path(config["cgroup"])
        row = {"sampled_ns": time.monotonic_ns()}
        try:
            row["cpu_usec"] = int(dict(line.split() for line in (root / "cpu.stat").read_text().splitlines())["usage_usec"])
            row["memory_bytes"] = int((root / "memory.current").read_text())
            io = [dict(item.split("=") for item in line.split()[1:]) for line in (root / "io.stat").read_text().splitlines()]
            row["disk_read_bytes"] = sum(int(v.get("rbytes", 0)) for v in io)
            row["disk_write_bytes"] = sum(int(v.get("wbytes", 0)) for v in io)
            row["cgroup_identity"] = root.stat().st_ino
        except (OSError, ValueError, KeyError) as error:
            row["error"] = str(error)
        # A dedicated network namespace is required; host net/dev is not per-process.
        row["network"] = None
        if config.get("network_namespace_pid"):
            try:
                net = Path(f"/proc/{int(config['network_namespace_pid'])}/net/dev").read_text().splitlines()[2:]
                row["network"] = {"rx_bytes": sum(int(x.split(":")[1].split()[0]) for x in net),
                                  "tx_bytes": sum(int(x.split(":")[1].split()[8]) for x in net)}
            except (OSError, ValueError):
                pass
        result[name] = row
    return result


def delta(before, after):
    rows = {}
    for name, initial in before.items():
        end = after[name]
        if "error" in initial or "error" in end or initial.get("cgroup_identity") != end.get("cgroup_identity"):
            rows[name] = {"error": "component accounting unavailable or cgroup replaced"}
            continue
        row = {key: end[key] - initial[key] for key in ("cpu_usec", "disk_read_bytes", "disk_write_bytes")}
        if any(value < 0 for value in row.values()):
            rows[name] = {"error": "accounting counter reset"}
            continue
        row.update(memory_before_bytes=initial["memory_bytes"], memory_after_bytes=end["memory_bytes"])
        row["network"] = ({k: end["network"][k] - initial["network"][k] for k in ("rx_bytes", "tx_bytes")}
                          if initial["network"] and end["network"] else None)
        rows[name] = row
    keys = ("cpu_usec", "disk_read_bytes", "disk_write_bytes", "memory_before_bytes", "memory_after_bytes")
    total = {key: sum(row[key] for row in rows.values()) if rows and all(key in row for row in rows.values()) else None for key in keys}
    return {"components": rows, "total": total,
            "scope": "request interval across listed disjoint cgroups, including background work; endpoint memory snapshots are not peak RSS; network is per dedicated namespace and not summed across links"}


def disk_usage(components):
    """Allocated filesystem blocks, including retained data; not block-device I/O."""
    result = {}
    for name, config in components.items():
        if not config.get("data_directory"):
            result[name] = None
            continue
        try:
            root = Path(config["data_directory"])
            if not root.is_dir():
                raise OSError("data directory unavailable")
            total, seen = 0, set()
            def fail(error):
                raise error
            for directory, _, files in os.walk(root, onerror=fail):
                for path in [Path(directory)] + [Path(directory) / f for f in files]:
                    st = path.lstat()
                    identity = (st.st_dev, st.st_ino)
                    if identity not in seen:
                        total += st.st_blocks * 512
                        seen.add(identity)
            result[name] = total
        except OSError:
            result[name] = None
    return result


def main():
    parser = argparse.ArgumentParser(description="Measure a complete load/build/query phase around an explicit command")
    parser.add_argument("--components", type=Path, required=True)
    parser.add_argument("--engine", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sample-seconds", type=float, default=0.1)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or not 0.01 <= args.sample_seconds <= 60 or args.output.exists():
        parser.error("provide command, unused output, and sample interval in [0.01,60]")
    components = json.loads(args.components.read_text())[args.engine]
    validate(components)
    directories = [Path(c["data_directory"]).resolve() for c in components.values() if c.get("data_directory")]
    if len(set(directories)) != len(directories) or any(a in b.parents for a in directories for b in directories if a != b):
        parser.error("storage directories must be disjoint")
    storage_before = disk_usage(components)
    before = snapshot(components)
    peaks = {name: row.get("memory_bytes") for name, row in before.items()}
    initial_values = list(peaks.values())
    aggregate_peak = sum(initial_values) if initial_values and all(v is not None for v in initial_values) else None
    start = time.monotonic_ns()
    process = subprocess.Popen(command)
    while True:
        sample = snapshot(components)
        for name, row in sample.items():
            value = row.get("memory_bytes")
            peaks[name] = max(peaks[name] or 0, value) if value is not None else peaks[name]
        values = [row.get("memory_bytes") for row in sample.values()]
        if values and all(v is not None for v in values):
            aggregate_peak = max(aggregate_peak or 0, sum(values))
        if process.poll() is not None:
            break
        time.sleep(args.sample_seconds)
    elapsed = time.monotonic_ns() - start
    report = delta(before, sample)
    report.update(scope="whole wrapped command phase, including ingestion, summary build and idle/background work if included in command",
                  engine=args.engine, command=command, returncode=process.returncode, elapsed_ns=elapsed,
                  sampled_memory_peak_bytes=peaks, sampled_total_memory_peak_bytes=aggregate_peak,
                  sample_seconds=args.sample_seconds, disk_allocated_before_bytes=storage_before,
                  disk_allocated_after_bytes=disk_usage(components))
    for key in ("disk_allocated_before_bytes", "disk_allocated_after_bytes"):
        values = list(report[key].values())
        report["total_" + key] = sum(values) if values and all(v is not None for v in values) else None
    with args.output.open("x") as output:
        json.dump(report, output, indent=2)
    return process.returncode


if __name__ == "__main__":
    raise SystemExit(main())
