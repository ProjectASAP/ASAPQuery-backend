"""Paths and small helpers shared by the harness scripts.

The base directory is $ASAPQ_BENCH (required). Every
binary path can be overridden by an environment variable: FEEDER_BIN,
ASAPQUERY_BIN, CONTROL_PLANE_BIN, DATA_PLANE_BIN, CONVERTER_BIN,
PROMETHEUS_BIN, VICTORIAMETRICS_BIN. Without one, a binary is looked up in
$BIN_DIR (required), then in CARGO_TARGET_DIR/release (Rust builds), then in
BASE/bin."""
import json
import os
from pathlib import Path

BENCH = Path(os.environ["ASAPQ_BENCH"])
ASAPQ = BENCH / "ASAPQuery"
WORKLOADS = ASAPQ / "asap-tools/experiments/datasets/fake_metric/random_workloads"
# Workload manifests written for the demo (same format as ASAPQuery's).
DEMO_WORKLOADS = BENCH / "workloads"
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", "target")) / "release"

# Pinned releases (../fetch_binaries.sh downloads and checks them).
PROMETHEUS_VERSION = "3.5.0"
# VICTORIAMETRICS_VERSION=v1.155.0 runs an older pinned release.
VICTORIAMETRICS_VERSION = os.environ.get("VICTORIAMETRICS_VERSION", "v1.156.0")


BIN_DIR = Path(os.environ["BIN_DIR"])


def _bin(env, *candidates):
    """$env if set, else the first candidate that exists (else the first)."""
    if os.environ.get(env):
        return Path(os.environ[env])
    return next((c for c in candidates if c.exists()), candidates[0])


BIN = {
    "feeder": _bin("FEEDER_BIN", BIN_DIR / "paced_feeder", TARGET / "paced_feeder"),
    "asapquery": _bin("ASAPQUERY_BIN", TARGET / "query_engine_rust", BIN_DIR / "query_engine_rust"),
    "control_plane": _bin("CONTROL_PLANE_BIN", BIN_DIR / "control_plane", TARGET / "control_plane"),
    "data_plane": _bin("DATA_PLANE_BIN", BIN_DIR / "data_plane", TARGET / "data_plane"),
    "converter": _bin("CONVERTER_BIN", TARGET / "examples/asapquery_random_workload"),
    "prometheus": _bin("PROMETHEUS_BIN", BIN_DIR / f"prometheus-{PROMETHEUS_VERSION}",
                       BENCH / f"bin/prometheus-{PROMETHEUS_VERSION}"),
    "victoriametrics": _bin("VICTORIAMETRICS_BIN", BIN_DIR / f"victoria-metrics-{VICTORIAMETRICS_VERSION}",
                            BENCH / f"bin/victoria-metrics-{VICTORIAMETRICS_VERSION}"),
    # VictoriaMetrics cluster (2026-11-22, the vmcluster arm): the same release's
    # cluster binaries, installed as BIN_DIR/<component>-<version>.
    **{c: BIN_DIR / f"{c}-{VICTORIAMETRICS_VERSION}" for c in ("vmstorage", "vmselect", "vminsert")},
}


def manifest_path(workload_set, workload):
    if workload_set == "demo":
        return DEMO_WORKLOADS / f"{workload}.json"
    return WORKLOADS / workload_set / f"{workload}.json"


def pilot_dir(workload_set, workload):
    return WORKLOADS / workload_set / f"{workload}-pilot"


def read_json(path):
    return json.loads(Path(path).read_text())


def write_json(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
