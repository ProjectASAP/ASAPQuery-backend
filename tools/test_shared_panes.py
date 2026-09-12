#!/usr/bin/env python3
"""Validate the unmerged Planner/backend pair without changing immutable IR pins."""
import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--planner", required=True, type=Path)
    parser.add_argument("--sketchlib", type=Path)
    parser.add_argument("--toolchain")
    parser.add_argument("cargo_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    backend = Path(__file__).resolve().parents[1]
    manifest = (backend / "control_plane/Cargo.toml").read_text()
    revision = re.search(r'planner-types = .*rev = "([0-9a-f]+)"', manifest).group(1)
    lock = backend / "Cargo.lock"
    original_lock = lock.read_bytes()
    with tempfile.TemporaryDirectory(prefix="asap-pane-reuse-") as temporary:
        root = Path(temporary)
        mapping = root / "asap-aware-mapping"
        shutil.copytree(args.planner.resolve() / "crates/asap-aware-mapping", mapping)
        cargo_toml = mapping / "Cargo.toml"
        source = cargo_toml.read_text()
        old = 'asap-types = { path = "../types" }'
        if old not in source:
            raise RuntimeError("unexpected Planner dependency declaration")
        cargo_toml.write_text(source.replace(old, 'asap-types = { git = "https://github.com/ProjectASAP/ASAPPlanner", rev = "' + revision + '" }'))
        config = root / "validation.toml"
        text = ""
        if args.sketchlib:
            text += "paths = [" + json.dumps(str(args.sketchlib.resolve())) + "]\n"
        text += '[patch."https://github.com/ProjectASAP/ASAPPlanner"]\nasap-aware-mapping = { path = ' + json.dumps(str(mapping)) + ' }\n'
        config.write_text(text)
        command = ["cargo"]
        if args.toolchain:
            command.append("+" + args.toolchain)
        cargo_args = args.cargo_args
        if cargo_args[:1] == ["--"]:
            cargo_args = cargo_args[1:]
        command += ["--config", str(config)] + (cargo_args or ["test", "-p", "control_plane"])
        try:
            return subprocess.call(command, cwd=backend)
        finally:
            lock.write_bytes(original_lock)


if __name__ == "__main__":
    raise SystemExit(main())
