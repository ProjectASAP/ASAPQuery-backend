#!/usr/bin/env python3
"""Run the control-plane offline example against an explicit planner checkout."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--planner", type=Path, help="optional local planner override; default uses the pinned git dependency")
    parser.add_argument("--queries", required=True, type=Path)
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--context", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if bool(args.evidence) != bool(args.context):
        parser.error("--evidence and --context must be provided together")
    backend = Path(__file__).resolve().parents[1]
    planner = args.planner.resolve() if args.planner else None
    command = ["cargo", "run", "--quiet", "-p", "control_plane", "--example", "offline_planner_replay"]
    for package, directory in ([("asap-types", "types"), ("asap-aware-mapping", "asap-aware-mapping"), ("asap-frontend-promql", "frontend-promql")] if planner else []):
        path = planner / "crates" / directory
        if not (path / "Cargo.toml").is_file():
            parser.error(f"missing planner crate: {path}")
        command.extend(["--config", f'patch."https://github.com/ProjectASAP/ASAPPlanner".{package}.path={json.dumps(str(path))}'])
    command.extend(["--", str(args.queries.resolve())])
    if args.evidence:
        command.extend([str(args.evidence.resolve()), str(args.context.resolve())])
    result = subprocess.run(command, cwd=backend, capture_output=True, text=True)
    if result.returncode:
        sys.stderr.write(result.stderr)
        result.check_returncode()
    report = json.loads(result.stdout)
    report["reproduction"] = {
        "command": command,
        "backend_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=backend, text=True).strip(),
        "planner_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=planner, text=True).strip() if planner else re.search(r'asap-aware-mapping = .*rev = "([a-f0-9]+)"', (backend / "Cargo.toml").read_text()).group(1),
        "backend_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=backend, text=True)),
        "planner_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=planner, text=True)) if planner else False,
        "planner_source": "local_override" if planner else "pinned_git_dependency",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"wrote {len(report['rows'])} query/mode results to {args.output}")


if __name__ == "__main__":
    main()
