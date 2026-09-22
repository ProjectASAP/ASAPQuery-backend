#!/usr/bin/env python3
"""Pin every Planner workspace dependency to one main revision, then resolve it."""
import re
import subprocess
from pathlib import Path

URL = "https://github.com/ProjectASAP/ASAPPlanner"


def pin_planner(manifest: str, revision: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Expected a full Planner commit SHA")
    count = 0
    lines = []
    for line in manifest.splitlines(keepends=True):
        if re.search(r'git\s*=\s*"' + re.escape(URL) + r'"', line):
            line, replaced = re.subn(r'(?:rev|branch|tag)\s*=\s*"[^"]+"', f'rev = "{revision}"', line)
            if replaced != 1:
                raise ValueError("Planner dependency must have exactly one Git revision selector")
            count += 1
        lines.append(line)
    if count != 4:
        raise ValueError(f"Expected four Planner dependencies, found {count}; review the updater")
    return "".join(lines)


if __name__ == "__main__":
    revision = subprocess.check_output(
        ["git", "ls-remote", URL, "refs/heads/main"], text=True
    ).split()[0]
    path = Path("Cargo.toml")
    original = path.read_text()
    updated = pin_planner(original, revision)
    if updated == original:
        print(f"Planner already pinned to main: {revision}")
        raise SystemExit(0)
    path.write_text(updated)
    subprocess.run(["cargo", "update", "-p", "asap-aware-mapping"], check=True)
    print(f"Planner main: {revision}")
