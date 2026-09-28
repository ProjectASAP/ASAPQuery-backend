#!/usr/bin/env bash
set -euo pipefail

planner_url="https://github.com/ProjectASAP/ASAPPlanner"
planner_rev="$(git ls-remote "$planner_url" refs/heads/main | awk 'NR == 1 { print $1 }')"
if [[ ! "$planner_rev" =~ ^[0-9a-f]{40}$ ]]; then
  echo "could not resolve ASAPPlanner main to a commit" >&2
  exit 1
fi

python3 - "$planner_rev" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path("Cargo.toml")
text = path.read_text()
revision = sys.argv[1]
pattern = re.compile(
    r'(git = "https://github.com/ProjectASAP/ASAPPlanner", rev = ")[0-9a-f]{40}("\s*})'
)
updated, replacements = pattern.subn(rf"\g<1>{revision}\2", text)
if replacements != 4:
    raise SystemExit(f"expected four ASAPPlanner dependencies, found {replacements}")
path.write_text(updated)
PY

cargo update \
  -p asap-types@0.1.0 \
  -p asap-aware-mapping \
  -p asap-frontend-promql \
  -p asap-frontend-sql \
  --precise "$planner_rev"

echo "synchronized ASAPPlanner dependencies to $planner_rev"
