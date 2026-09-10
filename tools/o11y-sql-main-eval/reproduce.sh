#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
artifact_dir="$repo_root/tools/o11y-sql-main-eval/artifacts"
corpus="$repo_root/tools/o11y-sql-main-eval/corpus.json"
baseline=791f7d7b0feab3827e7e6f5100e65ef29aec68de

export CLICKHOUSE_URL=${CLICKHOUSE_URL:-http://127.0.0.1:18123}
: "${CLICKHOUSE_USER:?set CLICKHOUSE_USER for the exact ClickHouse backend}"
: "${CLICKHOUSE_PASSWORD:?set CLICKHOUSE_PASSWORD for the exact ClickHouse backend}"

if [[ $(git merge-base "$baseline" HEAD) != "$baseline" ]]; then
  echo "HEAD does not descend from recorded main baseline $baseline" >&2
  exit 1
fi

mkdir -p "$artifact_dir"
cargo run -q -p control_plane --example audit_clickhouse_corpus -- "$corpus" \
  > "$artifact_dir/frontend-planner-publication.json"
cargo run -q -p data_plane --example audit_clickhouse_fallback -- "$corpus" \
  > "$artifact_dir/data-plane-routing.json"
python3 "$repo_root/tools/o11y-sql-main-eval/summarize.py" \
  --planner "$artifact_dir/frontend-planner-publication.json" \
  --runtime "$artifact_dir/data-plane-routing.json" \
  --output "$artifact_dir/matrix.json"
sha256sum "$corpus" > "$artifact_dir/corpus.sha256"
git -C "$repo_root" show -s --format='%H %s' "$baseline" > "$artifact_dir/baseline.txt"
