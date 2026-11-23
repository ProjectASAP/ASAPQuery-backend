#!/bin/bash
# Download the pinned third-party binaries the VictoriaMetrics demo uses into
# $BIN_DIR (default $BIN_DIR), each checked against its pinned sha256.
# Already-present binaries are kept. Linux amd64 only.
#
#   BIN_DIR=$BIN_DIR ./fetch_binaries.sh            # everything
#   ./fetch_binaries.sh victoriametrics prometheus     # only these
set -euo pipefail
BIN_DIR=${BIN_DIR:?set BIN_DIR}
mkdir -p "$BIN_DIR"
GH=https://github.com

# name | file installed in BIN_DIR | url | sha256 of the download | path inside the archive
PINS=(
  "victoriametrics|victoria-metrics-v1.156.0|$GH/VictoriaMetrics/VictoriaMetrics/releases/download/v1.156.0/victoria-metrics-linux-amd64-v1.156.0.tar.gz|54409ea381db0316f0bf6f242c718bc498085eb1eb37520b73526e6d86891601|victoria-metrics-prod"
  "prometheus|prometheus-3.5.0|$GH/prometheus/prometheus/releases/download/v3.5.0/prometheus-3.5.0.linux-amd64.tar.gz|e811827af26d822afb09a4f28314f61b618b12cff5369835a67f674d8b46f39a|prometheus-3.5.0.linux-amd64/prometheus"
  "node_exporter|node_exporter-1.12.1|$GH/prometheus/node_exporter/releases/download/v1.12.1/node_exporter-1.12.1.linux-amd64.tar.gz|b51d8a76aa2a9156a55d501aca6276fae09e262259a5e4e831d2c2222f084e63|node_exporter-1.12.1.linux-amd64/node_exporter"
  "process_exporter|process-exporter-0.8.7|$GH/ncabatoff/process-exporter/releases/download/v0.8.7/process-exporter-0.8.7.linux-amd64.tar.gz|6d274cca5e94c6a25e55ec05762a472561859ce0a05b984aaedb67dd857ceee2|process-exporter-0.8.7.linux-amd64/process-exporter"
  # Grafana is a directory (binary + public assets), installed as BIN_DIR/grafana-12.4.14/.
  "grafana|grafana-12.4.14|https://dl.grafana.com/oss/release/grafana-12.4.14.linux-amd64.tar.gz|c19ea4f993bba6e88c1269502c5bed8f8a717d82a6f43b8cc3ee9c960bb5c462|DIR"
  # Brendan Gregg's FlameGraph scripts (stackcollapse-perf.pl, flamegraph.pl), as a directory.
  "flamegraph|FlameGraph-41fee1f|$GH/brendangregg/FlameGraph/archive/41fee1f99f9276008b7cd112fca19dc3ea84ac32.tar.gz|b1ba3aaaf296ab72ced4dca84514377a4b2c8122831d743f726c106d3d339c2a|DIR"
)

want=("$@")
for pin in "${PINS[@]}"; do
  IFS='|' read -r name file url sha inner <<<"$pin"
  if [ ${#want[@]} -gt 0 ] && [[ ! " ${want[*]} " == *" $name "* ]]; then continue; fi
  if [ -e "$BIN_DIR/$file" ]; then echo "have $BIN_DIR/$file"; continue; fi
  tmp=$(mktemp -d)
  echo "fetch $name: $url"
  curl -fsSL --retry 3 -o "$tmp/archive.tar.gz" "$url"
  echo "$sha  $tmp/archive.tar.gz" | sha256sum -c --quiet - || { echo "checksum mismatch for $name" >&2; rm -rf "$tmp"; exit 1; }
  mkdir "$tmp/x"
  tar xzf "$tmp/archive.tar.gz" -C "$tmp/x"
  if [ "$inner" = DIR ]; then
    mv "$tmp/x/"* "$BIN_DIR/$file"
  else
    install -m 0755 "$tmp/x/$inner" "$BIN_DIR/$file"
  fi
  rm -rf "$tmp"
  echo "installed $BIN_DIR/$file"
done
