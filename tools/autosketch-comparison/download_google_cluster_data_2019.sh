#!/usr/bin/env bash
set -euo pipefail

destination=${1:-/mydata/datasets/google-cluster-data-2019}
mkdir -p "${destination}/cell-a"

curl --location --fail --show-error \
  --output "${destination}/cell-a/instance_usage-000000000000.parquet.gz" \
  https://storage.googleapis.com/clusterdata_2019_a/instance_usage-000000000000.parquet.gz
curl --location --fail --show-error \
  --output "${destination}/instance_usage.schema.json" \
  https://storage.googleapis.com/clusterdata_2019_schema/instance_usage.schema.json

cd "${destination}"
sha256sum --check <<'CHECKSUMS'
c70b806cb89f110a4235f40285957f0244edf49f28c2a83803646a911d0d6fb6  cell-a/instance_usage-000000000000.parquet.gz
82c1ffb74988c2b7a7b60a134840bb234f588949950bfab762dac8863a72b559  instance_usage.schema.json
CHECKSUMS
