#!/usr/bin/env python3
"""Compression of the run's data, bits per sample (2026-11-21).

usage: compression.py <run dir> [vm binary]

- VictoriaMetrics: its disk bytes per stored sample, from its own /metrics at the
  end of its arm (engine_metrics_end.txt): sum(vm_data_size_bytes{type=~"storage/.*"})
  / sum(vm_rows{type=~"storage/.*"}).
- Gorilla (Facebook's in-memory TSDB codec: delta-of-delta timestamps, XOR values) as
  a reference over the generated data: SAMPLE_SERIES (50) series per metric exported
  from VictoriaMetrics' kept data (the same data both engines received), whole run.
- ASAP: its views are summaries, not samples; reported as view bytes (in memory and
  persisted) per ingested sample, for scale only.
Writes <run>/compression.json.
"""
import os
import json, random, re, subprocess, sys, time, urllib.parse, urllib.request
from pathlib import Path

run = Path(sys.argv[1])
VM = sys.argv[2] if len(sys.argv) > 2 else os.environ["BIN_DIR"] + "/victoria-metrics-v1.156.0"
PORT = 19778
SAMPLE_SERIES = 50


def metrics(path):
    out = {}
    if path.exists():
        for line in path.read_text().splitlines():
            if line and not line.startswith("#"):
                k, _, v = line.rpartition(" ")
                try:
                    out[k] = float(v)
                except ValueError:
                    pass
    return out


class Bits:
    def __init__(self):
        self.n = 0

    def put(self, nbits):
        self.n += nbits


def gorilla_bits(ts, vs):
    """Bits to encode one series with Gorilla (Pelkonen et al., VLDB 2015)."""
    import struct
    b = Bits()
    b.put(64 + 64)  # first timestamp and value raw
    if len(ts) < 2:
        return b.n
    prev_t, prev_delta = ts[0], None
    prev_v = struct.unpack(">Q", struct.pack(">d", vs[0]))[0]
    lead, trail = None, None
    for t, v in zip(ts[1:], vs[1:]):
        delta = t - prev_t
        if prev_delta is None:
            b.put(14)  # first delta in 14 bits (as in the paper's block header)
        else:
            dod = delta - prev_delta
            if dod == 0:
                b.put(1)
            elif -63 <= dod <= 64:
                b.put(2 + 7)
            elif -255 <= dod <= 256:
                b.put(3 + 9)
            elif -2047 <= dod <= 2048:
                b.put(4 + 12)
            else:
                b.put(4 + 32)
        prev_t, prev_delta = t, delta
        cur = struct.unpack(">Q", struct.pack(">d", v))[0]
        x = cur ^ prev_v
        if x == 0:
            b.put(1)
        else:
            lz = 64 - x.bit_length()
            lz = min(lz, 31)
            tz = (x & -x).bit_length() - 1
            if lead is not None and lz >= lead and tz >= trail:
                b.put(2 + (64 - lead - trail))
            else:
                lead, trail = lz, tz
                b.put(2 + 5 + 6 + (64 - lz - tz))
        prev_v = cur
    return b.n


def main():
    out = {}
    m = metrics(run / "victoriametrics" / "engine_metrics_end.txt")
    size = sum(v for k, v in m.items() if k.startswith('vm_data_size_bytes{type="storage/'))
    rows = sum(v for k, v in m.items() if k.startswith('vm_rows{type="storage/'))
    if rows:
        out["victoriametrics_disk_bits_per_sample"] = 8 * size / rows
        out["victoriametrics_storage_bytes"] = size
        out["victoriametrics_stored_samples"] = rows
    a = metrics(run / "asap" / "engine_metrics_end.txt")
    ingested = a.get('asap_samples_total{outcome="accepted"}')
    if ingested:
        out["asap_view_bits_per_ingested_sample"] = 8 * a.get("asap_view_bytes", 0) / ingested
        out["asap_persisted_view_bits_per_ingested_sample"] = 8 * a.get("asap_view_persisted_bytes_total", 0) / ingested
    data = run / "victoriametrics" / "victoria-metrics-data"
    if data.exists() and any(data.iterdir()):
        vm = subprocess.Popen([VM, f"-storageDataPath={data}", f"-httpListenAddr=127.0.0.1:{PORT}", "-retentionPeriod=100y",
                               "-loggerLevel=ERROR"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(240):
                try:
                    urllib.request.urlopen(f"http://127.0.0.1:{PORT}/health", timeout=2); break
                except Exception:  # noqa: BLE001
                    time.sleep(0.5)
            per_metric = {}
            # The series API ignores start=0 (it returned nothing); the run's own start.
            try:
                start_s = json.load(open(run / "victoriametrics" / "resources.json"))["start_ms"] // 1000 - 60
            except (OSError, ValueError, KeyError):
                start_s = int(time.time()) - 86400
            names_all = json.load(urllib.request.urlopen(f"http://127.0.0.1:{PORT}/api/v1/label/__name__/values?" +
                                    urllib.parse.urlencode({"start": str(start_s)}), timeout=600))["data"]
            for metric in names_all:
                names = json.load(urllib.request.urlopen(
                    f"http://127.0.0.1:{PORT}/api/v1/series?" + urllib.parse.urlencode(
                        {"match[]": metric, "start": str(start_s)}), timeout=600))["data"]
                random.Random(862).shuffle(names)
                bits = samples = 0
                for labels in names[:SAMPLE_SERIES]:
                    sel = "{" + ",".join(f'{k}="{v}"' for k, v in labels.items()) + "}"
                    body = urllib.request.urlopen(f"http://127.0.0.1:{PORT}/api/v1/export?" + urllib.parse.urlencode(
                        {"match[]": sel, "start": str(start_s)}), timeout=600).read().decode()
                    for line in body.splitlines():
                        s = json.loads(line)
                        pairs = sorted(zip(s["timestamps"], s["values"]))
                        ts = [p[0] for p in pairs]
                        vs = [float(p[1]) for p in pairs]
                        bits += gorilla_bits(ts, vs)
                        samples += len(ts)
                if samples:
                    per_metric[metric] = {"gorilla_bits_per_sample": bits / samples, "series": min(len(names), SAMPLE_SERIES),
                                          "samples": samples}
            out["gorilla_reference"] = per_metric
        finally:
            vm.terminate()
            vm.wait()
    (run / "compression.json").write_text(json.dumps(out, indent=1))
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
