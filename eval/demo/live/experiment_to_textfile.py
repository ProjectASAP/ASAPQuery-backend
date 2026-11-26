#!/usr/bin/env python3
"""The presentation dashboard's "from 3-round experiment" numbers: read the
report generator's report_data.json (report_demo/build.py) and write them
as metrics for node-exporter's textfile collector.

    experiment_to_textfile.py --report-data report_demo/out/report_data.json --textfile ../deployment/monitoring/textfile/asap_demo_experiment.prom

One config is shown: --config (a substring of its `config` title, e.g.
"asap-demo · 10,000 series · range"), else the last one. Keys read from its
`numbers` (each {mean, min, max, rounds}; the mean is shown), per engine
(`asap`, `victoriametrics`):
  <engine>/cpu_ingest_window, <engine>/cpu_query_window   core-seconds
  <engine>/peak_rss_mib, <engine>/steady_rss_mib          MiB
  <engine>/disk_write_mib                                 MiB
  <engine>/ingest_ceiling_samples_per_s                   samples/s (the ingest-limit experiment)
  <engine>/cores                                          cores the engine ran on
  <engine>/feeder_max_lag_s, <engine>/feeder_overrun_ticks the engine's feeder in the query phase
  <engine>/ingest_falling_behind                          1 if that lag exceeded the query delay
  query_delay_s                                           the query delay
  <engine>/max_rel_error, <engine>/points_within_target_share   accuracy against the post-run truth
A key the file does not have is left out (the panel shows "No data").
"""
import argparse
import json
from pathlib import Path

ENGINES = ("asap", "victoriametrics")
MIB = 2**20


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--report-data", required=True)
    ap.add_argument("--textfile", required=True)
    ap.add_argument("--config", default="")
    args = ap.parse_args()
    lines = []
    try:
        data = json.loads(Path(args.report_data).read_text())
    except (OSError, ValueError):
        data = None
    configs = data if isinstance(data, list) else (data or {}).get("configs", [])
    chosen = [c for c in configs if args.config in c.get("config", "")] or configs
    if not chosen:
        lines.append("asap_demo_experiment_available 0")
    else:
        c = chosen[-1]
        numbers = c.get("numbers", {})
        rounds = max((v.get("rounds", 0) for v in numbers.values() if isinstance(v, dict)), default=0)
        title = c.get("config", "").replace('"', "'")
        lines.append(f'asap_demo_experiment_available{{config="{title}",rounds="{rounds}"}} 1')
        if mean("query_delay_s") is not None:
            lines.append(f"asap_demo_experiment_query_delay_seconds {mean('query_delay_s')}")

        def mean(key):
            v = numbers.get(key)
            return v.get("mean") if isinstance(v, dict) else v

        for e in ENGINES:
            for phase in ("ingest", "query"):
                v = mean(f"{e}/cpu_{phase}_window")
                if v is not None:
                    lines.append(f'asap_demo_experiment_cpu_core_seconds{{engine="{e}",phase="{phase}"}} {v}')
            for kind in ("peak", "steady"):
                v = mean(f"{e}/{kind}_rss_mib")
                if v is not None:
                    lines.append(f'asap_demo_experiment_rss_bytes{{engine="{e}",kind="{kind}"}} {v * MIB}')
            v = mean(f"{e}/disk_write_mib")
            if v is not None:
                lines.append(f'asap_demo_experiment_disk_written_bytes{{engine="{e}"}} {v * MIB}')
            for key, metric in (("feeder_max_lag_s", "feeder_max_lag_seconds"),
                                ("feeder_overrun_ticks", "feeder_overrun_ticks"),
                                ("ingest_falling_behind", "ingest_falling_behind")):
                v = mean(f"{e}/{key}")
                if v is not None:
                    lines.append(f'asap_demo_experiment_{metric}{{engine="{e}"}} {float(v)}')
            for key, metric in (("max_rel_error", "max_rel_error"),
                                ("points_within_target_share", "points_within_target_share")):
                v = mean(f"{e}/{key}")
                if v is not None:
                    lines.append(f'asap_demo_experiment_{metric}{{engine="{e}"}} {float(v)}')
            v = mean(f"{e}/ingest_ceiling_samples_per_s")
            cores = mean(f"{e}/cores")
            if v is not None:
                lines.append(f'asap_demo_experiment_ingest_ceiling_samples_per_second{{engine="{e}",'
                             f'cores="{int(cores) if cores else "?"}"}} {v}')
    out = Path(args.textfile)
    tmp = out.with_name("." + out.name + ".tmp")
    tmp.write_text("\n".join(lines) + "\n")
    tmp.rename(out)


if __name__ == "__main__":
    main()
