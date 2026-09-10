#!/usr/bin/env python3
"""Build reusable measured ERP evidence, run held-out trials, record provenance."""
import argparse
import hashlib
import json
import pathlib
import platform
import subprocess


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", type=pathlib.Path, required=True)
    p.add_argument("--google-replay", type=pathlib.Path, required=True)
    p.add_argument("--revision", required=True)
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    binary = pathlib.Path("target/release/examples/topk_dashboard_comparison")
    commands = []

    def run(name, extra):
        output = args.output / name
        command = [str(binary), "--output", str(output), "--backend-revision", args.revision, "--trials", "3", *extra]
        commands.append(command)
        if output.exists():
            existing = json.loads(output.read_text())
            recorded = existing.get("args", existing.get("provenance", {}).get("args", {}))
            if recorded.get("backend_revision") != args.revision:
                raise ValueError(f"stale output: {output}")
        else:
            with output.with_suffix(".log").open("w") as log:
                subprocess.run(command, stdout=log, stderr=log, check=True)
        return json.loads(output.read_text())

    catalogs = [
        ("zipf", run("zipf-profile.json", ["--build-erp", "--seed", "1000"])),
        ("uniform", run("uniform-profile.json", ["--build-erp", "--distribution", "uniform", "--seed", "2000", "--total-events", "1000000"])),
        ("google", run("google-profile.json", ["--build-erp", "--input-tsv", str(args.google_replay)])),
    ]
    combined = {"artifact": {"schema_version": 1, "producer_version": args.revision, "records": []},
                "shapes": {}, "generation_seconds": 0, "provenance": {"sources": []}}
    for name, catalog in catalogs:
        for key, value in catalog["shapes"].items():
            combined["shapes"][f"{name}/{key}"] = value
        for row in catalog["artifact"]["records"]:
            row["id"] = name + "/" + row["id"]
            row["distribution"]["profile"] = name + "/" + row["distribution"]["profile"]
            combined["artifact"]["records"].append(row)
        combined["generation_seconds"] += catalog["generation_seconds"]
        combined["provenance"]["sources"].append({"name": name, "generation_seconds": catalog["generation_seconds"], **catalog["provenance"]})
    catalog_path = args.output / "catalog.json"
    catalog_path.write_text(json.dumps(combined, indent=2) + "\n")
    run("synthetic.json", ["--erp-catalog", str(catalog_path), "--seed", "42"])
    run("google.json", ["--erp-catalog", str(catalog_path), "--input-tsv", str(args.google_replay)])
    provenance = {"commands": commands, "platform": platform.platform(),
                  "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                  "google_replay_sha256": hashlib.sha256(args.google_replay.read_bytes()).hexdigest(),
                  "sketchlib_revision": subprocess.check_output(["git", "-C", "../asap_sketchlib", "rev-parse", "HEAD"], text=True).strip(),
                  "planner_revision": "a9651cc", "backend_revision": args.revision,
                  "isolation": "sequential wall-clock runs on shared host; no CPU isolation"}
    (args.output / "manifest.json").write_text(json.dumps(provenance, indent=2) + "\n")


if __name__ == "__main__":
    main()
