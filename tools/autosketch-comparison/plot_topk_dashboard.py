#!/usr/bin/env python3
"""Render median Top-K dashboard results from the committed raw measurements."""
import argparse
import json
import statistics


def median_rows(document):
    grouped = {}
    for trial in document["trials"]:
        for row in trial["rows"]:
            grouped.setdefault(row["method"], []).append(row)
    result = []
    for method, rows in grouped.items():
        median = lambda field: statistics.median(field(row) for row in rows)
        result.append({
            "method": method,
            "planning": median(lambda row: row["planning_seconds"]),
            "runtime": median(lambda row: row["timing"]["exact_query_seconds"]
                if row["method"] == "exact_production" else sum(row["timing"][key] for key in
                ["update_seconds", "eviction_seconds", "merge_seconds", "topk_readout_seconds"])),
            "memory": median(lambda row: row["logical_payload_bytes"]),
            "recall": median(lambda row: row["mean_recall_at_10"]),
        })
    return result


def panel(rows, x, title, value, scale, unit):
    width, height = 430, 250
    maximum = max(row[value] for row in rows) or 1
    parts = [f'<g transform="translate({x},55)"><text x="215" y="-18" text-anchor="middle" font-size="16">{title}</text>']
    for index, row in enumerate(rows):
        y = 10 + index * 43
        bar = row[value] / maximum * 230
        label = row["method"].replace("asapplanner_", "ASAP-").replace("autosketch_per_query", "AutoSketch").replace("exact_production", "Exact")
        parts.append(f'<text x="0" y="{y+14}" font-size="11">{label}</text><rect x="155" y="{y}" width="{bar:.2f}" height="18" fill="#4c78a8"/><text x="{160+bar:.2f}" y="{y+14}" font-size="10">{row[value]*scale:.3g}{unit}</text>')
    parts.append("</g>")
    return "".join(parts)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("output")
    parser.add_argument("--title", required=True)
    args = parser.parse_args()
    rows = median_rows(json.load(open(args.input, encoding="utf-8")))
    svg = ['<svg xmlns="http://www.w3.org/2000/svg" width="1320" height="340" viewBox="0 0 1320 340">',
           '<rect width="100%" height="100%" fill="white"/>',
           f'<text x="660" y="28" text-anchor="middle" font-size="20">{args.title}</text>',
           panel(rows, 10, "Planning time", "planning", 1, "s"),
           panel(rows, 445, "Dashboard runtime", "runtime", 1, "s"),
           panel(rows, 880, "Retained logical payload", "memory", 1 / 1048576, "MiB"),
           '</svg>']
    with open(args.output, "w", encoding="utf-8") as target:
        target.write("".join(svg))


if __name__ == "__main__":
    main()
