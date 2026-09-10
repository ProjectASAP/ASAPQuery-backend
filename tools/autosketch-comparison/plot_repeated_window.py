#!/usr/bin/env python3
"""Render repeated-window raw results as a dependency-free SVG."""
import argparse
import html
import json
import statistics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("output")
    args = parser.parse_args()
    with open(args.input, encoding="utf-8") as source:
        report = json.load(source)
    grouped = {}
    for trial in report["trials"]:
        for row in trial["methods"]:
            grouped.setdefault(row["method"], []).append(row)
    order = ["autosketch_per_query", "asap_no_sharing", "asap_full_shared", "exact_raw"]
    labels = ["AutoSketch / PerQuery", "ASAP / NoSharing", "ASAP / Full", "Exact / Raw"]
    colors = ["#7086a3", "#91a4ba", "#e47732", "#6b9d72"]
    panels = [
        ("Update wall time (s)", "update_wall_seconds", 1.0),
        ("Query wall time (s)", "query_wall_seconds", 1.0),
        ("Logical retained memory (MiB)", "logical_payload_bytes", 1 / 1048576),
        ("Max normalized error", "max_normalized_additive_error", 1.0),
    ]
    parts = ['<svg xmlns="http://www.w3.org/2000/svg" width="1080" height="600" viewBox="0 0 1080 600">',
             '<rect width="100%" height="100%" fill="white"/>',
             '<style>text{font-family:system-ui,sans-serif;fill:#253047}.title{font-size:19px;font-weight:700}.axis{font-size:11px}.value{font-size:11px;font-weight:600}</style>',
             '<text x="540" y="30" text-anchor="middle" class="title">Repeated-window comparison — median of 7 executed release trials</text>']
    for panel, (title, field, scale) in enumerate(panels):
        x0, y0 = 55 + (panel % 2) * 530, 65 + (panel // 2) * 265
        chart_h, chart_w = 170, 455
        values = [statistics.median(row[field] for row in grouped[name]) * scale for name in order]
        maximum = max(values) or 1
        parts += [f'<text x="{x0}" y="{y0}" class="title">{html.escape(title)}</text>',
                  f'<line x1="{x0}" y1="{y0+chart_h}" x2="{x0+chart_w}" y2="{y0+chart_h}" stroke="#526075"/>']
        for index, value in enumerate(values):
            x, bar_width = x0 + 12 + index * 112, 78
            bar_height = value / maximum * (chart_h - 35)
            y = y0 + chart_h - bar_height
            shown = f"{value:.4f}" if value < 10 else f"{value:,.0f}"
            parts += [f'<rect x="{x}" y="{y:.1f}" width="{bar_width}" height="{bar_height:.1f}" rx="3" fill="{colors[index]}"/>',
                      f'<text x="{x+bar_width/2}" y="{y-5:.1f}" text-anchor="middle" class="value">{shown}</text>',
                      f'<text x="{x+bar_width/2}" y="{y0+chart_h+18}" text-anchor="middle" class="axis">{html.escape(labels[index].split(" / ")[0])}</text>',
                      f'<text x="{x+bar_width/2}" y="{y0+chart_h+31}" text-anchor="middle" class="axis">{html.escape(labels[index].split(" / ")[1])}</text>']
    parts.append('</svg>')
    with open(args.output, "w", encoding="utf-8") as target:
        target.write("\n".join(parts) + "\n")


if __name__ == "__main__":
    main()
