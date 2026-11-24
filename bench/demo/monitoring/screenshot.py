#!/usr/bin/env python3
"""Render a Grafana dashboard to PNG with headless Chromium: the whole
dashboard (full page) and every panel alone (/d-solo), for one time range.

    screenshot.py --grafana http://127.0.0.1:13000 --uid asap-vs-vm --from <ms|now-1h> --to <ms|now> \
        --out DIR [--vars run=NAME]

Runs inside the pinned Playwright image (screenshot.sh)."""
import argparse
import datetime
import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright


def slug(text):
    return re.sub(r"[^A-Za-z0-9]+", "-", text).strip("-")[:60] or "panel"


def grafana_time(value):
    """Grafana 12 ignores an epoch-milliseconds from/to in the URL (it falls back to the
    dashboard's default range); an ISO-8601 UTC time is honored. Relative times pass through."""
    if value.isdigit():
        t = datetime.datetime.fromtimestamp(int(value) / 1000, tz=datetime.timezone.utc)
        return t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"
    return value


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--grafana", default="http://127.0.0.1:13000")
    ap.add_argument("--uid", required=True)
    ap.add_argument("--from", dest="start", required=True)
    ap.add_argument("--to", dest="end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--vars", action="append", default=[], help="dashboard variable NAME=VALUE")
    ap.add_argument("--settle-s", type=float, default=8, help="wait for queries after load")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    dash = json.load(urllib.request.urlopen(f"{args.grafana}/api/dashboards/uid/{args.uid}"))["dashboard"]
    params = {"orgId": "1", "from": grafana_time(args.start), "to": grafana_time(args.end), "refresh": ""}
    for v in args.vars:
        k, _, val = v.partition("=")
        params[f"var-{k}"] = val
    query = urllib.parse.urlencode(params)
    panels = [p for p in dash["panels"] if p.get("type") != "row"]
    with sync_playwright() as p:
        browser = p.chromium.launch()
        # Grafana renders only panels in view, inside its own scroll area: a
        # viewport as tall as the whole grid (about 40 px per grid unit) shows
        # every panel at once.
        grid_units = max(p["gridPos"]["y"] + p["gridPos"]["h"] for p in dash["panels"])
        page = browser.new_page(viewport={"width": 1800, "height": grid_units * 40 + 200},
                                locale="en-US", timezone_id="UTC")
        page.goto(f"{args.grafana}/d/{args.uid}?{query}&kiosk", timeout=120_000)
        time.sleep(args.settle_s * 2)
        page.screenshot(path=str(out / f"{args.uid}.png"), full_page=True)
        for panel in panels:
            w = max(600, panel["gridPos"]["w"] * 75)
            h = max(300, panel["gridPos"]["h"] * 40)
            page.set_viewport_size({"width": w, "height": h})
            page.goto(f"{args.grafana}/d-solo/{args.uid}?{query}&panelId={panel['id']}", timeout=120_000)
            time.sleep(args.settle_s)
            page.screenshot(path=str(out / f"{args.uid}-{panel['id']:02d}-{slug(panel.get('title', ''))}.png"))
        browser.close()
    print(f"{len(panels) + 1} images in {out}")


if __name__ == "__main__":
    main()
