#!/usr/bin/env python3
"""Keep the live dashboard open in a headless browser (so Grafana issues
its queries on every auto-refresh, as a viewer's browser would) and take
full-page screenshots.

    browse.py --url http://127.0.0.1:13000/d/asap-vs-vm-live --minutes 5 --out demo/live/screenshots

Runs inside the pinned Playwright image (run_live_demo.sh browse)."""
import argparse
import time
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", required=True)
    ap.add_argument("--minutes", type=float, default=5)
    ap.add_argument("--every-s", type=float, default=60, help="seconds between screenshots")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        # Grafana needs a locale (it fails to start without one) and the
        # dashboard is in UTC.
        page = browser.new_page(viewport={"width": 1800, "height": 7000}, locale="en-US", timezone_id="UTC")
        page.goto(args.url + ("&" if "?" in args.url else "?") + "kiosk", timeout=120_000)
        end = time.time() + args.minutes * 60
        n = 0
        while True:
            time.sleep(min(args.every_s, max(0, end - time.time())) if n else 20)
            # The viewport is as tall as the dashboard, so every panel is in
            # view and queries on each refresh, as on a tall screen.
            page.screenshot(path=str(out / f"live-{time.strftime('%H%M%S', time.gmtime())}.png"), full_page=True)
            n += 1
            if time.time() >= end:
                break
        browser.close()


if __name__ == "__main__":
    main()
