#!/usr/bin/env python3
import argparse, json, os, re, sys, time
from pathlib import Path
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright

PATTERN = re.compile(r"\.(m3u8|mpd)(\?.*)?$", re.IGNORECASE)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="Web page that plays the stream")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--wait", type=float, default=12.0, help="Extra seconds to idle on page")
    ap.add_argument("--timeout", type=int, default=45000, help="Page goto timeout ms")
    ap.add_argument("--debug", action="store_true", help="Dump request log")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    found = []           # list of {"url": "...", "type": "hls|dash"}
    request_log = []     # optional for debug

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        def maybe_record(req):
            url = req.url
            if args.debug:
                request_log.append({"method": req.method, "url": url})
            m = PATTERN.search(url)
            if m:
                kind = "hls" if m.group(1).lower() == "m3u8" else "dash"
                # Avoid duplicates (ignore querystring differences)
                key = urlsplit(url)._replace(query="").geturl()
                if not any(urlsplit(x["url"])._replace(query="").geturl() == key for x in found):
                    found.append({"url": url, "type": kind})

        page.on("request", maybe_record)

        page.goto(args.url, wait_until="domcontentloaded", timeout=args.timeout)
        # Some players lazy-load after user gesture; try a gentle click
        try:
            page.mouse.click(200, 200)
        except Exception:
            pass

        time.sleep(args.wait)

        if args.debug:
            (out / "network_requests.json").write_text(json.dumps(request_log, indent=2))

        if not found:
            print("No HLS or DASH manifest found", file=sys.stderr)
            # 2 = nothing matched (kept for your workflow logic)
            sys.exit(2)

        # Save a concise summary and the first URL for downstream steps
        (out / "manifests.json").write_text(json.dumps(found, indent=2))
        (out / "first_manifest_url.txt").write_text(found[0]["url"] + "\n")

        # Also write one file per manifest with a friendly name
        for i, item in enumerate(found, 1):
            suffix = ".m3u8" if item["type"] == "hls" else ".mpd"
            (out / f"manifest_{i:02d}_{item['type']}{suffix}.url").write_text(item["url"] + "\n")

        print(f"Captured {len(found)} manifest URL(s).")
        for item in found:
            print(f" - [{item['type'].upper()}] {item['url']}")

if __name__ == "__main__":
    main()