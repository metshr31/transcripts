#!/usr/bin/env python3
# capture_manifest.py
#
# Usage example:
#   python scripts/capture_manifest.py \
#     --url "https://event.webcasts.com/starthere.jsp?ei=1732689&tp_key=88708506b9&tp_special=8" \
#     --out "out" \
#     --wait-ms 60000 \
#     --auto-play
#
# Notes:
# - Requires Playwright Python:  pip install playwright
# - One-time:  playwright install --with-deps
#
# Exit codes:
#   0 = success
#   2 = no manifest detected
#   3 = other error

import argparse
import json
import re
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

M3U8_RE = re.compile(r"\.m3u8(\?|$)", re.IGNORECASE)
MPD_RE  = re.compile(r"\.mpd(\?|$)", re.IGNORECASE)

COMMON_PLAY_SELECTORS = [
    "button[aria-label='Play']",
    ".vjs-big-play-button",
    "button[title='Play']",
    "button.play",
    "[data-testid='play-button']",
    "video",  # as last resort, try a click on the <video>
]

def parse_args():
    p = argparse.ArgumentParser(description="Capture HLS/DASH manifest URL via Playwright.")
    p.add_argument("--url", dest="url", required=True, help="Page URL with the embedded player.")
    p.add_argument("--out", dest="out_dir", default="out", help="Output directory (default: out)")
    p.add_argument("--wait-ms", dest="wait_ms", type=int, default=45000,
                   help="Max time to wait for manifest (ms). Default 45000.")
    p.add_argument("--auto-play", action="store_true",
                   help="Attempt to click a Play button automatically.")
    p.add_argument("--play-selector", action="append", default=[],
                   help="Additional CSS selector(s) to click to start playback. Can be repeated.")
    p.add_argument("--eval-js", default=None,
                   help="Optional JS to eval after load (e.g., to unmute or trigger play).")
    p.add_argument("--headless", action="store_true", help="Run browser headless.")
    p.add_argument("--device-scale", type=float, default=1.0, help="Device scale factor.")
    p.add_argument("--viewport-w", type=int, default=1280)
    p.add_argument("--viewport-h", type=int, default=720)
    return p.parse_args()

def save_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def try_click_play(page, extra_selectors):
    tried = []
    for sel in extra_selectors + COMMON_PLAY_SELECTORS:
        try:
            el = page.query_selector(sel)
            if el:
                el.click(force=True, timeout=1000)
                tried.append(sel)
                # give the page a moment to react
                time.sleep(0.5)
        except Exception:
            pass
    return tried

def main():
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    captured_urls = []
    manifest_url = None
    manifest_kind = None  # "hls" or "dash"

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=args.headless)
        context = browser.new_context(
            viewport={"width": args.viewport_w, "height": args.viewport_h},
            device_scale_factor=args.device_scale,
            ignore_https_errors=True,
        )
        page = context.new_page()

        # Listen on ALL responses (main page + iframes) and collect candidate URLs
        def on_response(resp):
            try:
                url = resp.url
                if M3U8_RE.search(url):
                    captured_urls.append(("hls", url))
                elif MPD_RE.search(url):
                    captured_urls.append(("dash", url))
            except Exception:
                pass

        page.on("response", on_response)

        # Go to the page and wait for network to settle a bit
        page.goto(args.url, wait_until="domcontentloaded", timeout=60000)

        # Optional eval
        if args.eval_js:
            try:
                page.evaluate(args.eval_js)
                time.sleep(0.2)
            except Exception:
                # Non-fatal
                pass

        # Optional auto-play: try common selectors + any provided
        tried_selectors = []
        if args.auto_play:
            try:
                tried_selectors = try_click_play(page, args.play_selector)
            except Exception:
                pass

        # As an extra nudge, try to press Space (many players map this to play/pause)
        if args.auto_play:
            try:
                page.keyboard.press("Space")
            except Exception:
                pass

        # Actively wait up to wait_ms for the first manifest URL to appear
        deadline = time.time() + (args.wait_ms / 1000.0)
        while time.time() < deadline and manifest_url is None:
            # Check anything we have captured so far
            for kind, url in captured_urls:
                if M3U8_RE.search(url):
                    manifest_url, manifest_kind = url, "hls"
                    break
            if manifest_url is None:
                for kind, url in captured_urls:
                    if MPD_RE.search(url):
                        manifest_url, manifest_kind = url, "dash"
                        break
            if manifest_url:
                break
            time.sleep(0.25)

        # Prepare session info for downstream steps
        session = {
            "page_url": args.url,
            "manifest_url": manifest_url,
            "manifest_type": manifest_kind,
            "clicked_selectors": tried_selectors,
            "saw_candidates_count": len(captured_urls),
            "candidates_sample": [u for _, u in captured_urls[:10]],
            "timestamp": int(time.time()),
        }

        save_json(out_dir / "session_info.json", session)

        if manifest_url:
            # Optionally, save a copy of the manifest file itself
            # (some players require headers; this simple GET may not always work —
            # but saving the URL itself is usually sufficient for downstream tooling)
            print(manifest_url)  # keep stdout clean for GitHub Actions steps
            browser.close()
            sys.exit(0)

        # No manifest found
        print("ERROR: No HLS .m3u8 or DASH .mpd manifest detected.", file=sys.stderr)
        print(json.dumps(session, indent=2), file=sys.stderr)
        browser.close()
        sys.exit(2)


if __name__ == "__main__":
    try:
        main()
    except PWTimeoutError as e:
        print(f"ERROR: Playwright timeout — {e}", file=sys.stderr)
        sys.exit(3)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(3)