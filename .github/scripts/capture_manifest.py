#!/usr/bin/env python3
# capture_manifest.py
# Usage:
#   python .github/scripts/capture_manifest.py --url "https://example.com" --out out
#
# What it does:
# - Opens the URL with Playwright (headless)
# - Listens for network requests/responses that look like HLS manifests (.m3u8)
# - Writes out/session_info.json with {"manifest_url": "...", "page_url": "...", "found_by": "..."}
# - Also writes out/manifests.txt with all candidates it saw

import argparse
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

HLS_EXTENSIONS = (".m3u8",)
HLS_CONTENT_TYPES = {
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "audio/mpegurl",
}

def looks_like_hls_url(url: str) -> bool:
    u = url.lower()
    if any(ext in u for ext in HLS_EXTENSIONS):
        return True
    # Some players serve manifests behind query params without .m3u8 in path
    # Try a loose match for "m3u8" anywhere
    if "m3u8" in u:
        return True
    return False

def choose_best_candidate(candidates):
    """Pick a likely master manifest first (contains 'master' or 'index'), else last seen."""
    if not candidates:
        return None
    # Prefer master-ish
    for url in candidates:
        u = url.lower()
        if "master" in u or "index" in u:
            return url
    # otherwise return the last one we saw
    return candidates[-1]

def main():
    parser = argparse.ArgumentParser(description="Capture HLS manifest URL from a webpage.")
    parser.add_argument("--url", dest="page_url", required=True, help="Page URL to open")
    parser.add_argument("--out", dest="out_dir", default="out", help="Output directory")
    parser.add_argument("--wait", dest="wait_seconds", type=int, default=25,
                        help="Seconds to wait while the page loads and streams start")
    parser.add_argument("--click-selector", dest="click_selector", default=None,
                        help="Optional CSS selector to click Play/Start if needed")
    parser.add_argument("--user-agent", dest="user_agent", default=None,
                        help="Optional custom user agent")
    args = parser.parse_args()

    out_path = Path(args.out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    candidates = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context_kwargs = {}
        if args.user_agent:
            context_kwargs["user_agent"] = args.user_agent
        context = browser.new_context(**context_kwargs)
        page = context.new_page()

        # Collect from requests
        def on_request(request):
            url = request.url
            if looks_like_hls_url(url):
                candidates.append(url)

        # Collect from responses by content-type
        def on_response(response):
            try:
                ct = response.headers.get("content-type", "")
            except Exception:
                ct = ""
            if any(mime in ct.lower() for mime in HLS_CONTENT_TYPES):
                candidates.append(response.url)

        page.on("request", on_request)
        page.on("response", on_response)

        try:
            page.goto(args.page_url, wait_until="load", timeout=45000)
        except PWTimeoutError:
            # Still proceed; some pages keep loading assets for a long time
            pass

        # If a click is needed to start playback
        if args.click_selector:
            try:
                page.wait_for_selector(args.click_selector, timeout=8000)
                page.click(args.click_selector)
            except PWTimeoutError:
                # Not fatal; continue to wait for network traffic
                pass
            except Exception:
                pass

        # Also probe for <video><source src="...m3u8"></video>
        try:
            srcs = page.eval_on_selector_all(
                "video source, video",
                "els => els.map(e => e.src || e.currentSrc || e.getAttribute('src') || '')"
            )
            for s in srcs:
                if s and looks_like_hls_url(s):
                    candidates.append(s)
        except Exception:
            pass

        # Let the page run and network settle
        t0 = time.time()
        while time.time() - t0 < args.wait_seconds:
            time.sleep(0.5)

        context.close()
        browser.close()

    # Deduplicate but preserve order
    seen = set()
    uniq = []
    for u in candidates:
        if u and u not in seen:
            seen.add(u)
            uniq.append(u)

    # Persist all candidates for debugging
    (out_path / "manifests.txt").write_text("\n".join(uniq), encoding="utf-8")

    best = choose_best_candidate(uniq)
    session_info = {
        "page_url": args.page_url,
        "manifest_url": best or "",
        "found_by": "request/response sniffing" if best else "",
        "all_candidates_count": len(uniq),
    }
    (out_path / "session_info.json").write_text(json.dumps(session_info, indent=2), encoding="utf-8")

    if best:
        print(best)
        sys.exit(0)
    else:
        print("No HLS manifest found", file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()