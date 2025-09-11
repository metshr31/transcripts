#!/usr/bin/env python3
import argparse, asyncio, json, os, re, sys, time
from pathlib import Path
from typing import List, Dict, Optional

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

M3U8_PAT = re.compile(r"\.m3u8(\?|$)", re.IGNORECASE)
M3U8_CT  = ("application/vnd.apple.mpegurl", "application/x-mpegURL", "audio/mpegurl")

COMMON_PLAY_SELECTORS = [
    "button[aria-label='Play']",
    "button[title='Play']",
    "button:has-text('Play')",
    "button.play",
    ".vjs-play-control",
    ".jw-controlbar .jw-icon-playback",
    ".ytp-play-button",
    "[data-control='play']",
    "video",                # fallback: focus video and press Space/Enter
]

IFRAME_QUERY = "iframe, frame"

def now_ms() -> int:
    return int(time.time() * 1000)

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def write_json(path: Path, data: Dict) -> None:
    path.write_text(json.dumps(data, indent=2))

def looks_like_m3u8(url: str, content_type: Optional[str]) -> bool:
    if M3U8_PAT.search(url):
        return True
    if content_type:
        ct = content_type.split(";")[0].strip()
        if ct in M3U8_CT:
            return True
    return False

async def click_play_in_frame(frame, extra_wait_ms: int, debug: bool) -> bool:
    # Try a bunch of likely selectors; on success, wait a bit for network.
    for sel in COMMON_PLAY_SELECTORS:
        try:
            el = await frame.query_selector(sel)
            if el:
                if debug: print(f"[debug] Clicking '{sel}'")
                try:
                    await el.click(timeout=2000)
                except Exception:
                    # Some players need a user-gesture on the video element
                    try: await el.focus()
                    except Exception: pass
                    try: await frame.keyboard.press("Space")
                    except Exception: pass
                await frame.wait_for_timeout(extra_wait_ms)
                return True
        except Exception:
            continue
    # Final resort: click center of the frame
    try:
        if debug: print("[debug] Clicking center of frame as fallback")
        box = await frame.evaluate("() => ({w: window.innerWidth, h: window.innerHeight})")
        await frame.mouse.click(box["w"]/2, box["h"]/2)
        await frame.wait_for_timeout(extra_wait_ms)
        return True
    except Exception:
        return False

async def collect_manifests(page, wait_ms: int, click_after_ms: int, debug: bool) -> List[Dict]:
    found: List[Dict] = []

    def maybe_add(kind: str, url: str, ct: Optional[str]):
        if not url:
            return
        if looks_like_m3u8(url, ct):
            # Dedup by URL
            if not any(x["url"] == url for x in found):
                if debug: print(f"[debug] Found {kind} m3u8: {url} (ct={ct})")
                found.append({"url": url, "content_type": ct or "", "source": kind})

    # Hook requests + responses
    page.on("request", lambda req: maybe_add("request", req.url, None))
    page.on("response", lambda resp: maybe_add("response", resp.url, resp.headers.get("content-type", "")))

    # Wait for initial network after load
    start = now_ms()
    await page.wait_for_load_state("domcontentloaded")
    try:
        await page.wait_for_load_state("networkidle")
    except PWTimeout:
        pass

    # Allow some idle time for auto-starting players
    await page.wait_for_timeout(min(wait_ms, 8000))

    # If nothing yet, try to click Play on page & iframes
    if not found:
        if debug: print("[debug] No m3u8 yet; attempting to trigger playback...")
        # Try top-level first
        triggered = await click_play_in_frame(page, click_after_ms, debug)

        # Then scan frames
        for frame in page.frames:
            try:
                if frame is page.main_frame: 
                    continue
                if debug: print(f"[debug] Attempting click inside iframe: {frame.url}")
                trig2 = await click_play_in_frame(frame, click_after_ms, debug)
                triggered = triggered or trig2
            except Exception:
                continue

        # Give network more time post-clicks
        await page.wait_for_timeout(max(1500, click_after_ms))

    # Final grace period to catch late requests
    remaining = max(0, wait_ms - (now_ms() - start))
    if remaining:
        if debug: print(f"[debug] Final wait {remaining}ms")
        await page.wait_for_timeout(remaining)

    return found

async def run(opts):
    out_dir = Path(opts.out).resolve()
    ensure_dir(out_dir)

    storage_state_path = out_dir / "storage_state.json"
    session_info_path  = out_dir / "session_info.json"
    har_path           = out_dir / "session.har" if opts.har else None

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=not opts.headed, args=[
            "--autoplay-policy=no-user-gesture-required",
        ])
        context_kwargs = {
            "ignore_https_errors": True,
            "user_agent": opts.user_agent or None,
            "viewport": {"width": 1400, "height": 850},
            "java_script_enabled": True,
            "accept_downloads": False,
        }
        if har_path:
            context_kwargs["record_har_path"] = str(har_path)
            context_kwargs["record_har_omit_content"] = True

        context = await browser.new_context(**context_kwargs)
        page = await context.new_page()

        # Navigate
        if opts.debug: print(f"[debug] Navigating to {opts.url}")
        try:
            await page.goto(opts.url, wait_until="domcontentloaded", timeout=opts.timeout)
        except PWTimeout:
            print("ERROR: Page load timed out.", file=sys.stderr)
            await browser.close()
            sys.exit(1)

        # Optional: accept consent banners that block playback
        try:
            # Common CMP accept buttons (best-effort)
            for sel in ["button:has-text('Accept')", "button:has-text('I Agree')", "#onetrust-accept-btn-handler"]:
                el = await page.query_selector(sel)
                if el:
                    if opts.debug: print(f"[debug] Clicking consent '{sel}'")
                    await el.click(timeout=1000)
                    await page.wait_for_timeout(500)
        except Exception:
            pass

        manifests = await collect_manifests(
            page,
            wait_ms=opts.wait,
            click_after_ms=opts.after_click_wait,
            debug=opts.debug,
        )

        await context.storage_state(path=str(storage_state_path))
        await browser.close()

    if not manifests:
        print("ERROR: No HLS .m3u8 manifest detected. Check auth gates, playback triggers, or increase WAIT_MS.", file=sys.stderr)
        sys.exit(1)

    # Prefer the first found; also emit all candidates.
    primary = manifests[0]["url"]
    out = {
        "page_url": opts.url,
        "manifest_url": primary,
        "all_manifests": manifests,
        "har_path": str(har_path) if har_path else "",
        "storage_state": str(storage_state_path),
        "ts": int(time.time()),
    }
    write_json(session_info_path, out)
    print(f"Captured manifest: {primary}")
    print(f"Wrote: {session_info_path}")

def parse_args():
    ap = argparse.ArgumentParser(description="Capture HLS .m3u8 manifest URL with Playwright")
    ap.add_argument("--url", required=True, help="Web page URL with embedded player")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--wait", type=int, default=15000, help="Total max wait (ms) for manifests (default 15000)")
    ap.add_argument("--after-click-wait", type=int, default=4000, help="Extra wait (ms) after triggering Play (default 4000)")
    ap.add_argument("--timeout", type=int, default=45000, help="Navigation timeout (ms)")
    ap.add_argument("--headed", action="store_true", help="Run headed (for local debugging)")
    ap.add_argument("--debug", action="store_true", help="Verbose debug logs")
    ap.add_argument("--har", action="store_true", help="Record a HAR file to inspect network later")
    ap.add_argument("--user-agent", default="", help="Override User-Agent if needed")
    return ap.parse_args()

if __name__ == "__main__":
    opts = parse_args()
    asyncio.run(run(opts))