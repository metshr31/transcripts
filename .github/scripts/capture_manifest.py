# capture_manifest.py
# Purpose: Open PAGE_URL in Playwright, listen for network activity, and capture an HLS .m3u8 manifest URL.
# Outputs:
#   - <OUT_DIR>/session_info.json  -> {"manifest_url": "..."}  (only if found)
#   - <OUT_DIR>/requests.log       -> all request URLs seen
#   - GitHub Actions step output "manifest_url" if GITHUB_OUTPUT is set
#
# Env vars:
#   PAGE_URL   (required)
#   OUT_DIR    (default: "out")
#   HEADLESS   ("true"/"false", default: "true")
#   WAIT_MS    (ms to keep the page open for capturing, default: 8000)
#   TIMEOUT_MS (overall timeout for the run, default: 30000)

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Optional, List

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError

M3U8_HINTS = (".m3u8",)  # extend if needed
M3U8_CT_HINTS = ("application/vnd.apple.mpegurl", "application/x-mpegURL")

def env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "y")

def env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return int(val)
    except Exception:
        return default

async def capture_manifest() -> int:
    page_url = os.getenv("PAGE_URL")
    if not page_url:
        print("ERROR: PAGE_URL env var is required.", file=sys.stderr)
        return 1

    out_dir = Path(os.getenv("OUT_DIR", "out"))
    out_dir.mkdir(parents=True, exist_ok=True)

    headless = env_bool("HEADLESS", True)
    wait_ms = env_int("WAIT_MS", 8000)
    timeout_ms = env_int("TIMEOUT_MS", 30000)

    # In-memory stores
    all_requests: List[str] = []
    found_manifest_url: Optional[str] = None

    def maybe_record_request(url: str):
        # Always log every request URL
        all_requests.append(url)

    def looks_like_m3u8(url: str) -> bool:
        u = url.lower()
        return any(h in u for h in M3U8_HINTS)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        # --- Event listeners -------------------------------------------------
        # 1) Raw request URLs as they are issued
        page.on("request", lambda req: maybe_record_request(req.url))

        # 2) When a request finishes, check the response headers/content-type
        async def on_request_finished(req):
            nonlocal found_manifest_url
            if found_manifest_url:
                return
            try:
                resp = await req.response()
                if not resp:
                    return
                ct = (resp.headers or {}).get("content-type", "")
                url = req.url
                # If extension or content-type suggests HLS, capture it
                if looks_like_m3u8(url) or any(h in ct for h in M3U8_CT_HINTS):
                    found_manifest_url = url
            except Exception:
                # Ignore per-request errors; keep listening
                pass

        page.on("requestfinished", lambda req: asyncio.create_task(on_request_finished(req)))

        # Some HLS players fetch the master manifest via XHR/fetch after user interaction.
        # Try to trigger play by pressing keys and clicking common selectors, but don’t fail if not present.
        async def gentle_play_attempts():
            selectors = [
                "button[aria-label*='play' i]",
                "button:has-text('Play')",
                "button[title*='play' i]",
                ".vjs-big-play-button",
            ]
            for sel in selectors:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.click(timeout=1000)
                except Exception:
                    pass
            # Space key sometimes toggles play
            try:
                await page.keyboard.press("Space")
            except Exception:
                pass

        try:
            # Navigate and wait for initial network to settle a bit
            await page.goto(page_url, wait_until="domcontentloaded", timeout=timeout_ms)
        except PWTimeoutError:
            print("WARNING: Initial navigation hit timeout; continuing to listen for requests...", file=sys.stderr)

        # Try to coax the player to start
        await gentle_play_attempts()

        # Keep the page alive to collect requests
        await page.wait_for_timeout(wait_ms)

        # If still nothing, try a small scroll/interaction + extra wait
        if not found_manifest_url:
            try:
                await page.mouse.wheel(0, 800)
            except Exception:
                pass
            await page.wait_for_timeout(2000)

        # Write logs regardless
        try:
            (out_dir / "requests.log").write_text("\n".join(all_requests), encoding="utf-8")
        except Exception as e:
            print(f"WARNING: could not write requests.log: {e}", file=sys.stderr)

        # If we found a manifest, persist and expose it
        if found_manifest_url:
            session = {"manifest_url": found_manifest_url}
            try:
                (out_dir / "session_info.json").write_text(json.dumps(session, indent=2), encoding="utf-8")
                print(f"Captured manifest URL: {found_manifest_url}")
            except Exception as e:
                print(f"ERROR: failed to write session_info.json: {e}", file=sys.stderr)
                await browser.close()
                return 1

            # GitHub Actions step output
            gha_out = os.getenv("GITHUB_OUTPUT")
            if gha_out:
                try:
                    with open(gha_out, "a", encoding="utf-8") as f:
                        print(f"manifest_url={found_manifest_url}", file=f)
                except Exception as e:
                    print(f"WARNING: failed to write GITHUB_OUTPUT: {e}", file=sys.stderr)

            await browser.close()
            return 0
        else:
            print("ERROR: No HLS .m3u8 manifest detected. Check auth gates, playback triggers, or increase WAIT_MS.", file=sys.stderr)
            await browser.close()
            return 1

if __name__ == "__main__":
    # Run the async entry and return a proper exit code
    code = asyncio.run(capture_manifest())
    sys.exit(code)