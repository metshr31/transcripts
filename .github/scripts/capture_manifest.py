# scripts/capture_manifest.py
import json, os, re, sys, asyncio
from pathlib import Path
from playwright.async_api import async_playwright

OUT = Path(os.environ.get("OUT_DIR", "out"))
URL = os.environ["PAGE_URL"]  # pass in the webcast page URL
OUT.mkdir(parents=True, exist_ok=True)

M3U8_RE = re.compile(r"\.m3u8(\?.*)?$", re.I)

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        found_url = {"url": None}

        def maybe_capture(req):
            u = req.url
            if M3U8_RE.search(u):
                found_url["url"] = u

        page.on("request", maybe_capture)
        await page.goto(URL, wait_until="domcontentloaded")

        # Try to click common "Play" buttons to start the HLS fetch
        for sel in ["button[aria-label='Play']", "button.play", ".vjs-big-play-button", "button[title='Play']"]:
            try:
                if await page.locator(sel).first.is_visible():
                    await page.locator(sel).first.click()
                    break
            except:
                pass

        # Wait up to 30s for a .m3u8 request to appear
        try:
            await page.wait_for_event("request", timeout=30000, predicate=lambda r: M3U8_RE.search(r.url) is not None)
        except:
            pass

        # One more sweep through existing requests just in case
        for r in page.context.requests:
            if M3U8_RE.search(r.url):
                found_url["url"] = r.url
                break

        if not found_url["url"]:
            print("❌ No .m3u8 URL captured", file=sys.stderr)
            await browser.close()
            sys.exit(10)

        # Save manifest URL
        (OUT / "manifest.m3u8").write_text(found_url["url"], encoding="utf-8")

        # Save cookies
        storage = await context.storage_state()
        cookies = [{"name": c["name"], "value": c["value"]} for c in storage.get("cookies", [])]
        (OUT / "cookies.json").write_text(json.dumps(cookies, indent=2))

        # Minimal headers often required by CDN (Referer + UA)
        headers = [
            {"name": "Referer", "value": URL},
            {"name": "User-Agent", "value": await page.evaluate("() => navigator.userAgent")},
        ]
        (OUT / "headers.json").write_text(json.dumps(headers, indent=2))

        await browser.close()
        print("✅ Captured:", found_url["url"])

if __name__ == "__main__":
    asyncio.run(main())