#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Press Release Tracker → Filter → Report → Email (Yahoo-only)

Secrets required (set in GitHub Actions > Settings > Secrets):
  - YAHOO_EMAIL          : Yahoo sender email
  - YAHOO_APP_PASSWORD   : Yahoo app password (16-char)
  - TO_EMAIL             : comma-separated recipients
  - YAHOO_CC             : optional comma-separated CCs

Outputs:
  - reports/press_releases_YYYYMMDD_HHMM.{csv,xlsx,json,pdf}
"""

import os, re, ssl, smtplib, argparse, requests, feedparser
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from requests.adapters import HTTPAdapter, Retry

import pandas as pd
from pandas import Timestamp
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from bs4 import BeautifulSoup

# ---------------- Feeds ----------------
DEFAULT_FEEDS = [
    "https://www.globenewswire.com/RssFeed/industry/Transportation.xml",
    "https://www.businesswire.com/portal/site/home/news/rss/industry/?vnsId=31367",
    "https://www.businesswire.com/portal/site/home/news/rss/industry/?vnsId=1050097",
    "https://www.businesswire.com/portal/site/home/news/rss/industry/?vnsId=1000155",
    "https://www.businesswire.com/portal/site/home/news/rss/industry/?vnsId=1000188",
    "https://www.prnewswire.com/news/norfolk-southern-corporation/",
    "https://www.prnewswire.com/news/cpkc/",
    "https://www.globenewswire.com/search/organization/Hub%20Group%20Inc",
]

# ---------------- Helpers ----------------
def _norm(s: str) -> str: return (s or "").strip()

def _domain_from_url(url: str) -> str:
    try: return re.sub(r"^https?://", "", url.split("/")[2].lower())
    except Exception: return ""

def _parse_dt(dt_str: str):
    if not dt_str: return None
    try: return Timestamp(dt_str).to_pydatetime().astimezone(timezone.utc)
    except Exception: return None

# ---------------- Fetch ----------------
def fetch_feed_content(url: str, timeout: int = 45) -> bytes | None:
    """Fetch RSS/HTML with retries and longer timeout for BusinessWire etc."""
    s = requests.Session()
    retries = Retry(
        total=6,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; PressReleaseBot/1.0)",
        "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
    }
    try:
        r = s.get(url, headers=headers, timeout=timeout)
        if r.ok and r.content:
            return r.content
    except requests.RequestException as e:
        print(f"[WARN] HTTP error for {url}: {e}")
    return None

# ---------------- Collect (RSS + HTML fallback) ----------------
def collect_from_feeds(feed_urls: list[str], lookback_hours: int) -> pd.DataFrame:
    rows, cutoff = [], datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

    for url in feed_urls:
        try:
            blob = fetch_feed_content(url)
            if not blob:
                print(f"[WARN] Failed feed {url}: no content")
                continue

            text = blob.decode("utf-8", errors="ignore")
            is_rss = text.strip().startswith("<?xml") or "<rss" in text.lower()

            if is_rss:
                # ✅ Normal RSS feed
                feed = feedparser.parse(text)
                for e in feed.entries:
                    title = _norm(getattr(e, "title", ""))
                    link  = _norm(getattr(e, "link", "")) or url
                    summ  = _norm(getattr(e, "summary", "") or getattr(e, "description", ""))

                    published = None
                    for key in ("published", "updated", "created"):
                        val = getattr(e, key, None)
                        if val:
                            published = _parse_dt(val)
                            break
                    if published and published < cutoff:
                        continue

                    rows.append({
                        "source": _domain_from_url(link) or _domain_from_url(url),
                        "title": title,
                        "url": link,
                        "published_utc": published.isoformat() if published else "",
                        "summary": summ,
                    })
            else:
                # ✅ HTML fallback
                print(f"[INFO] Scraping HTML fallback for {url}")
                soup = BeautifulSoup(text, "html.parser")

                for tag in soup.select("article, div.news-release, div.card, li, a"):
                    title = _norm(tag.get_text(" ", strip=True))
                    if not title or len(title) < 10:
                        continue

                    link = ""
                    a = tag.find("a", href=True)
                    if a:
                        link = a["href"]
                        if link.startswith("/"):
                            link = f"https://{_domain_from_url(url)}{link}"

                    summ = ""
                    p = tag.find("p")
                    if p:
                        summ = _norm(p.get_text(" ", strip=True))

                    published = datetime.now(timezone.utc)  # fallback timestamp
                    if published < cutoff:
                        continue

                    rows.append({
                        "source": _domain_from_url(url),
                        "title": title,
                        "url": link or url,
                        "published_utc": published.isoformat(),
                        "summary": summ,
                    })

        except Exception as ex:
            print(f"[WARN] Feed parse error {url}: {ex}")

    return pd.DataFrame(rows)

# ---------------- PDF ----------------
def write_pdf(df: pd.DataFrame, path: str, title: str):
    c = canvas.Canvas(path, pagesize=LETTER)
    width, height = LETTER; margin = 0.75 * inch; y = height - margin
    c.setFont("Helvetica-Bold", 14); c.drawString(margin, y, title); y -= 0.3 * inch
    c.setFont("Helvetica", 9)
    if df.empty:
        c.drawString(margin, y, "No qualifying press releases in the selected window.")
        c.save(); return

    def draw_line(text: str):
        nonlocal y
        while text:
            if len(text) <= 110:
                line, text = text, ""
            else:
                cut = text.rfind(" ", 0, 110); cut = 110 if cut == -1 else cut
                line, text = text[:cut], text[cut:].lstrip()
            if y < 1.0 * inch:
                c.showPage(); y = height - margin; c.setFont("Helvetica", 9)
            c.drawString(margin, y, line); y -= 12

    for _, r in df.iterrows():
        t = _norm(r.get("title") or ""); u = _norm(r.get("url") or ""); s = _norm(r.get("summary") or "")
        ts = r.get("published_utc", "")
        draw_line(f"• {t}")
        if u: draw_line(f"  {u}")
        if ts: draw_line(f"  {ts}")
        if s: draw_line(f"  {s}")
        y -= 6
    c.save()

# ---------------- Email ----------------
def send_email(subject: str, html_body: str, attachments: list[tuple[str, bytes, str]]):
    sender = os.environ.get("YAHOO_EMAIL", "").strip()
    app_pw = os.environ.get("YAHOO_APP_PASSWORD", "").strip()
    to_raw = os.environ.get("TO_EMAIL", "").strip()
    cc_raw = os.environ.get("YAHOO_CC", "").strip()

    if not (sender and app_pw and to_raw):
        raise RuntimeError("Missing YAHOO_EMAIL, YAHOO_APP_PASSWORD, or TO_EMAIL.")

    to_list = [x.strip() for x in to_raw.split(",") if x.strip()]
    cc_list = [x.strip() for x in cc_raw.split(",") if x.strip()] if cc_raw else []

    # Mask for logging
    def mask(addr: str) -> str:
        if "@" in addr:
            user, domain = addr.split("@", 1)
            return user[0] + "***@" + domain
        return addr

    print(f"[INFO] Preparing email:")
    print(f"  From: {mask(sender)}")
    print(f"  To:   {[mask(a) for a in to_list]}")
    if cc_list:
        print(f"  Cc:   {[mask(a) for a in cc_list]}")

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    msg["Subject"] = subject
    msg.set_content("HTML version required to view this report.")
    msg.add_alternative(html_body, subtype="html")

    for fname, data, mime in attachments:
        maintype, subtype = mime.split("/", 1)
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=fname)

    try:
        with smtplib.SMTP_SSL("smtp.mail.yahoo.com", 465, context=ssl.create_default_context()) as s:
            s.login(sender, app_pw)
            s.send_message(msg)
        print("[OK] Email sent successfully.")
    except smtplib.SMTPAuthenticationError:
        print("[ERROR] SMTP authentication failed — check YAHOO_EMAIL and YAHOO_APP_PASSWORD secrets.")
        raise
    except smtplib.SMTPException as e:
        print(f"[ERROR] SMTP error: {e}")
        raise

# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback_hours", default=os.environ.get("LOOKBACK_HOURS","24"))
    ap.add_argument("--send_always", default=os.environ.get("SEND_ALWAYS","true"))
    args = ap.parse_args()

    lookback_hours = int(str(args.lookback_hours))
    send_always = str(args.send_always).lower().strip() == "true"

    feed_urls = DEFAULT_FEEDS
    df_raw = collect_from_feeds(feed_urls, lookback_hours)

    print(f"[INFO] Raw rows: {len(df_raw)}")
    df = df_raw
    print(f"[INFO] Filtered rows: {len(df)}")

    os.makedirs("reports", exist_ok=True)
    now_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    base = f"reports/press_releases_{now_tag}"
    out_csv, out_xlsx, out_json, out_pdf = f"{base}.csv", f"{base}.xlsx", f"{base}.json", f"{base}.pdf"

    df.to_csv(out_csv, index=False)
    df.to_json(out_json, orient="records", indent=2, date_format="iso")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xl:
        df.to_excel(xl, index=False, sheet_name="Press Releases")
    write_pdf(df, out_pdf, title="Press Release Brief")

    total = len(df); subject = f"[Press Releases] {total} items in last {lookback_hours}h"
    if total or send_always:
        rows_html = ""
        if total:
            for _, r in df.head(100).iterrows():
                t, u, s = _norm(r.get("title","")), _norm(r.get("url","")), _norm(r.get("summary",""))
                ts = r.get("published_utc"); ts_str = ts if ts else ""
                dom = r.get("source","")
                rows_html += f"""<tr><td style="padding:6px;border-bottom:1px solid #ddd;">
                    <a href="{u}">{t}</a><br><span style="color:#666;">{dom} | {ts_str}</span><br><span>{s}</span></td></tr>"""
        else:
            rows_html = f'<tr><td style="padding:12px;">No qualifying items in the last {lookback_hours} hours.</td></tr>'

        html_body = f"""<html><body>
          <h3>Press Release Brief (lookback: {lookback_hours}h)</h3>
          <table style="width:100%;border-collapse:collapse;font-family:Arial,sans-serif;font-size:14px;">
            {rows_html}
          </table>
          <p style="color:#666;font-size:12px;">Auto-generated at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}</p>
        </body></html>"""

        attachments = []
        for path, mime in [(out_csv,"text/csv"),
                           (out_xlsx,"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                           (out_json,"application/json"),
                           (out_pdf,"application/pdf")]:
            with open(path, "rb") as f:
                attachments.append((os.path.basename(path), f.read(), mime))

        send_email(subject, html_body, attachments)
    else:
        print("[OK] No items and SEND_ALWAYS=false — no email sent.")

if __name__ == "__main__":
    main()
