#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Press Release Collector → Filter → Report → Email (Strict Mode)

- Collects from RSS/newsroom feeds (with HTML fallback if RSS fails).
- Requires BOTH: (a) watchlist company AND (b) sector keyword → strict filtering.
- Aggressively excludes noise (lawsuits, law firms, tech buzzwords, festivals, etc.).
- Caps results to most recent 5 items for email.
- Outputs CSV, XLSX, JSON, PDF.
- Emails results via Yahoo SMTP (secrets: YAHOO_EMAIL, YAHOO_APP_PASSWORD, TO_EMAIL).

CLI:
  python press_release_tracker.py --lookback_hours 24 --send_always true
"""

import os, re, ssl, argparse, smtplib, requests, feedparser
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
import pandas as pd
from pandas import Timestamp
from requests.adapters import HTTPAdapter, Retry
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from bs4 import BeautifulSoup

# ---------------- Watchlist companies ----------------
WATCHLIST_COMPANIES = [
    # Class I rail
    "Union Pacific", "BNSF", "CSX", "Norfolk Southern",
    "Canadian National", "Canadian Pacific Kansas City", "CPKC",
    # TL / LTL carriers
    "J.B. Hunt", "Schneider", "Knight-Swift", "Swift", "Werner",
    "Heartland Express", "Old Dominion", "ODFL",
    "Saia", "XPO", "Estes", "R+L", "ABF Freight", "ArcBest", "TFI",
    # Brokers / IMCs / 3PL
    "C.H. Robinson", "CHRW", "RXO", "Echo Global Logistics", "Arrive Logistics",
    "NFI", "Hub Group", "Coyote", "Uber Freight", "IMC Companies",
]

# ---------------- Strict sector keywords ----------------
SECTOR_KEYWORDS = [
    "truckload", "less-than-truckload", "intermodal",
    "railroad", "rail", "container", "containers",
    "drayage", "chassis", "ramp", "interchange",
    "brokerage", "3pl", "imc", "intermodal marketing company"
]

# ---------------- Exclusion filters ----------------
EXCLUSION_PHRASES = [
    "class action", "shareholder alert", "lawsuit", "litigation",
    "investigation", "pomerantz", "rosen law", "glancy prongay",
    "awareness month", "festival", "concert", "fashion", "cosmetics",
    "AI", "blockchain", "platform", "cloud", "cyber", "software",
    "app launch", "webinar", "earnings call"
]

EXCLUSION_DOMAINS = {
    "api.taboola.com", "ad.doubleclick.net",
    "mail.yahoo.com", "news.mail.yahoo.com"
}

# ---------------- Feed sources ----------------
FEEDS = [
    "https://www.globenewswire.com/RssFeed/industry/Transportation.xml",
    "https://www.businesswire.com/portal/site/home/news/rss/industry/?vnsId=31367",
    "https://www.prnewswire.com/news/norfolk-southern-corporation/",
    "https://www.prnewswire.com/news/cpkc/",
    "https://www.globenewswire.com/search/organization/Hub%20Group%20Inc",
]

# ---------------- Helpers ----------------
def _norm(s: str) -> str: return (s or "").strip()
def _parse_dt(dt_str: str):
    try: return Timestamp(dt_str).to_pydatetime().astimezone(timezone.utc)
    except Exception: return None
def _domain_from_url(url: str) -> str:
    try: return re.sub(r"^https?://", "", url.split("/")[2].lower())
    except Exception: return ""

def _contains_any(text: str, needles: list[str]) -> bool:
    t = (text or "").lower()
    return any(n and n.lower() in t for n in needles)

# Regex
RE_WATCHLIST = re.compile("|".join([re.escape(c) for c in WATCHLIST_COMPANIES]), re.I)
RE_SECTOR    = re.compile("|".join([re.escape(k) for k in SECTOR_KEYWORDS]), re.I)

# Robust fetch
def fetch_feed_content(url: str, timeout: int = 20) -> bytes | None:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    try:
        r = s.get(url, timeout=timeout)
        if r.ok: return r.content
    except Exception as e:
        print(f"[WARN] fetch error {url}: {e}")
    return None

# Fallback scraper
def scrape_html(url: str) -> list[dict]:
    print(f"[INFO] Scraping HTML fallback for {url}")
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        items = []
        for a in soup.find_all("a", href=True):
            title = a.get_text(" ", strip=True)
            if not title: continue
            href = a["href"]
            if not href.startswith("http"):
                href = requests.compat.urljoin(url, href)
            items.append({
                "title": title,
                "url": href,
                "summary": "",
                "published_utc": datetime.now(timezone.utc).isoformat(),
                "source": _domain_from_url(href)
            })
        return items
    except Exception as e:
        print(f"[WARN] scrape error {url}: {e}")
        return []

# Collect
def collect(lookback_hours: int) -> pd.DataFrame:
    rows = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    for url in FEEDS:
        blob = fetch_feed_content(url)
        if blob:
            feed = feedparser.parse(blob)
            for e in feed.entries:
                title = _norm(getattr(e, "title", ""))
                link  = _norm(getattr(e, "link", ""))
                summ  = _norm(getattr(e, "summary", ""))
                published = None
                for k in ("published", "updated"):
                    if getattr(e, k, None):
                        published = _parse_dt(getattr(e, k)); break
                if published and published < cutoff: continue
                rows.append({
                    "title": title, "url": link, "summary": summ,
                    "published_utc": published.isoformat() if published else "",
                    "source": _domain_from_url(link)
                })
        else:
            rows.extend(scrape_html(url))
    return pd.DataFrame(rows)

# Filter
def apply_filters(df: pd.DataFrame, lookback_hours: int) -> pd.DataFrame:
    if df.empty: return df
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    df["_dt"] = df["published_utc"].apply(_parse_dt)
    df = df[df["_dt"].notna() & (df["_dt"] >= cutoff)]
    df["_domain"] = df["url"].map(_domain_from_url)
    df = df[~df["_domain"].isin(EXCLUSION_DOMAINS)]
    mask_excl = df.apply(lambda r: _contains_any(
        (r["title"] + " " + r["summary"]), EXCLUSION_PHRASES), axis=1)
    df = df[~mask_excl]
    # Require BOTH company and sector
    def match_row(r): return bool(RE_WATCHLIST.search(r["title"] + r["summary"])) and \
                               bool(RE_SECTOR.search(r["title"] + r["summary"]))
    df = df[df.apply(match_row, axis=1)]
    df = df.drop_duplicates("url").sort_values("_dt", ascending=False)
    return df.head(5)  # cap at 5

# PDF
def write_pdf(df, path):
    c = canvas.Canvas(path, pagesize=LETTER)
    w, h = LETTER; y = h - 50
    c.setFont("Helvetica-Bold", 14); c.drawString(50, y, "Press Release Brief"); y -= 30
    c.setFont("Helvetica", 9)
    if df.empty: c.drawString(50, y, "No qualifying releases."); c.save(); return
    for _, r in df.iterrows():
        c.drawString(50, y, f"• {r['title']}"); y -= 12
        c.drawString(60, y, r['url']); y -= 12
        c.drawString(60, y, r['_dt'].strftime("%Y-%m-%d %H:%M UTC")); y -= 20
    c.save()

# Email
def send_email(subject, html_body, attachments):
    sender = os.environ.get("YAHOO_EMAIL","").strip()
    app_pw = os.environ.get("YAHOO_APP_PASSWORD","").strip()
    to_raw = os.environ.get("TO_EMAIL","").strip()
    if not (sender and app_pw and to_raw):
        raise RuntimeError("Missing YAHOO_EMAIL, YAHOO_APP_PASSWORD, or TO_EMAIL.")
    to_list = [x.strip() for x in to_raw.split(",")]
    msg = EmailMessage()
    msg["From"] = sender; msg["To"] = ", ".join(to_list); msg["Subject"] = subject
    msg.set_content("HTML version required."); msg.add_alternative(html_body, subtype="html")
    for fname, data, mime in attachments:
        msg.add_attachment(data, maintype=mime.split("/")[0], subtype=mime.split("/")[1], filename=fname)
    with smtplib.SMTP_SSL("smtp.mail.yahoo.com", 465, context=ssl.create_default_context()) as s:
        s.login(sender, app_pw); s.send_message(msg)

# Main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback_hours", default=os.environ.get("LOOKBACK_HOURS","24"))
    ap.add_argument("--send_always", default=os.environ.get("SEND_ALWAYS","true"))
    args = ap.parse_args()
    lookback = int(args.lookback_hours)
    send_always = args.send_always.lower() == "true"

    df_raw = collect(lookback)
    print(f"[INFO] Raw rows: {len(df_raw)}")
    df = apply_filters(df_raw, lookback)
    print(f"[INFO] Filtered rows: {len(df)}")
    if not df.empty:
        print("[DEBUG] Sample results:")
        for _, r in df.iterrows():
            print(" -", r['title'], "|", r['url'])

    os.makedirs("reports", exist_ok=True)
    now_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    base = f"reports/press_releases_{now_tag}"
    out_csv, out_json, out_xlsx, out_pdf = f"{base}.csv", f"{base}.json", f"{base}.xlsx", f"{base}.pdf"
    df.to_csv(out_csv, index=False)
    df.to_json(out_json, orient="records", indent=2)
    df.drop(columns=["_dt"], errors="ignore").to_excel(out_xlsx, index=False)
    write_pdf(df, out_pdf)

    subject = f"[Press Releases] {len(df)} items"
    if len(df) or send_always:
        html_body = "<h3>Press Release Brief</h3><ul>" + "".join(
            f"<li><a href='{r['url']}'>{r['title']}</a></li>" for _, r in df.iterrows()) + "</ul>"
        attachments = [(os.path.basename(p), open(p,"rb").read(), m) for p,m in [
            (out_csv,"text/csv"), (out_json,"application/json"),
            (out_xlsx,"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            (out_pdf,"application/pdf")]]
        send_email(subject, html_body, attachments)
        print("[OK] Email sent successfully.")
    else:
        print("[OK] No results and SEND_ALWAYS=false, skipping email.")

if __name__ == "__main__":
    main()
