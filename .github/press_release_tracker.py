#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Press Release Collector → Filter → Report → Email
------------------------------------------------
- Collects from RSS feeds (preferred).
- Falls back to HTML scraping (Businesswire, Globenewswire, PRNewswire).
- Filters for trucking/LTL/intermodal/rail/brokers.
- Saves CSV, XLSX, JSON, PDF into /reports.
- Emails results via Yahoo SMTP.

Secrets required:
  YAHOO_EMAIL
  YAHOO_APP_PASSWORD
  TO_EMAIL
"""

import os, re, ssl, smtplib, argparse, requests, feedparser
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
from pandas import Timestamp
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from bs4 import BeautifulSoup

# -------------------- FEEDS --------------------
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

# -------------------- WATCHLIST --------------------
WATCHLIST_COMPANIES = [
    "Union Pacific", "BNSF", "CSX", "Norfolk Southern", "Canadian National",
    "Canadian Pacific Kansas City", "CPKC", "J.B. Hunt", "Schneider", "Knight-Swift",
    "Swift", "Werner", "Heartland Express", "Prime Inc", "Old Dominion", "ODFL",
    "Saia", "XPO", "Yellow", "Estes", "R+L", "ABF Freight", "ArcBest", "TFI",
    "C.H. Robinson", "CHRW", "RXO", "Echo Global Logistics", "Arrive Logistics",
    "NFI", "Hub Group", "Coyote", "Uber Freight", "Convoy", "IMC Companies",
]

SECTOR_KEYWORDS = [
    "truck", "trucking", "truckload", "less-than-truckload",
    "intermodal", "rail", "railroad", "container", "containers",
    "drayage", "chassis", "interchange", "ramp", "broker", "brokerage",
    "3pl", "intermodal marketing company", "transload", "transloading",
    "linehaul", "capacity", "tender", "diesel", "fuel",
    "supply chain", "freight", "shipper", "intermodal rail", "interline", "lane",
    "service metrics", "transit time",
]
SECTOR_KEYWORDS.extend(WATCHLIST_COMPANIES)

SOURCE_DOMAIN_ALLOWLIST = {
    "globenewswire.com", "businesswire.com", "prnewswire.com",
    "newsroom.jbhunt.com", "media.unionpacific.com", "bnsf.com",
    "investors.csx.com", "media.nscorp.com", "cn.ca", "cpkcr.com",
    "investors.schneider.com", "investors.hubgroup.com", "investors.chrobinson.com",
}

EXCLUSION_DOMAINS = {"taboola.com", "doubleclick.net", "mail.yahoo.com"}
EXCLUSION_PHRASES = [
    "class action", "shareholder alert", "law firm", "pomerantz",
    "festival", "awareness month", "sexual health", "haunted",
]

# -------------------- HELPERS --------------------
def _norm(s): return (s or "").strip()
def _domain_from_url(url: str) -> str:
    try:
        d = url.split("/")[2].lower()
        return d.replace("www.", "")
    except: return ""
def _parse_dt(dt_str):
    try: return Timestamp(dt_str).to_pydatetime().astimezone(timezone.utc)
    except: return None
def _build_word_regex(terms): 
    safe = [re.escape(t.strip()) for t in terms if t]
    return re.compile(r"\b(?:%s)\b" % "|".join(safe), re.IGNORECASE) if safe else re.compile(r"$^")

RE_WATCHLIST = _build_word_regex(WATCHLIST_COMPANIES)
RE_SECTOR    = _build_word_regex(SECTOR_KEYWORDS)

def fetch_content(url: str, timeout: int = 45):
    s = requests.Session()
    retries = Retry(total=6, backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    headers = {"User-Agent": "Mozilla/5.0 PressReleaseBot"}
    try:
        r = s.get(url, headers=headers, timeout=timeout)
        if r.ok: return r.text
    except Exception as e:
        print(f"[WARN] Fetch error {url}: {e}")
    return None

# -------------------- SCRAPERS --------------------
def scrape_businesswire(url):
    html = fetch_content(url)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    articles = []
    for a in soup.select("a[data-resource-type='PressRelease']"):
        link = "https://www.businesswire.com" + a.get("href", "")
        title = _norm(a.get_text())
        if title:
            articles.append({"title": title, "url": link, "summary": "", 
                             "published_utc": datetime.now(timezone.utc).isoformat(),
                             "source": "businesswire.com"})
    return articles

def scrape_globenewswire(url):
    html = fetch_content(url)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    articles = []
    for a in soup.select("div.release-card a[href]"):
        link = a.get("href", "")
        title = _norm(a.get_text())
        if title:
            articles.append({"title": title, "url": link, "summary": "",
                             "published_utc": datetime.now(timezone.utc).isoformat(),
                             "source": "globenewswire.com"})
    return articles

def scrape_prnewswire(url):
    html = fetch_content(url)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    articles = []
    for a in soup.select("div.card a[href]"):
        link = a.get("href", "")
        if not link.startswith("http"): link = "https://www.prnewswire.com" + link
        title = _norm(a.get_text())
        if title:
            articles.append({"title": title, "url": link, "summary": "",
                             "published_utc": datetime.now(timezone.utc).isoformat(),
                             "source": "prnewswire.com"})
    return articles

# -------------------- COLLECT --------------------
def collect_from_feeds(feed_urls, lookback_hours: int):
    rows = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    for url in feed_urls:
        if "businesswire.com" in url:
            rows.extend(scrape_businesswire(url)); continue
        if "globenewswire.com" in url and "RssFeed" not in url:
            rows.extend(scrape_globenewswire(url)); continue
        if "prnewswire.com" in url:
            rows.extend(scrape_prnewswire(url)); continue

        content = fetch_content(url)
        if not content:
            print(f"[WARN] Failed feed {url}: no content")
            continue
        feed = feedparser.parse(content)
        for e in feed.entries:
            title = _norm(getattr(e, "title", ""))
            link  = _norm(getattr(e, "link", "")) or url
            summ  = _norm(getattr(e, "summary", ""))
            published = None
            for key in ("published", "updated", "created"):
                if getattr(e, key, None):
                    published = _parse_dt(getattr(e, key)); break
            if published and published < cutoff: continue
            rows.append({"title": title, "url": link, "summary": summ,
                         "published_utc": published.isoformat() if published else "",
                         "source": _domain_from_url(link)})
    return pd.DataFrame(rows)

# -------------------- FILTER --------------------
def apply_filters(df, lookback_hours: int, strict_mode: int = 1):
    if df.empty: return df
    df["_dt"] = df["published_utc"].map(_parse_dt)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    before = len(df); df = df[df["_dt"].notna() & (df["_dt"] >= cutoff)]
    print(f"[DEBUG] Time filter: {before} -> {len(df)}")

    before = len(df); df = df[~df["source"].isin(EXCLUSION_DOMAINS)]
    print(f"[DEBUG] Domain hard-block: {before} -> {len(df)}")

    before = len(df); df = df[df["source"].isin(SOURCE_DOMAIN_ALLOWLIST)]
    print(f"[DEBUG] Allowlist: {before} -> {len(df)}")

    def row_positive(r):
        text = f"{r['title']} {r['summary']}"
        has_company = bool(RE_WATCHLIST.search(text))
        has_sector  = bool(RE_SECTOR.search(text))
        return (has_company and has_sector) if strict_mode == 2 else (has_company or has_sector)

    before = len(df); df = df[df.apply(row_positive, axis=1)]
    print(f"[DEBUG] Positive match: {before} -> {len(df)}")
    return df.sort_values("_dt", ascending=False)

# -------------------- PDF --------------------
def write_pdf(df, path: str, title: str):
    c = canvas.Canvas(path, pagesize=LETTER)
    w, h = LETTER; y = h - 0.75*inch
    c.setFont("Helvetica-Bold", 14); c.drawString(0.75*inch, y, title); y -= 20
    c.setFont("Helvetica", 9)
    if df.empty:
        c.drawString(0.75*inch, y, "No qualifying press releases."); c.save(); return
    for _, r in df.iterrows():
        c.drawString(0.75*inch, y, f"• {r['title']}"); y -= 12
        c.drawString(1.0*inch, y, f"{r['url']}"); y -= 12
        c.drawString(1.0*inch, y, f"{r['source']} | {r['_dt']}"); y -= 20
        if y < 1.0*inch: c.showPage(); y = h - 0.75*inch; c.setFont("Helvetica", 9)
    c.save()

# -------------------- EMAIL --------------------
def send_email(subject, html_body, attachments):
    sender = os.environ.get("YAHOO_EMAIL","").strip()
    app_pw = os.environ.get("YAHOO_APP_PASSWORD","").strip()
    to_raw = os.environ.get("TO_EMAIL","").strip()
    if not (sender and app_pw and to_raw):
        raise RuntimeError("Missing YAHOO_EMAIL, YAHOO_APP_PASSWORD, or TO_EMAIL.")
    to_list = [x.strip() for x in to_raw.split(",") if x.strip()]
    msg = EmailMessage()
    msg["From"] = sender; msg["To"] = ", ".join(to_list)
    msg["Subject"] = subject
    msg.set_content("See HTML part")
    msg.add_alternative(html_body, subtype="html")
    for fname, data, mime in attachments:
        maintype, subtype = mime.split("/", 1)
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=fname)
    with smtplib.SMTP_SSL("smtp.mail.yahoo.com", 465, context=ssl.create_default_context()) as s:
        s.login(sender, app_pw); s.send_message(msg)

# -------------------- MAIN --------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback_hours", default="24")
    ap.add_argument("--send_always", default="true")
    args = ap.parse_args()

    lookback_hours = int(args.lookback_hours)
    send_always = args.send_always.lower() == "true"

    df_raw = collect_from_feeds(DEFAULT_FEEDS, lookback_hours)
    print(f"[INFO] Raw rows: {len(df_raw)}")

    df = apply_filters(df_raw, lookback_hours)
    print(f"[INFO] Filtered rows: {len(df)}")

    os.makedirs("reports", exist_ok=True)
    tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    out_csv, out_json, out_xlsx, out_pdf = (
        f"reports/press_releases_{tag}.csv",
        f"reports/press_releases_{tag}.json",
        f"reports/press_releases_{tag}.xlsx",
        f"reports/press_releases_{tag}.pdf"
    )
    df.to_csv(out_csv, index=False)
    df.to_json(out_json, orient="records", indent=2, date_format="iso")
    with pd.ExcelWriter(out_xlsx) as xl: df.to_excel(xl, index=False)
    write_pdf(df, out_pdf, "Press Release Brief")

    subject = f"[Press Releases] {len(df)} items in last {lookback_hours}h"
    if len(df) or send_always:
        rows_html = "".join(
            f"<tr><td><a href='{r['url']}'>{r['title']}</a><br>"
            f"<span>{r['source']} | {r['_dt']}</span></td></tr>"
            for _, r in df.iterrows()
        ) or f"<tr><td>No qualifying items in {lookback_hours}h.</td></tr>"
        html_body = f"<html><body><h3>Press Releases</h3><table>{rows_html}</table></body></html>"
        attachments = [(os.path.basename(p), open(p,"rb").read(), m)
                       for p,m in [(out_csv,"text/csv"),
                                   (out_json,"application/json"),
                                   (out_xlsx,"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
                                   (out_pdf,"application/pdf")]]
        send_email(subject, html_body, attachments)
        print("[OK] Email sent successfully.")
    else:
        print("[OK] No items, no email.")

if __name__ == "__main__":
    main()
