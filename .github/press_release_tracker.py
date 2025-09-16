#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
One Big Beautiful PY — Press Release Collector → Filter → Report → Email

What it does (single file):
  1) (Optional) Collects press releases from RSS/newsroom feeds.
  2) Filters STRICTLY to trucking/LTL/intermodal/rail/brokers (watchlist + sector).
  3) Excludes law-firm spam, awareness months, festivals, ads/portal links.
  4) Writes CSV, XLSX, JSON, and PDF to reports/press_releases_YYYYMMDD_HHMM.*
  5) Emails the bundle via Yahoo (SMTP over SSL).

Run locally or in GitHub Actions.

CLI:
  python press_releases_report.py --lookback_hours 24 --send_always true

Environment variables (typical for Actions):
  # Core behavior
  LOOKBACK_HOURS       (default "24")
  SEND_ALWAYS          ("true"|"false", default "true")
  COLLECT              ("1" to collect feeds [default]; "0" to read INPUT_PATH)
  INPUT_PATH           (CSV or JSON if COLLECT=0; default "outputs/press_releases_raw.csv")
  SOURCE_URLS          (comma-separated RSS URLs; if empty, uses DEFAULT_FEEDS)
  STRICT_POSITIVE      ("1" default => (watchlist OR sector); "2" => require BOTH)

  # Email (Yahoo)
  YAHOO_EMAIL          (sender, e.g., ari_ashe@yahoo.com)
  YAHOO_APP_PASSWORD   (Yahoo app password)
  YAHOO_TO             (comma-separated recipients)
  YAHOO_CC             (optional, comma-separated)

Dependencies (requirements.txt):
  pandas>=2.2.2
  openpyxl>=3.1.5
  reportlab>=4.2.2
  feedparser>=6.0.11
"""

import os
import re
import io
import ssl
import json
import smtplib
import argparse
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

import pandas as pd
from pandas import Timestamp
import feedparser
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas


# =========================
# Configuration
# =========================

# Default feeds (broad wires). Add company-specific newsroom RSS for higher precision.
DEFAULT_FEEDS = [
    # Transportation/logistics categories on major wires:
    "https://www.globenewswire.com/RssFeed/industry/Transportation.xml",
    "https://www.businesswire.com/portal/site/home/news/rss/industry/?vnsId=31367",
    # Examples of company newsrooms (uncomment/edit to focus):
    # "https://investors.schneider.com/rss/news-releases.xml",
    # "https://investors.hubgroup.com/rss/news-releases.xml",
    # "https://investors.chrobinson.com/rss/news-releases.xml",
    # "https://media.unionpacific.com/releases/rss.xml",
    # "https://www.bnsf.com/rss/news-media.xml",
    # "https://investors.csx.com/rss/news-releases.xml",
    # "https://media.nscorp.com/rss/news.xml",
    # "https://www.cpkcr.com/en/media/rss.xml",
]

# Watchlist (NO 2-letter abbreviations that cause false positives)
WATCHLIST_COMPANIES = [
    # Class I rail
    "Union Pacific", "BNSF", "CSX", "Norfolk Southern",
    "Canadian National", "Canadian Pacific Kansas City", "CPKC",

    # TL / LTL
    "J.B. Hunt", "Schneider", "Knight-Swift", "Swift", "Werner",
    "Heartland Express", "Prime Inc",
    "Old Dominion", "ODFL", "Saia", "XPO", "Yellow", "Estes", "R+L",
    "ABF Freight", "ArcBest", "TFI",

    # Brokers / IMCs / 3PL
    "C.H. Robinson", "CHRW", "RXO", "Echo Global Logistics", "Arrive Logistics",
    "NFI", "Hub Group", "Coyote", "Uber Freight", "Convoy",
    "Schneider Logistics", "IMC Companies",
]

# Sector keywords (matched with word-boundary regex)
SECTOR_KEYWORDS = [
    "truck", "trucking", "truckload", "tl", "ltl", "less-than-truckload",
    "intermodal", "rail", "railroad", "class i",
    "container", "containers", "drayage", "chassis", "interchange", "ramp",
    "broker", "brokerage", "3pl", "imc",
    "transload", "transloading",
    "linehaul", "capacity", "tender", "diesel", "fuel",
    "supply chain", "freight", "shipper", "bco",
    "interline", "lane", "service metrics", "transit time",
]

# Keep only from these (after we also drop EXCLUSION_DOMAINS); add official newsrooms here
SOURCE_DOMAIN_ALLOWLIST = {
    "www.globenewswire.com",
    "www.businesswire.com",
    "www.prnewswire.com",
    # official newsrooms (add freely)
    "newsroom.jbhunt.com", "media.unionpacific.com", "www.bnsf.com",
    "investors.csx.com", "media.nscorp.com", "www.cn.ca", "www.cpkcr.com",
    "investors.schneider.com", "investors.hubgroup.com", "investors.chrobinson.com",
}

# Hard-block ad/portal/tracker domains that are not real press releases
EXCLUSION_DOMAINS = {
    "api.taboola.com",
    "ad.doubleclick.net",
    "mail.yahoo.com",
    "r.mail.yahoo.com",
    "news.mail.yahoo.com",
}

# Phrase-based exclusions (case-insensitive)
EXCLUSION_PHRASES = [
    # law firm/class-action spam
    "class action", "securities litigation", "shareholder alert", "investigation -",
    "m&a class action", "rosen law firm", "pomerantz", "glancy prongay", "monteverde & associates",
    # awareness/festivals/medical promos
    "awareness month", "prostate cancer", "sexual health", "festival", "fall festival",
    "haunted", "pelvic tech", "ubiquinol",
]


# =========================
# Helpers
# =========================

def _norm(s: str) -> str:
    return (s or "").strip()

def _domain_from_url(url: str) -> str:
    try:
        return re.sub(r"^https?://", "", url.split("/")[2].lower())
    except Exception:
        return ""

def _parse_dt(dt_str: str):
    if not dt_str:
        return None
    try:
        return Timestamp(dt_str).to_pydatetime().astimezone(timezone.utc)
    except Exception:
        return None

def _contains_any(text: str, needles: list[str]) -> bool:
    t = (text or "").lower()
    for n in needles:
        if n and n.lower() in t:
            return True
    return False

def _build_word_regex(terms: list[str]) -> re.Pattern:
    """
    Build a case-insensitive regex that matches any whole term using word boundaries.
    Multi-word terms are handled (e.g., 'Union Pacific').
    """
    safe = [re.escape(t.strip()) for t in terms if t and t.strip()]
    pattern = r"\b(?:%s)\b" % "|".join(safe) if safe else r"$^"
    return re.compile(pattern, flags=re.IGNORECASE)

RE_WATCHLIST = _build_word_regex(WATCHLIST_COMPANIES)
RE_SECTOR = _build_word_regex(SECTOR_KEYWORDS)


# =========================
# Collection
# =========================

def collect_from_feeds(feed_urls: list[str], lookback_hours: int) -> pd.DataFrame:
    rows = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

    for url in feed_urls:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries:
                title = _norm(getattr(e, "title", ""))
                link  = _norm(getattr(e, "link", ""))
                summ  = _norm(getattr(e, "summary", "") or getattr(e, "description", ""))

                # published / updated / created
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
                    "companies_matched": "",
                    "title": title,
                    "url": link,
                    "published_utc": published.isoformat() if published else "",
                    "summary": summ,
                })
        except Exception as ex:
            print(f"[WARN] Failed feed {url}: {ex}")

    return pd.DataFrame(rows)


# =========================
# Filtering
# =========================

def apply_filters(df_raw: pd.DataFrame, lookback_hours: int, strict_mode: int = 1) -> pd.DataFrame:
    """
    strict_mode:
      1 => pass if (watchlist OR sector) [recommended]
      2 => pass only if (watchlist AND sector) [very strict]
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()

    df = df_raw.copy()
    cols = {c.lower(): c for c in df.columns}
    def col(name): return cols.get(name, name)

    # Normalize text fields
    for c in ["title", "summary", "companies_matched", "source", "url"]:
        if c in cols:
            df[col(c)] = df[col(c)].astype(str).fillna("").map(_norm)

    # Datetime cutoff (UTC)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=int(lookback_hours))
    if "published_utc" in cols:
        df["_dt"] = df[col("published_utc")].apply(_parse_dt)
    elif "published_et" in cols:
        df["_dt"] = df[col("published_et")].apply(_parse_dt)
    else:
        df["_dt"] = None
    before_time = len(df)
    df = df[df["_dt"].notna() & (df["_dt"] >= cutoff)]
    print(f"[DEBUG] Time filter: {before_time} -> {len(df)}")

    if df.empty:
        return df

    # Domains: drop hard-blocks, then allowlist
    if "url" in cols:
        df["_domain"] = df[col("url")].map(_domain_from_url)

        before_block = len(df)
        df = df[~df["_domain"].isin(EXCLUSION_DOMAINS)]
        print(f"[DEBUG] Domain hard-block: {before_block} -> {len(df)}")

        if SOURCE_DOMAIN_ALLOWLIST:
            allow = {d.replace("https://","").replace("http://","") for d in SOURCE_DOMAIN_ALLOWLIST}
            before_allow = len(df)
            df = df[df["_domain"].isin(allow)]
            print(f"[DEBUG] Domain allowlist: {before_allow} -> {len(df)}")

    if df.empty:
        return df

    # Exclusion phrases (title + summary)
    before_phrase = len(df)
    excl_mask = df.apply(
        lambda r: _contains_any((r.get(col("title"), "") + " " + r.get(col("summary"), "")), EXCLUSION_PHRASES),
        axis=1
    )
    df = df[~excl_mask]
    print(f"[DEBUG] Exclusion phrases: {before_phrase} -> {len(df)}")

    if df.empty:
        return df

    # Positive match with word-boundary regex
    def row_positive(r) -> bool:
        title = r.get(col("title"), "") or ""
        summ  = r.get(col("summary"), "") or ""
        comps = r.get(col("companies_matched"), "") or ""
        text_all = f"{title} {summ} {comps}"

        has_company = bool(RE_WATCHLIST.search(text_all))
        has_sector  = bool(RE_SECTOR.search(f"{title} {summ}"))
        if strict_mode == 2:  # require BOTH
            return has_company and has_sector
        return has_company or has_sector

    before_pos = len(df)
    df = df[df.apply(row_positive, axis=1)]
    print(f"[DEBUG] Positive match: {before_pos} -> {len(df)}")

    # Dedup & sort
    if "url" in cols:
        before_dup = len(df)
        df = df.sort_values(by=["_dt"], ascending=False).drop_duplicates(subset=[col("url")], keep="first")
        print(f"[DEBUG] Dedup by URL: {before_dup} -> {len(df)}")
    if "title" in cols:
        before_title = len(df)
        df = df.drop_duplicates(subset=[col("title")], keep="first")
        print(f"[DEBUG] Dedup by title: {before_title} -> {len(df)}")

    if "_dt" in df.columns:
        df = df.sort_values("_dt", ascending=False)

    return df


# =========================
# PDF Output
# =========================

def write_pdf(df: pd.DataFrame, path: str, title: str):
    c = canvas.Canvas(path, pagesize=LETTER)
    width, height = LETTER
    margin = 0.75 * inch
    y = height - margin
    c.setFont("Helvetica-Bold", 14)
    c.drawString(margin, y, title)
    y -= 0.3 * inch
    c.setFont("Helvetica", 9)

    if df.empty:
        c.drawString(margin, y, "No qualifying press releases in the selected window.")
        c.save()
        return

    def draw_line(text: str):
        nonlocal y
        # naive wrap at ~110 chars
        while text:
            if len(text) <= 110:
                line = text
                text = ""
            else:
                cut = text.rfind(" ", 0, 110)
                if cut == -1:
                    cut = 110
                line, text = text[:cut], text[cut:].lstrip()
            if y < 1.0 * inch:
                c.showPage()
                y = height - margin
                c.setFont("Helvetica", 9)
            c.drawString(margin, y, line)
            y -= 12

    for _, r in df.iterrows():
        t = _norm(r.get("title") or "")
        u = _norm(r.get("url") or "")
        s = _norm(r.get("summary") or "")
        ts = r.get("_dt")
        ts_str = ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if ts else ""
        domain = r.get("_domain","")
        draw_line(f"• {t}")
        if u: draw_line(f"  {u}")
        meta = "  " + " | ".join([x for x in [domain, ts_str] if x])
        if meta.strip(): draw_line(meta)
        if s: draw_line(f"  {s}")
        y -= 6

    c.save()


# =========================
# Email
# =========================

def send_email(subject: str, html_body: str, attachments: list[tuple[str, bytes, str]]):
    sender = os.environ.get("YAHOO_EMAIL","").strip()
    app_pw = os.environ.get("YAHOO_APP_PASSWORD","").strip()
    to_raw = os.environ.get("YAHOO_TO","").strip()
    cc_raw = os.environ.get("YAHOO_CC","").strip()
    if not (sender and app_pw and to_raw):
        raise RuntimeError("Missing YAHOO_EMAIL, YAHOO_APP_PASSWORD, or YAHOO_TO.")
    to_list = [x.strip() for x in to_raw.split(",") if x.strip()]
    cc_list = [x.strip() for x in cc_raw.split(",") if x.strip()] if cc_raw else []

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join(to_list)
    if cc_list: msg["Cc"] = ", ".join(cc_list)
    msg["Subject"] = subject
    msg.set_content("HTML version required to view this report.")
    msg.add_alternative(html_body, subtype="html")

    for fname, data, mime in attachments:
        maintype, subtype = mime.split("/", 1)
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=fname)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.mail.yahoo.com", 465, context=context) as server:
        server.login(sender, app_pw)
        server.send_message(msg)


# =========================
# Main
# =========================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback_hours", default=os.environ.get("LOOKBACK_HOURS","24"))
    ap.add_argument("--send_always", default=os.environ.get("SEND_ALWAYS","true"))
    args = ap.parse_args()

    lookback_hours = int(str(args.lookback_hours))
    send_always = str(args.send_always).lower().strip() == "true"

    collect = os.environ.get("COLLECT","1").strip() != "0"
    input_path = os.environ.get("INPUT_PATH","outputs/press_releases_raw.csv")
    strict_mode_env = os.environ.get("STRICT_POSITIVE","1").strip()
    strict_mode = 2 if strict_mode_env == "2" else 1

    # Feeds
    env_urls = os.environ.get("SOURCE_URLS","").strip()
    feed_urls = [u.strip() for u in env_urls.split(",") if u.strip()] if env_urls else DEFAULT_FEEDS

    # 1) Collect or read
    if collect:
        print(f"[INFO] Collecting from {len(feed_urls)} feeds (lookback {lookback_hours}h)…")
        df_raw = collect_from_feeds(feed_urls, lookback_hours)
    else:
        print(f"[INFO] Skipping collection; reading {input_path}")
        if not os.path.exists(input_path):
            print(f"[WARN] Input not found: {input_path}")
            df_raw = pd.DataFrame()
        else:
            if input_path.lower().endswith(".json"):
                with open(input_path, "r", encoding="utf-8") as f:
                    df_raw = pd.DataFrame(json.load(f))
            else:
                df_raw = pd.read_csv(input_path)

    print(f"[INFO] Raw rows: {len(df_raw)}")

    # 2) Filter
    df = apply_filters(df_raw, lookback_hours=lookback_hours, strict_mode=strict_mode)
    print(f"[INFO] Filtered rows: {len(df)}")

    # 3) Outputs
    os.makedirs("reports", exist_ok=True)
    now_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    base = f"reports/press_releases_{now_tag}"
    out_csv  = f"{base}.csv"
    out_xlsx = f"{base}.xlsx"
    out_json = f"{base}.json"
    out_pdf  = f"{base}.pdf"

    df.to_csv(out_csv, index=False)
    df.to_json(out_json, orient="records", indent=2, date_format="iso")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xl:
        df.to_excel(xl, index=False, sheet_name="Press Releases")

    write_pdf(df, out_pdf, title="Press Release Brief — TL/LTL/Intermodal/Rail/Brokers")

    # 4) Email
    total = len(df)
    subject = f"[Press Releases] {total} items in last {lookback_hours}h"
    if total or send_always:
        rows_html = ""
        if total:
            for _, r in df.head(100).iterrows():
                t = _norm(r.get("title",""))
                u = _norm(r.get("url",""))
                s = _norm(r.get("summary",""))
                ts = r.get("_dt")
                ts_str = ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if ts else ""
                dom = r.get("_domain","")
                rows_html += f"""
                <tr>
                  <td style="padding:6px;border-bottom:1px solid #ddd;">
                    <a href="{u}">{t}</a><br>
                    <span style="color:#666;">{dom} | {ts_str}</span><br>
                    <span>{s}</span>
                  </td>
                </tr>
                """
        else:
            rows_html = f'<tr><td style="padding:12px;">No qualifying items in the last {lookback_hours} hours.</td></tr>'

        html_body = f"""
        <html><body>
          <h3>Press Release Brief (lookback: {lookback_hours}h)</h3>
          <table style="width:100%;border-collapse:collapse;font-family:Arial,sans-serif;font-size:14px;">
            {rows_html}
          </table>
          <p style="color:#666;font-size:12px;">Auto-generated at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}</p>
        </body></html>
        """

        attachments = []
        for path, mime in [
            (out_csv,  "text/csv"),
            (out_xlsx, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            (out_json, "application/json"),
            (out_pdf,  "application/pdf"),
        ]:
            with open(path, "rb") as f:
                attachments.append((os.path.basename(path), f.read(), mime))

        send_email(subject, html_body, attachments)
        print(f"[OK] Emailed report with {total} items.")
    else:
        print("[OK] No items and SEND_ALWAYS=false — no email sent.")


if __name__ == "__main__":
    main()