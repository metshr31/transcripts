#!/usr/bin/env python3
"""
Press Release Tracker — Yahoo email + manual/cron friendly.

REQUIRED (from GitHub Actions secrets -> env):
  YAHOO_EMAIL         # sender (your Yahoo address), e.g., ari_ashe@yahoo.com
  YAHOO_APP_PASSWORD  # Yahoo App Password (not your login password)
  TO_EMAIL            # comma-separated recipients, e.g. "ari.ashe@spglobal.com, ari_ashe@yahoo.com"

OPTIONAL:
  LOOKBACK_HOURS=24
  OUTPUT_DIR=outputs
  MAIL_FROM_NAME="Press Release Tracker"
  SEND_ALWAYS=true   # if "false", skip email when no items
"""

import os, re, json, textwrap, smtplib
from email.message import EmailMessage
from email.utils import formataddr
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from pathlib import Path

import feedparser
from dateutil import parser as dtparse

# --- Sources (add more newsroom/IR feeds as you like) ---
FEEDS = [
    "https://www.prnewswire.com/rss/all-news-releases-list.rss",
    # Add GlobeNewswire / company IR feeds here if desired
    # "https://www.globenewswire.com/RssFeed/industry/4008-transportation-logistics?subtype=all",
    # "https://investors.csx.com/rss/press-releases.xml",
]

# --- Watchlist (abbrev. — keep full set you use) ---
COMPANY_GROUPS = {
    # Rail
    "Union Pacific": {"aliases": ["Union Pacific", r"\bUP\b"], "mode": "rail"},
    "BNSF": {"aliases": ["BNSF"], "mode": "rail"},
    "CSX": {"aliases": [r"\bCSX\b"], "mode": "rail"},
    "Norfolk Southern": {"aliases": ["Norfolk Southern", r"\bNS\b"], "mode": "rail"},
    "CPKC": {"aliases": ["CPKC", "Canadian Pacific Kansas City"], "mode": "rail"},
    "CN": {"aliases": [r"\bCN\b", "Canadian National"], "mode": "rail"},
    # TL/LTL (sample)
    "J.B. Hunt": {"aliases": ["J.B. Hunt", "JB Hunt", r"\bJBHT\b"], "mode": "TL"},
    "Schneider": {"aliases": ["Schneider"], "mode": "TL"},
    "Old Dominion": {"aliases": ["Old Dominion", "Old Dominion Freight Line", r"\bODFL\b"], "mode": "LTL"},
    # 3PL/Intermodal (sample)
    "Hub Group": {"aliases": ["Hub Group"], "mode": "intermodal"},
    "C.H. Robinson": {"aliases": ["C.H. Robinson", "CH Robinson"], "mode": "intermodal"},
    "Ryder": {"aliases": ["Ryder", "Ryder System"], "mode": "intermodal"},
    # ... (include the rest of your full list here)
}

HARD_NEWS = [
    "announces","launches","opens","inaugurates","begins service","resumes","suspends",
    "delays","cancels","acquires","merges","invests","orders","delivers","service change",
    "timetable","corridor","lane","gateway","terminal","ramp","intermodal","imdl","drayage",
    "chassis","embargo","outage","cyberattack","strike","lockout","tariff","surcharge","gri",
    "detention","demurrage","closure","reopens"
]
EXCLUDE_HINTS = [
    "webinar","sponsorship","sponsor","newsletter","hiring","careers",
    "investor calendar","conference call","earnings release","earnings call",
    "dividend","annual report"
]
PREFERRED_DOMAINS = [
    "prnewswire.com",
    "globenewswire.com",
    "apnews.com",
    "reuters.com",
    "wsj.com",
]

def now_et(): return datetime.now(ZoneInfo("America/New_York"))
def in_last_n_hours(dt, hours): return dt >= now_et() - timedelta(hours=hours)

def parse_pubdate(entry):
    for k in ("published","updated","created"):
        if entry.get(k):
            try:
                d = dtparse.parse(entry[k])
                if not d.tzinfo: d = d.replace(tzinfo=ZoneInfo("UTC"))
                return d.astimezone(ZoneInfo("America/New_York"))
            except Exception: pass
    if getattr(entry, "published_parsed", None):
        try:
            d = datetime(*entry.published_parsed[:6], tzinfo=ZoneInfo("UTC"))
            return d.astimezone(ZoneInfo("America/New_York"))
        except Exception: pass
    return None

def strip_tracking(url: str) -> str:
    try:
        u = urlparse(url)
        q = [(k,v) for (k,v) in parse_qsl(u.query, keep_blank_values=True)
             if not k.lower().startswith("utm") and k.lower() not in {"cmpid","clid","ocid","cmp","ref"}]
        return urlunparse((u.scheme,u.netloc,u.path,u.params,urlencode(q),""))
    except Exception:
        return url

def domain_rank(url: str) -> int:
    host = ".".join(urlparse(url).netloc.lower().split(".")[-2:])
    return PREFERRED_DOMAINS.index(host) if host in PREFERRED_DOMAINS else len(PREFERRED_DOMAINS) + 1

def slug_title(s: str) -> str:
    return re.sub(r"\s+"," ", re.sub(r"[^a-z0-9]+"," ", s.lower()).strip())

def any_kw(s: str, kws: list[str]) -> bool:
    s = s.lower()
    return any(kw in s for kw in kws)

def match_company(text: str):
    t = text.lower()
    for primary, cfg in COMPANY_GROUPS.items():
        for alias in cfg["aliases"]:
            if re.search(alias.lower(), t):
                return primary, cfg["mode"]
    return None, None

def guess_tag(text: str) -> str:
    imdl_kws = ["intermodal","imdl","ramp","terminal","drayage","chassis","stack train","domestic container"]
    return "[Intermodal]" if any_kw(text, imdl_kws) else "[Not intermodal]"

def collect_items(hours=24):
    rows = []
    for feed in FEEDS:
        try:
            parsed = feedparser.parse(feed)
        except Exception:
            continue
        for e in parsed.entries:
            title = (e.get("title") or "").strip()
            link = strip_tracking((e.get("link") or "").strip())
            summary = (e.get("summary") or e.get("description") or "").strip()
            if not title or not link: 
                continue
            pub = parse_pubdate(e) or now_et()
            if not in_last_n_hours(pub, hours): 
                continue

            body = f"{title}\n{summary[:800]}"
            if any_kw(body, EXCLUDE_HINTS): 
                continue
            if not any_kw(body, HARD_NEWS): 
                continue
            company, mode = match_company(body)
            if not company: 
                continue

            rows.append({
                "title": title,
                "link": link,
                "summary": summary,
                "published_et": pub.isoformat(),
                "company": company,
                "mode": mode,
                "tag": guess_tag(body),
                "domain_rank": domain_rank(link),
                "title_slug": slug_title(title),
            })

    # de-dupe (prefer better domain), newest first
    keep = {}
    for it in rows:
        k = it["title_slug"]
        if k not in keep or it["domain_rank"] < keep[k]["domain_rank"]:
            keep[k] = it
    return sorted(keep.values(), key=lambda x: x["published_et"], reverse=True)

def format_markdown(items):
    if not items:
        return "_No qualifying hard-news items in the last 24 hours._\n"
    tally = {"rail":0,"intermodal":0,"TL":0,"LTL":0}
    out = []
    for it in items:
        if it["mode"] in tally: tally[it["mode"]] += 1
        pub_dt = dtparse.parse(it["published_et"]).astimezone(ZoneInfo("America/New_York"))
        when_str = pub_dt.strftime("%b %d, %Y %I:%M %p ET")
        lead = textwrap.shorten(re.sub("<[^>]+>","", it["summary"]).replace("&nbsp;"," "), 300, placeholder="…")
        out.append(
            f"**Company:** {it['company']}\n"
            f"**Headline:** {it['title']}\n"
            f"**Summary:** {lead}\n"
            f"**Notable dates / timing:** Published {when_str}\n"
            f"**Canonical wire link:** {it['link']}\n"
            f"**Tag:** {it['tag']}\n\n---\n"
        )
    out.append(f"**Tally (last 24h):**  Rail: **{tally['rail']}** | Intermodal: **{tally['intermodal']}** | TL: **{tally['TL']}** | LTL: **{tally['LTL']}**\n")
    return "".join(out)

def write_outputs(items, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    ts = now_et().strftime("%Y%m%d_%H%M")
    md_path = outdir / f"press_release_tracker_{ts}_ET.md"
    json_path = outdir / f"press_release_tracker_{ts}_ET.json"
    md_path.write_text(format_markdown(items), encoding="utf-8")
    json_path.write_text(json.dumps(items, indent=2, ensure_ascii=False), encoding="utf-8")
    return md_path, json_path, len(items)

def email_via_yahoo(md_path, json_path, items_count):
    FROM_EMAIL = os.getenv("YAHOO_EMAIL", "").strip()
    APP_PASS   = os.getenv("YAHOO_APP_PASSWORD", "").strip()
    TO_LIST    = [e.strip() for e in os.getenv("TO_EMAIL", "").split(",") if e.strip()]
    FROM_NAME  = os.getenv("MAIL_FROM_NAME", "Press Release Tracker").strip()
    SEND_ALWAYS = os.getenv("SEND_ALWAYS", "true").lower() == "true"

    if not (FROM_EMAIL and APP_PASS and TO_LIST):
        print("Yahoo email env not fully set; skipping email.")
        return
    if not SEND_ALWAYS and items_count == 0:
        print("No items and SEND_ALWAYS=false; skipping email.")
        return

    msg = EmailMessage()
    ts = now_et().strftime("%b %d, %Y %I:%M %p ET")
    subj = f"Press Release Tracker: {items_count} item(s) — {ts}" if items_count else f"Press Release Tracker: No qualifying items — {ts}"
    msg["Subject"] = subj
    msg["From"] = formataddr((FROM_EMAIL if not FROM_NAME else FROM_NAME, FROM_EMAIL))
    msg["To"] = ", ".join(TO_LIST)
    msg.set_content(Path(md_path).read_text(encoding="utf-8"))

    msg.add_attachment(Path(md_path).read_bytes(), maintype="text", subtype="markdown", filename=os.path.basename(md_path))
    msg.add_attachment(Path(json_path).read_bytes(), maintype="application", subtype="json", filename=os.path.basename(json_path))

    try:
        with smtplib.SMTP_SSL("smtp.mail.yahoo.com", 465, timeout=30) as s:
            s.login(FROM_EMAIL, APP_PASS)
            s.send_message(msg)
    except Exception as e_ssl:
        print(f"SSL send failed ({e_ssl}); trying STARTTLS 587…")
        with smtplib.SMTP("smtp.mail.yahoo.com", 587, timeout=30) as s:
            s.ehlo(); s.starttls(); s.login(FROM_EMAIL, APP_PASS); s.send_message(msg)

    print(f"Emailed results to {len(TO_LIST)} recipient(s).")

def main():
    hours = int(os.getenv("LOOKBACK_HOURS", "24"))
    outdir = Path(os.getenv("OUTPUT_DIR", "outputs"))
    items = collect_items(hours=hours)
    md_path, json_path, n = write_outputs(items, outdir)
    email_via_yahoo(md_path, json_path, n)

if __name__ == "__main__":
    main()