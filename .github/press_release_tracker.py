#!/usr/bin/env python3
"""
Press Release Tracker — with Yahoo email

ENV (required for email):
  YAHOO_EMAIL         -> your Yahoo address (e.g., ariashenews@yahoo.com)
  YAHOO_APP_PASSWORD  -> Yahoo "App Password" (not your login PW)
  TO_EMAIL            -> comma-separated recipients (e.g., "you@x.com,desk@y.com")

Optional:
  LOOKBACK_HOURS=24
  OUTPUT_DIR=outputs
  MAIL_FROM_NAME="Press Release Tracker"
  SEND_ALWAYS=true   # if false, skip email when 0 items
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

# ----------------------------
# Sources (add more newsroom/IR feeds as you like)
# ----------------------------
FEEDS = [
    # PR Newswire (all)
    "https://www.prnewswire.com/rss/all-news-releases-list.rss",
    # GlobeNewswire / company IR feeds can be added here:
    # "https://www.globenewswire.com/RssFeed/industry/4008-transportation-logistics?subtype=all",
    # "https://investors.csx.com/rss/press-releases.xml",
    # "https://www.up.com/media/releases/rss.xml",
    # "https://www.nscorp.com/content/nscorp/en/newsroom/_jcr_content.feed",
]

# ----------------------------
# Watchlist + buckets
# ----------------------------
COMPANY_GROUPS = {
    # Class I rail
    "Union Pacific": {"aliases": ["Union Pacific", r"\bUP\b"], "mode": "rail"},
    "BNSF": {"aliases": ["BNSF"], "mode": "rail"},
    "CSX": {"aliases": [r"\bCSX\b"], "mode": "rail"},
    "Norfolk Southern": {"aliases": ["Norfolk Southern", r"\bNS\b"], "mode": "rail"},
    "CPKC": {"aliases": ["CPKC", "Canadian Pacific Kansas City"], "mode": "rail"},
    "CN": {"aliases": [r"\bCN\b", "Canadian National"], "mode": "rail"},

    # Major TL/LTL
    "J.B. Hunt": {"aliases": ["J.B. Hunt", "JB Hunt", r"\bJBHT\b"], "mode": "TL"},
    "Schneider": {"aliases": ["Schneider"], "mode": "TL"},
    "Knight-Swift": {"aliases": ["Knight-Swift", "Knight Swift", "Swift Transportation", "Knight Transportation"], "mode": "TL"},
    "Werner": {"aliases": ["Werner Enterprises", "Werner"], "mode": "TL"},
    "U.S. Xpress": {"aliases": ["U.S. Xpress", "US Xpress"], "mode": "TL"},
    "Heartland Express": {"aliases": ["Heartland Express"], "mode": "TL"},
    "Covenant": {"aliases": ["Covenant Logistics", "Covenant Transport"], "mode": "TL"},
    "Marten": {"aliases": ["Marten", "Marten Transport"], "mode": "TL"},
    "PAM": {"aliases": ["PAM Transport", r"\bPAM\b"], "mode": "TL"},
    "CRST": {"aliases": ["CRST"], "mode": "TL"},
    "Roehl": {"aliases": ["Roehl"], "mode": "TL"},
    "Old Dominion": {"aliases": ["Old Dominion", "Old Dominion Freight Line", r"\bODFL\b"], "mode": "LTL"},
    "Saia": {"aliases": ["Saia"], "mode": "LTL"},
    "Estes": {"aliases": ["Estes"], "mode": "LTL"},
    "XPO": {"aliases": ["XPO", "XPO Logistics"], "mode": "LTL"},
    "FedEx Freight": {"aliases": ["FedEx Freight"], "mode": "LTL"},
    "TForce/UPS Freight": {"aliases": ["TForce Freight", "UPS Freight"], "mode": "LTL"},

    # 3PL / Intermodal
    "Hub Group": {"aliases": ["Hub Group"], "mode": "intermodal"},
    "C.H. Robinson": {"aliases": ["C.H. Robinson", "CH Robinson"], "mode": "intermodal"},
    "STG Logistics": {"aliases": ["STG Logistics"], "mode": "intermodal"},
    "XPO (intermodal)": {"aliases": ["XPO Intermodal"], "mode": "intermodal"},
    "TQL": {"aliases": ["Total Quality Logistics", r"\bTQL\b"], "mode": "intermodal"},
    "NFI": {"aliases": [r"\bNFI\b", "NFI Industries"], "mode": "intermodal"},
    "Ryder": {"aliases": ["Ryder", "Ryder System"], "mode": "intermodal"},
    "Penske Logistics": {"aliases": ["Penske Logistics"], "mode": "intermodal"},
    "Schneider Logistics": {"aliases": ["Schneider Logistics"], "mode": "intermodal"},
    "Uber Freight": {"aliases": ["Uber Freight", "Transplace"], "mode": "intermodal"},
    "Arrive Logistics": {"aliases": ["Arrive Logistics"], "mode": "intermodal"},
    "Worldwide Express": {"aliases": ["Worldwide Express"], "mode": "intermodal"},
    "Mode Global": {"aliases": ["Mode Global", "MODE Global"], "mode": "intermodal"},

    # Smaller TL (dry van / reefer only)
    "Prime Inc.": {"aliases": ["Prime Inc"], "mode": "TL"},
    "KLLM": {"aliases": ["KLLM"], "mode": "TL"},
    "Hirschbach": {"aliases": ["Hirschbach"], "mode": "TL"},
    "Navajo Express": {"aliases": ["Navajo Express"], "mode": "TL"},
    "Stevens Transport": {"aliases": ["Stevens Transport"], "mode": "TL"},
    "Freymiller": {"aliases": ["Freymiller"], "mode": "TL"},
    "Swift Refrigerated": {"aliases": ["Swift Refrigerated"], "mode": "TL"},
    "May Trucking": {"aliases": ["May Trucking"], "mode": "TL"},
    "Wilson Logistics": {"aliases": ["Wilson Logistics"], "mode": "TL"},
    "Nussbaum": {"aliases": ["Nussbaum"], "mode": "TL"},
    "Crete Carrier": {"aliases": ["Crete Carrier"], "mode": "TL"},
    "Shaffer Trucking": {"aliases": ["Shaffer Trucking"], "mode": "TL"},
    "Decker Truck Line": {"aliases": ["Decker Truck Line"], "mode": "TL"},
    "Carter Express": {"aliases": ["Carter Express"], "mode": "TL"},
    "Veriha": {"aliases": ["Veriha"], "mode": "TL"},
    "Bay & Bay": {"aliases": ["Bay & Bay", "Bay and Bay"], "mode": "TL"},
    "Roehl Refrigerated": {"aliases": ["Roehl Refrigerated"], "mode": "TL"},
    "Marten Refrigerated": {"aliases": ["Marten Refrigerated"], "mode": "TL"},
    "Ruan (reefer)": {"aliases": ["Ruan"], "mode": "TL"},
    "H.E.B. Logistics (reefer)": {"aliases": ["H.E.B. Logistics", "HEB Logistics"], "mode": "TL"},
    "WEL Companies": {"aliases": ["WEL Companies"], "mode": "TL"},
    "John Christner Trucking": {"aliases": ["John Christner Trucking", r"\bJCT\b"], "mode": "TL"},
    "CR England": {"aliases": ["CR England", "C.R. England"], "mode": "TL"},
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
def in_last_n_hours(dt: datetime, hours: int) -> bool: return dt >= now_et() - timedelta(hours=hours)

def parse_pubdate(entry):
    for key in ("published", "updated", "created"):
        if entry.get(key):
            try:
                dt = dtparse.parse(entry[key])
                if not dt.tzinfo: dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                return dt.astimezone(ZoneInfo("America/New_York"))
            except Exception:
                pass
    if getattr(entry, "published_parsed", None):
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=ZoneInfo("UTC"))
            return dt.astimezone(ZoneInfo("America/New_York"))
        except Exception:
            pass
    return None

def strip_tracking(url: str) -> str:
    try:
        u = urlparse(url)
        q = [(k, v) for (k, v) in parse_qsl(u.query, keep_blank_values=True)
             if not k.lower().startswith("utm") and k.lower() not in {"cmpid","clid","ocid","cmp","ref"}]
        return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), ""))
    except Exception:
        return url

def domain_rank(url: str) -> int:
    host = ".".join(urlparse(url).netloc.lower().split(".")[-2:])
    return PREFERRED_DOMAINS.index(host) if host in PREFERRED_DOMAINS else len(PREFERRED_DOMAINS) + 1

def slug_title(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", s.lower()).strip())

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
    items = []
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

            items.append({
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
    # prefer canonical wires; newest first
    keep = {}
    for it in items:
        k = it["title_slug"]
        if k not in keep or it["domain_rank"] < keep[k]["domain_rank"]:
            keep[k] = it
    return sorted(keep.values(), key=lambda x: x["published_et"], reverse=True)

def format_markdown(items):
    if not items:
        return "_No qualifying hard-news items in the last 24 hours._\n"
    tally = {"rail": 0, "intermodal": 0, "TL": 0, "LTL": 0}
    out = []
    for it in items:
        if it["mode"] in tally:
            tally[it["mode"]] += 1
        pub_dt = dtparse.parse(it["published_et"]).astimezone(ZoneInfo("America/New_York"))
        when_str = pub_dt.strftime("%b %d, %Y %I:%M %p ET")
        lead = textwrap.shorten(re.sub("<[^>]+>", "", it["summary"]).replace("&nbsp;", " "), 300, placeholder="…")
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
    subject_ts = now_et().strftime("%b %d, %Y %I:%M %p ET")
    subj = f"Press Release Tracker: {items_count} item(s) — {subject_ts}" if items_count else f"Press Release Tracker: No qualifying items — {subject_ts}"
    msg["Subject"] = subj
    msg["From"] = formataddr((FROM_NAME, FROM_EMAIL))
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
            s.ehlo()
            s.starttls()
            s.login(FROM_EMAIL, APP_PASS)
            s.send_message(msg)

    print(f"Emailed results to: {', '.join(TO_LIST)}")

def main():
    hours = int(os.getenv("LOOKBACK_HOURS", "24"))
    outdir = Path(os.getenv("OUTPUT_DIR", "outputs"))

    items = collect_items(hours=hours)
    md_path, json_path, n = write_outputs(items, outdir)
    email_via_yahoo(md_path, json_path, n)

if __name__ == "__main__":
    main()