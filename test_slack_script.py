# Python 3.9-compatible – Slack → Notion link harvester
# - Canonicalizes URLs to avoid duplicates
# - Fetches human-friendly titles for web pages (OG/Twitter meta, <title>, <h1>, or URL inference)
# - For PDFs: uses Slack filename as the title
# - Notion properties: Article Name (Title), URL or Permalink (URL), Shared by (Rich text), Shared on (Date)

import os, re, time, html
from typing import Iterable, List, Dict, Any, Optional, Union
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, unquote, parse_qs

try:
    from zoneinfo import ZoneInfo
    CENTRAL = ZoneInfo("America/Chicago")
except Exception:
    CENTRAL = timezone.utc

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from notion_client import Client as Notion
from notion_client.errors import APIResponseError

# ------------- Config -------------
load_dotenv()
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

for k, v in {
    "SLACK_BOT_TOKEN": SLACK_BOT_TOKEN,
    "SLACK_CHANNEL_ID": SLACK_CHANNEL_ID,
    "NOTION_TOKEN": NOTION_TOKEN,
    "NOTION_DATABASE_ID": NOTION_DATABASE_ID,
}.items():
    if not v:
        raise RuntimeError(f"Missing {k} in environment/.env")

slack = WebClient(token=SLACK_BOT_TOKEN)
notion = Notion(auth=NOTION_TOKEN)

URL_RE = re.compile(r"https?://[^\s<>]+")  # capture URL tokens, stop at spaces/angle-brackets

# Common tracking params to strip for canonicalization
TRACKING_KEYS = {
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "utm_name","utm_cid","utm_reader","utm_viz_id","utm_pubreferrer",
    "utm_swu","ga_source","ga_medium","ga_campaign","ga_content",
    "fbclid","gclid","mc_cid","mc_eid","igshid","mkt_tok"
}

# ------------- Time helpers -------------
def chicago_time_from_ts(ts: Union[str, float]) -> str:
    try:
        tsf = float(ts)
    except Exception:
        tsf = time.time()
    return datetime.fromtimestamp(tsf, tz=CENTRAL).strftime("%Y-%m-%d %H:%M:%S %Z")

def iso_from_ts(ts: Union[str, float]) -> str:
    try:
        tsf = float(ts)
    except Exception:
        tsf = time.time()
    return datetime.fromtimestamp(tsf, tz=timezone.utc).isoformat()

# ------------- Slack helpers -------------
def get_user_display(uid: Optional[str]) -> str:
    if not uid:
        return "Unknown"
    if uid.startswith(("U", "W")):
        try:
            u = slack.users_info(user=uid)["user"]
            p = u.get("profile", {})
            return p.get("display_name") or u.get("real_name") or uid
        except SlackApiError:
            return uid
    return "Bot"

def list_all_messages(cid: str) -> Iterable[Dict[str, Any]]:
    """Yield all messages (chronological) including thread replies."""
    cursor = None
    messages: List[Dict[str, Any]] = []
    while True:
        r = slack.conversations_history(channel=cid, cursor=cursor, limit=200)
        messages += r.get("messages", [])
        cursor = r.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
        time.sleep(0.25)

    for m in reversed(messages):
        yield m
        if m.get("thread_ts") and int(m.get("reply_count", 0)) > 0:
            tcur = None
            tmsgs: List[Dict[str, Any]] = []
            while True:
                rr = slack.conversations_replies(channel=cid, ts=m["thread_ts"], cursor=tcur, limit=200)
                tmsgs += rr.get("messages", [])[1:]  # skip parent
                tcur = rr.get("response_metadata", {}).get("next_cursor")
                if not tcur:
                    break
                time.sleep(0.2)
            for t in tmsgs:
                yield t

# ------------- URL normalization -------------
def canonicalize_url(u: str) -> Optional[str]:
    if not u:
        return None
    u = html.unescape(u).strip()
    # Slack <url|label> or <url>
    if u.startswith("<") and ">" in u:
        inner = u[1:u.index(">")]
        if "|" in inner:
            inner = inner.split("|", 1)[0]
        u = inner
    # If someone typed two links separated by |, take the first
    if "|" in u:
        u = u.split("|", 1)[0].strip()
    u = u.rstrip(").,]}>\"'")

    try:
        p = urlparse(u)
    except Exception:
        return None
    if not p.scheme or not p.netloc:
        return None

    scheme = p.scheme.lower()
    netloc = p.netloc.lower()
    if netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]
    if netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]

    path = p.path or ""
    if path.endswith("/") and len(path) > 1:
        path = path.rstrip("/")

    q_pairs = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)]
    q_pairs = [(k, v) for k, v in q_pairs if k not in TRACKING_KEYS]
    if q_pairs:
        q_pairs.sort()
        query = urlencode(q_pairs, doseq=True)
    else:
        query = ""

    fragment = ""
    return urlunparse((scheme, netloc, path, "", query, fragment))

def extract_urls(msg: Dict[str, Any]) -> List[str]:
    candidates: List[str] = []
    text = msg.get("text") or ""
    candidates += URL_RE.findall(text)

    for a in msg.get("attachments", []) or []:
        if not isinstance(a, dict):
            continue
        for k in ("original_url", "title_link", "from_url"):
            v = a.get(k)
            if isinstance(v, str):
                candidates.append(v)

    def walk(o: Any):
        if isinstance(o, dict):
            v = o.get("url")
            if isinstance(v, str):
                candidates.append(v)
            for vv in o.values():
                walk(vv)
        elif isinstance(o, list):
            for vv in o:
                walk(vv)

    for b in msg.get("blocks", []) or []:
        walk(b)

    seen = set()
    out: List[str] = []
    for raw in candidates:
        canon = canonicalize_url(raw)
        if canon and canon not in seen:
            seen.add(canon)
            out.append(canon)
    return out

def is_pdf_like(f: Dict[str, Any]) -> bool:
    if not f:
        return False
    mt = (f.get("mimetype") or "").lower()
    ft = (f.get("filetype") or "").lower()
    n = (f.get("name") or f.get("title") or "").lower()
    return mt == "application/pdf" or ft == "pdf" or n.endswith(".pdf")

def message_permalink(cid: str, ts: str) -> Optional[str]:
    try:
        return slack.chat_getPermalink(channel=cid, message_ts=ts).get("permalink")
    except SlackApiError:
        return None

def first_pdf_name(msg: Dict[str, Any]) -> Optional[str]:
    for f in msg.get("files") or []:
        if isinstance(f, dict) and is_pdf_like(f):
            return f.get("name") or f.get("title")
    return None

def pdf_message_links(msg: Dict[str, Any], cid: str) -> List[str]:
    fs = msg.get("files") or []
    if not isinstance(fs, list) or not fs:
        return []
    if not any(is_pdf_like(f) for f in fs if isinstance(f, dict)):
        return []
    ts = msg.get("ts")
    if not ts:
        return []
    link = message_permalink(cid, ts)
    if not link:
        return []
    canon = canonicalize_url(link) or link
    return [canon]

# ------------- Title resolution for web pages -------------
def _clean_title(txt: Optional[str]) -> Optional[str]:
    if not txt:
        return None
    t = " ".join(txt.strip().split())
    return t[:300] if t else None

def _infer_title_from_url(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        q = parse_qs(p.query)
        for key in ("title", "headline", "paper", "article", "name"):
            if key in q and q[key]:
                guess = _clean_title(unquote(q[key][0]))
                if guess:
                    return guess

        parts = [seg for seg in p.path.split("/") if seg]
        if parts:
            leaf = parts[-1]
            if "." in leaf:
                leaf = leaf.rsplit(".", 1)[0]
            leaf = unquote(leaf)
            leaf = leaf.replace("-", " ").replace("_", " ")
            guess = _clean_title(leaf.title())
            if guess and len(guess) >= 4:
                return guess

        host = (p.netloc or "").split(":")[0]
        if host:
            return host
    except Exception:
        pass
    return None

def fetch_title(url: str, timeout: int = 8) -> Optional[str]:
    """
    Resolve a human-friendly title for HTML pages:
    1) OG/Twitter meta, <title>, <h1>
    2) Fall back to URL inference
    (Note: PDFs are handled separately via Slack filename.)
    """
    try:
        r = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (LinkHarvester/1.0)"},
            allow_redirects=True,
        )
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")

        # Preferred: Open Graph / Twitter
        for css, attr in [
            ('meta[property="og:title"]', "content"),
            ('meta[name="og:title"]', "content"),
            ('meta[name="twitter:title"]', "content"),
            ('meta[property="twitter:title"]', "content"),
        ]:
            el = soup.select_one(css)
            if el and el.get(attr):
                cand = _clean_title(el.get(attr))
                if cand:
                    return cand

        # <title>
        if soup.title and soup.title.string:
            cand = _clean_title(soup.title.string)
            if cand:
                return cand

        # First <h1>
        h1 = soup.find("h1")
        if h1:
            cand = _clean_title(h1.get_text(" "))
            if cand:
                return cand

    except Exception:
        pass

    return _infer_title_from_url(url)

# ------------- Notion (upsert by URL) -------------
def notion_find_by_url(db: str, url: str) -> Optional[str]:
    try:
        r = notion.databases.query(
            database_id=db,
            filter={"property": "URL or Permalink", "url": {"equals": url}},
            page_size=1,
        )
        res = r.get("results", [])
        return res[0]["id"] if res else None
    except APIResponseError as e:
        print("[Notion] Query failed:", getattr(e, "message", str(e)))
        return None

def notion_upsert(db: str, url: str, title: str, shared_by: str, shared_on_iso: str) -> Optional[str]:
    pid = notion_find_by_url(db, url)
    props = {
        "Article Name": {"title": [{"text": {"content": title[:2000]}}]},
        "URL or Permalink": {"url": url},
        "Shared by": {"rich_text": [{"text": {"content": shared_by}}]},
        "Shared on": {"date": {"start": shared_on_iso}},
    }
    try:
        if pid:
            return notion.pages.update(page_id=pid, properties=props)["id"]
        else:
            return notion.pages.create(parent={"database_id": db}, properties=props)["id"]
    except APIResponseError as e:
        print("[Notion] Upsert failed:", getattr(e, "message", str(e)))
        return None

# ------------- Main -------------
def main():
    slack.auth_test()
    processed = 0

    for m in list_all_messages(SLACK_CHANNEL_ID):
        ts = m.get("ts", time.time())
        iso = iso_from_ts(ts)
        human = chicago_time_from_ts(ts)
        user = get_user_display(m.get("user") or m.get("bot_id") or "")

        # 1) Canonicalized article links (HTML pages)
        for u in extract_urls(m):
            title = fetch_title(u) or u
            notion_upsert(NOTION_DATABASE_ID, u, title, user, iso)
            processed += 1
            print(f"[Upsert] LINK | {u} | {user} | {human} | title={title}")

        # 2) PDFs → use (canonicalized) message permalink; title = Slack filename (reverted)
        pdfs = pdf_message_links(m, SLACK_CHANNEL_ID)
        if pdfs:
            p = pdfs[0]
            title = first_pdf_name(m) or "PDF shared in Slack"   # << reverted behavior
            notion_upsert(NOTION_DATABASE_ID, p, title, user, iso)
            processed += 1
            print(f"[Upsert] PDF  | {p} | {user} | {human} | title={title}")

    print(f"Processed {processed} items.")

if __name__ == "__main__":
    main()
