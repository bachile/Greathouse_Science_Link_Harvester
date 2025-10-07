# Slack → Notion (scientific articles; aggressive title/DOI resolution + cross-run dedupe)
# Notion props (unchanged): Article Name (Title), URL or Permalink (URL), Shared by (Rich text), Shared on (Date)
# Python 3.9 compatible

import os, re, time, html, io, json, random
from typing import Iterable, List, Dict, Any, Optional, Union, Tuple
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, unquote, parse_qs, quote

try:
    from zoneinfo import ZoneInfo
    CENTRAL = ZoneInfo("America/Chicago")
except Exception:
    CENTRAL = timezone.utc

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from pypdf import PdfReader
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from notion_client import Client as Notion
from notion_client.errors import APIResponseError

# ---------- Config ----------
load_dotenv()
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
FORCE_API_TITLES = os.getenv("FORCE_API_TITLES", "0") == "1"  # optional "no-scrape" fallback

for k,v in {"SLACK_BOT_TOKEN":SLACK_BOT_TOKEN,"SLACK_CHANNEL_ID":SLACK_CHANNEL_ID,
            "NOTION_TOKEN":NOTION_TOKEN,"NOTION_DATABASE_ID":NOTION_DATABASE_ID}.items():
    if not v: raise RuntimeError(f"Missing {k} in env/.env")

slack = WebClient(token=SLACK_BOT_TOKEN)
notion = Notion(auth=NOTION_TOKEN)

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5, connect=3, read=3, backoff_factor=0.6,
        status_forcelist=[403, 408, 429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({
        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":"en-US,en;q=0.8",
        "Referer":"https://www.google.com/"
    })
    return s

SESSION = make_session()

URL_RE = re.compile(r"https?://[^\s<>]+")
DOI_RE = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.I)

TRACKING_KEYS = {
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content",
    "utm_name","utm_cid","utm_reader","utm_viz_id","utm_pubreferrer",
    "utm_swu","ga_source","ga_medium","ga_campaign","ga_content",
    "fbclid","gclid","mc_cid","mc_eid","igshid","mkt_tok",
    "uuid","via","src","si","s","login","returnurl","redirect","ref"
}

SCHOLAR_HOSTS = {
    "doi.org","dx.doi.org","doi.wiley.com",
    "biorxiv.org","www.biorxiv.org","www.medrxiv.org","arxiv.org",
    "nature.com","www.nature.com","science.org","www.science.org",
    "cell.com","www.cell.com","sciencedirect.com","www.sciencedirect.com",
    "springer.com","link.springer.com","onlinelibrary.wiley.com","academic.oup.com",
    "aacrjournals.org","jamanetwork.com","pnas.org","www.pnas.org","bmj.com","www.bmj.com",
    "thelancet.com","tandfonline.com","www.tandfonline.com","frontiersin.org","www.frontiersin.org",
    "asm.org","journals.asm.org","asmscience.org","mdpi.com","www.mdpi.com","royalsocietypublishing.org",
    "sciencemag.org","www.sciencemag.org","jci.org","www.jci.org","embopress.org","www.embopress.org",
    "journals.sagepub.com","plos.org","journals.plos.org","pmc.ncbi.nlm.nih.gov","pubmed.ncbi.nlm.nih.gov",
}
NON_ARTICLE_EXTS = (".jpg",".jpeg",".png",".gif",".webp",".svg",".mp4",".mov",".avi",".mkv",".webm",".mp3")
SKIP_HOSTS = {"x.com","twitter.com","www.twitter.com","youtube.com","www.youtube.com","youtu.be",
              "facebook.com","www.facebook.com","instagram.com","www.instagram.com",
              "reddit.com","www.reddit.com"}

CROSSREF_HEADERS = {"User-Agent":"LinkHarvester/1.0 (mailto:you@example.com)"}

# Journal maps
OUP_JOURNAL_MAP = {"femsre": "FEMS Microbiology Reviews"}
OUP_JOURNAL_ABBREV = {"femsre": "FEMS Microbiol Rev"}
CELL_JOURNAL_MAP = {
    "cell": "Cell",
    "cell-reports": "Cell Reports",
    "cell-metabolism": "Cell Metabolism",
    "cell-host-microbe": "Cell Host & Microbe",
    "cell-systems": "Cell Systems",
    "cancer-cell": "Cancer Cell",
    "cell-chemical-biology": "Cell Chemical Biology",
    "immunity": "Immunity",
}

# ---------- Time ----------
def chicago_time_from_ts(ts: Union[str,float]) -> str:
    try: tsf=float(ts)
    except: tsf=time.time()
    return datetime.fromtimestamp(tsf,tz=CENTRAL).strftime("%Y-%m-%d %H:%M:%S %Z")

def iso_from_ts(ts: Union[str,float]) -> str:
    try: tsf=float(ts)
    except: tsf=time.time()
    return datetime.fromtimestamp(tsf,tz=timezone.utc).isoformat()

# ---------- Slack ----------
def get_user_display(uid: Optional[str]) -> str:
    if not uid: return "Unknown"
    if uid.startswith(("U","W")):
        try:
            u=slack.users_info(user=uid)["user"]
            p=u.get("profile",{})
            return p.get("display_name") or u.get("real_name") or uid
        except SlackApiError:
            return uid
    return "Bot"

def list_all_messages(cid:str)->Iterable[Dict[str,Any]]:
    cur=None; msgs=[]
    while True:
        r=slack.conversations_history(channel=cid,cursor=cur,limit=200)
        msgs+=r.get("messages",[])
        cur=r.get("response_metadata",{}).get("next_cursor")
        if not cur: break
        time.sleep(0.25)
    for m in reversed(msgs):
        yield m
        if m.get("thread_ts") and int(m.get("reply_count",0))>0:
            tcur=None; tmsgs=[]
            while True:
                rr=slack.conversations_replies(channel=cid,ts=m["thread_ts"],cursor=tcur,limit=200)
                tmsgs+=rr.get("messages",[])[1:]
                tcur=rr.get("response_metadata",{}).get("next_cursor")
                if not tcur: break
                time.sleep(0.2)
            for t in tmsgs: yield t

# ---------- URL normalization & article filtering ----------
def canonicalize_url(u: str) -> Optional[str]:
    if not u: return None
    u = html.unescape(u).strip()
    if u.startswith("<") and ">" in u:
        inner = u[1:u.index(">")]
        if "|" in inner: inner = inner.split("|",1)[0]
        u = inner
    if "|" in u: u = u.split("|",1)[0].strip()
    u = u.rstrip(").,]}>\"'")
    try: p = urlparse(u)
    except: return None
    if not p.scheme or not p.netloc: return None
    scheme = p.scheme.lower(); netloc = p.netloc.lower()
    if netloc.endswith(":80") and scheme=="http": netloc = netloc[:-3]
    if netloc.endswith(":443") and scheme=="https": netloc = netloc[:-4]
    path = p.path or ""
    if path.endswith("/") and len(path)>1: path = path.rstrip("/")
    q_pairs = [(k,v) for k,v in parse_qsl(p.query, keep_blank_values=True) if k not in TRACKING_KEYS]
    query = urlencode(sorted(q_pairs), doseq=True) if q_pairs else ""
    return urlunparse((scheme, netloc, path, "", query, ""))

def is_scholarly_url(url: str) -> bool:
    try: p = urlparse(url)
    except: return False
    host = (p.netloc or "").lower()
    path = (p.path or "").lower()
    if host in SKIP_HOSTS or host.endswith(".slack.com"): return False
    if path.endswith(NON_ARTICLE_EXTS): return False
    if DOI_RE.search(url): return True
    if host in SCHOLAR_HOSTS: return True
    if any(seg in path for seg in ("/doi/","/article/","/articles/","/abs/","/fulltext/","/content/")): return True
    return False

def quick_content_type(url: str) -> Optional[str]:
    try:
        r = SESSION.head(url, timeout=10, allow_redirects=True)
        if r.status_code // 100 == 3:
            r = SESSION.get(url, timeout=10, allow_redirects=True, stream=True)
        return r.headers.get("Content-Type","").lower()
    except Exception:
        return None

def extract_urls(msg:Dict[str,Any])->List[str]:
    cand=[]
    text=msg.get("text") or ""
    cand += URL_RE.findall(text)
    for a in msg.get("attachments",[]) or []:
        if isinstance(a,dict):
            for k in ("original_url","title_link","from_url"):
                v=a.get(k)
                if isinstance(v,str): cand.append(v)
    def walk(o: Any):
        if isinstance(o,dict):
            v=o.get("url")
            if isinstance(v,str): cand.append(v)
            for vv in o.values(): walk(vv)
        elif isinstance(o,list):
            for vv in o: walk(vv)
    for b in msg.get("blocks",[]) or []: walk(b)

    seen=set(); out=[]
    for raw in cand:
        u = canonicalize_url(raw)
        if u and u not in seen:
            seen.add(u)
            out.append(u)

    keep=[]
    for u in out:
        if not is_scholarly_url(u): continue
        ct = quick_content_type(u)
        if ct and any(x in ct for x in ("image/","video/","audio/")): continue
        keep.append(u)
    return keep

# ---------- PDFs ----------
def is_pdf_like_file(f:Dict[str,Any])->bool:
    if not f: return False
    mt=(f.get("mimetype") or "").lower()
    ft=(f.get("filetype") or "").lower()
    n =(f.get("name") or f.get("title") or "").lower()
    return mt=="application/pdf" or ft=="pdf" or n.endswith(".pdf")

def get_pdf_files(msg:Dict[str,Any])->List[Dict[str,Any]]:
    return [f for f in (msg.get("files") or []) if isinstance(f,dict) and is_pdf_like_file(f)]

def is_direct_pdf_url(url: str) -> bool:
    ct = quick_content_type(url) or ""
    return ("application/pdf" in ct) or url.lower().endswith(".pdf")

def message_permalink(cid:str,ts:str)->Optional[str]:
    try: return slack.chat_getPermalink(channel=cid,message_ts=ts).get("permalink")
    except SlackApiError: return None

def pdf_message_permalink(msg:Dict[str,Any],cid:str)->Optional[str]:
    ts=msg.get("ts")
    if not ts: return None
    link=message_permalink(cid,ts)
    return canonicalize_url(link) or link

# ---------- Crossref & APIs ----------
def try_crossref_title(doi: str, retries:int=2)->Optional[str]:
    doi = doi.strip()
    for i in range(retries+1):
        try:
            r = SESSION.get(f"https://api.crossref.org/works/{doi}",
                            timeout=10, headers=CROSSREF_HEADERS)
            if r.status_code==200:
                msg=(r.json() or {}).get("message",{})
                titles=msg.get("title")
                if isinstance(titles,list) and titles:
                    t=" ".join(str(x) for x in titles if x).strip()
                    if t: return t[:300]
            time.sleep(0.5*(i+1)+random.random()*0.3)
        except Exception:
            time.sleep(0.5*(i+1))
    return None

def crossref_search_title(query: str, prefer_domain: Optional[str]=None) -> Tuple[Optional[str], Optional[str]]:
    try:
        r = SESSION.get("https://api.crossref.org/works",
                        params={"query.bibliographic": query, "rows": 7, "select": "title,URL,DOI"},
                        timeout=12, headers=CROSSREF_HEADERS)
        if r.status_code != 200: return None, None
        items = (r.json() or {}).get("message",{}).get("items",[]) or []
        best_title, best_doi = None, None
        for it in items:
            titles = it.get("title") or []
            title = " ".join(titles).strip() if titles else ""
            urls = [it.get("URL")] if it.get("URL") else []
            doi = it.get("DOI")
            if prefer_domain and any((prefer_domain in (url or "")) for url in urls):
                if title: return title[:300], doi
            if not best_title and title:
                best_title, best_doi = title[:300], doi
        return best_title, best_doi
    except Exception:
        return None, None

def crossref_struct_title(container_title: Optional[str], volume: Optional[str], issue: Optional[str], page: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    params = {"rows": 5, "select": "title,URL,DOI"}
    if container_title: params["query.container-title"] = container_title
    if volume:          params["volume"] = volume
    if issue:           params["issue"] = issue
    if page:            params["page"] = page
    try:
        r = SESSION.get("https://api.crossref.org/works", params=params, timeout=12, headers=CROSSREF_HEADERS)
        if r.status_code != 200: return None, None
        items = (r.json() or {}).get("message", {}).get("items", []) or []
        for it in items:
            titles = it.get("title") or []
            t = " ".join(titles).strip() if titles else ""
            if t:
                return t[:300], it.get("DOI")
    except Exception:
        return None, None
    return None, None

def crossref_search_in_container(raw_query: str, container_title: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        params = {
            "query": raw_query,
            "query.container-title": container_title,
            "rows": 12,
            "select": "title,URL,DOI",
        }
        r = SESSION.get("https://api.crossref.org/works", params=params, timeout=12, headers=CROSSREF_HEADERS)
        if r.status_code != 200:
            return None, None
        items = (r.json() or {}).get("message", {}).get("items", []) or []
        for it in items:
            titles = it.get("title") or []
            t = " ".join(titles).strip() if titles else ""
            if t:
                return t[:300], it.get("DOI")
    except Exception:
        return None, None
    return None, None

def biorxiv_title_from_api(doi: str) -> Optional[str]:
    try:
        doi_enc = quote(doi, safe="")
        r = SESSION.get(f"https://api.biorxiv.org/details/biorxiv/{doi_enc}", timeout=12)
        if r.status_code == 200:
            coll = (r.json() or {}).get("collection") or []
            if coll:
                t = coll[0].get("title") or ""
                t = clean_text_strip_html(t)
                if t: return t[:300]
    except Exception:
        pass
    return None

# ---------- Utilities ----------
def clean_text_strip_html(s:str)->str:
    if not isinstance(s,str): return ""
    s = html.unescape(s)
    if "<" in s and ">" in s:
        s = BeautifulSoup(s,"html.parser").get_text(" ")
    return " ".join(s.split())

def normalize_title_for_key(s: str) -> str:
    s = clean_text_strip_html(s).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def looks_numericish(s: str) -> bool:
    if not isinstance(s, str): return True
    t = s.strip()
    if not t: return True
    core = re.sub(r'[^A-Za-z0-9]', '', t)
    if not core: return True
    digits = sum(c.isdigit() for c in core)
    return digits/len(core) >= 0.6 or bool(re.match(r'^\d{4}\.\d{2}\.\d{2}', t)) or bool(re.match(r'^[A-Z]{1,6}\d{2,}', t))

def safe_meta(soup: BeautifulSoup, names: list) -> Optional[str]:
    for nm in names:
        el = soup.find("meta", attrs={"name": nm})
        if el and el.get("content"):
            return clean_text_strip_html(el["content"])
    return None

# ---------- HTML parsing helpers ----------
SCHOLAR_META_CANDIDATES = [
    ('meta[name="citation_title"]',"content"),
    ('meta[name="dc.title"]',"content"),
    ('meta[name="DC.title"]',"content"),
    ('meta[name="prism.title"]',"content"),
    ('meta[property="og:title"]',"content"),
    ('meta[name="og:title"]',"content"),
    ('meta[name="twitter:title"]',"content"),
    ('meta[property="twitter:title"]',"content"),
]

def find_doi_in_soup(soup: BeautifulSoup) -> Optional[str]:
    for name in ("citation_doi","dc.identifier","dc.identifier.doi","prism.doi"):
        el = soup.find("meta", attrs={"name": name})
        if el and el.get("content"):
            m = DOI_RE.search(el["content"])
            if m: return m.group(0)
    for tag in soup.find_all("script", {"type":"application/ld+json"}):
        try: data=json.loads(tag.string or "{}")
        except Exception: continue
        def hunt(obj):
            if isinstance(obj,dict):
                for k,v in obj.items():
                    if k.lower() in ("doi","identifier") and isinstance(v,str):
                        m=DOI_RE.search(v)
                        if m: return m.group(0)
                    res = hunt(v)
                    if res: return res
            elif isinstance(obj,list):
                for it in obj:
                    res=hunt(it)
                    if res: return res
            return None
        got = hunt(data)
        if got: return got
    txt = soup.get_text(" ", strip=True)
    m = DOI_RE.search(txt)
    return m.group(0) if m else None

def find_title_in_jsonld(soup: BeautifulSoup) -> Optional[str]:
    for tag in soup.find_all("script", {"type":"application/ld+json"}):
        try: data=json.loads(tag.string or "{}")
        except Exception: continue
        def hunt(obj):
            if isinstance(obj,dict):
                typ=(obj.get("@type") or "").lower()
                if typ in ("article","scholarlyarticle","report","chapter"):
                    for key in ("headline","name"):
                        if key in obj and isinstance(obj[key],str):
                            return clean_text_strip_html(obj[key])
                for v in obj.values():
                    res=hunt(v)
                    if res: return res
            elif isinstance(obj,list):
                for it in obj:
                    res=hunt(it)
                    if res: return res
            return None
        t = hunt(data)
        if t: return t
    return None

def fetch_and_parse(url: str, timeout:int=20) -> Tuple[BeautifulSoup, requests.Response]:
    r = SESSION.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    return soup, r

# ---------- PubMed fallback ----------
def pubmed_title_by_jvp(journal: str, volume: str, page: str) -> Optional[str]:
    try:
        term = f'"{journal}"[Journal] {volume}[Volume] {page}[Page]'
        es = SESSION.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            params={"db":"pubmed","term":term,"retmode":"json","retmax":"1"},
            timeout=10,
        )
        es.raise_for_status()
        ids = (es.json() or {}).get("esearchresult", {}).get("idlist", [])
        if not ids: return None
        pmid = ids[0]
        esum = SESSION.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
            params={"db":"pubmed","id":pmid,"retmode":"json"},
            timeout=10,
        )
        esum.raise_for_status()
        res = (esum.json() or {}).get("result", {})
        itm = res.get(pmid, {})
        title = itm.get("title") or ""
        title = clean_text_strip_html(title)
        return title[:300] if title else None
    except Exception:
        return None

# ---------- Publisher-specific ----------
def publisher_specific_title_and_doi(url: str, soup: Optional[BeautifulSoup]) -> Tuple[Optional[str], Optional[str]]:
    host = urlparse(url).netloc.lower()

    # bioRxiv / medRxiv
    if "biorxiv.org" in host or "medrxiv.org" in host:
        if soup:
            el = soup.find("meta", attrs={"name":"citation_title"})
            if el and el.get("content"):
                return clean_text_strip_html(el["content"]), find_doi_in_soup(soup)
            d = find_doi_in_soup(soup)
            if d:
                t = biorxiv_title_from_api(d) or try_crossref_title(d)
                if t: return clean_text_strip_html(t), d
        m = DOI_RE.search(url)
        if m:
            d = m.group(0)
            t = biorxiv_title_from_api(d) or try_crossref_title(d)
            if t: return clean_text_strip_html(t), d

    # Cell / Elsevier family (Cell, ScienceDirect PII)
    if "cell.com" in host or "sciencedirect.com" in host or "elsevier.com" in host:
        if soup:
            t = safe_meta(soup, ["citation_title","dc.title","DC.title","prism.title"])
            if t: return t, find_doi_in_soup(soup)
            jl = find_title_in_jsonld(soup)
            if jl: return clean_text_strip_html(jl), find_doi_in_soup(soup)
            d = find_doi_in_soup(soup)
            if d:
                tt = try_crossref_title(d)
                if tt: return clean_text_strip_html(tt), d

        parts = [seg for seg in urlparse(url).path.split("/") if seg]
        journal_slug = parts[0] if parts else ""
        container = CELL_JOURNAL_MAP.get(journal_slug, None)
        leaf = parts[-1] if parts else ""  # raw PII
        if container and leaf:
            t, d = crossref_search_in_container(leaf, container)
            if t: return clean_text_strip_html(t), d
        hint_raw = leaf or url
        t, d = crossref_search_title(hint_raw, prefer_domain="cell.com")
        if not t:
            t, d = crossref_search_title(hint_raw, prefer_domain="sciencedirect.com")
        if t: return clean_text_strip_html(t), d

    # Oxford Academic (OUP)
    if "academic.oup.com" in host:
        jname = None
        if soup:
            t = safe_meta(soup, ["citation_title","dc.title","DC.title","prism.title"])
            if t: return t, find_doi_in_soup(soup)
            jname = safe_meta(soup, ["citation_journal_title"])
        parts = [seg for seg in urlparse(url).path.split("/") if seg]
        try:
            j_idx = parts.index("article")
            journal_slug = parts[j_idx-1] if j_idx-1 >= 0 else ""
            vol = parts[j_idx+1] if j_idx+1 < len(parts) else ""
            iss = parts[j_idx+2] if j_idx+2 < len(parts) else ""
            pg  = parts[j_idx+3] if j_idx+3 < len(parts) else ""
            container = jname or OUP_JOURNAL_MAP.get(journal_slug, journal_slug.replace("-", " "))

            t, d = crossref_struct_title(container, vol, iss, pg)
            if t: return clean_text_strip_html(t), d

            leaf = parts[-1] if parts else ""
            if container and (vol or iss or pg):
                qry = " ".join(x for x in [leaf, vol, iss, pg] if x)
                t, d = crossref_search_in_container(qry or leaf, container)
                if t: return clean_text_strip_html(t), d

            bib = " ".join(x for x in [container, vol, iss, pg] if x)
            if bib:
                t, d = crossref_search_title(bib, prefer_domain="academic.oup.com")
                if t: return clean_text_strip_html(t), d

            t2 = pubmed_title_by_jvp(container, vol, pg) or pubmed_title_by_jvp(OUP_JOURNAL_ABBREV.get(journal_slug,""), vol, pg)
            if t2: return clean_text_strip_html(t2), None
        except ValueError:
            pass

    return None, None

# ---------- PDF title helpers ----------
def _pick_best_sentence(candidate: str) -> str:
    parts = re.split(r"[\.!?]+", candidate)
    parts = [re.sub(r"\s+", " ", p).strip(" :;,-\u2013\u2014 ") for p in parts]
    parts = [p for p in parts if p]

    def alpha_ratio(s: str) -> float:
        letters = sum(c.isalpha() for c in s)
        return letters / max(1, len(s))

    def looks_all_caps(s: str) -> bool:
        letters = [c for c in s if c.isalpha()]
        return len(letters) >= 6 and sum(c.isupper() for c in letters) / len(letters) > 0.9

    def penalize_boiler(s: str) -> float:
        sl = s.lower()
        hits = 0
        for w in ("open access", "creative commons", "license", "copyright", "received", "accepted"):
            if w in sl: hits += 1
        return hits * 30.0

    best = candidate
    best_score = -1.0
    for s in parts:
        if len(s) < 10 or len(s) > 200: 
            continue
        if looks_all_caps(s): 
            continue
        score = len(s) * 1.0 + alpha_ratio(s) * 60.0 - penalize_boiler(s)
        if not s.islower(): 
            score += 10.0
        score -= s.count(",") * 2.0
        if score > best_score:
            best_score = score
            best = s

    best = re.sub(r"\s+", " ", best).strip(" .,:;-–—")
    return best

def extract_pdf_title_from_bytes(data: bytes) -> Tuple[Optional[str], Optional[str]]:
    try:
        reader = PdfReader(io.BytesIO(data))
    except Exception:
        return None, None

    try:
        md = getattr(reader, "metadata", None)
        if md and isinstance(md.title, str):
            t = " ".join(md.title.strip().split())
            if t and 10 <= len(t) <= 220:
                junk_flags = ("creative commons", "open access", "the author(s)", "this article is licensed")
                if not any(s in t.lower() for s in junk_flags):
                    return t, None
    except Exception:
        pass

    texts = []
    try:
        maxp = min(5, len(reader.pages))
        for i in range(maxp):
            txt = reader.pages[i].extract_text() or ""
            texts.append(txt)
    except Exception:
        pass

    all_text = "\n".join(texts)

    m = DOI_RE.search(all_text or "")
    if m:
        doi = m.group(0)
        cr = try_crossref_title(doi)
        if cr:
            return cr, doi

    page1 = (texts[0] if texts else "") or ""
    if not page1.strip():
        return None, None

    lines = [ln.strip() for ln in page1.splitlines()]
    lines = [ln for ln in lines if ln]

    BOILER_PATTERNS = [
        r"^\s*(research|review|article|original article|open access)\b.*$",
        r".*creative\s+commons.*",
        r".*this article is licensed.*",
        r".*the author\(s\).*",
        r".*rights\s+and\s+permissions.*",
        r".*springer\s+nature.*|.*elsevier.*|.*wiley.*|.*oxford\s+university\s+press.*",
        r".*received\s+\d{1,2}\s+\w+\s+\d{4}.*|.*accepted\s+\d{1,2}\s+\w+\s+\d{4}.*",
        r".*corresponding author.*|.*affiliation.*|.*email.*@.*",
        r"^doi:\s*10\.[^ ]+.*",
        r"^\d{1,3}\s*-\s*\d{1,3}$",
        r"^supplementary.*|^graphical abstract.*",
    ]
    boiler_res = [re.compile(p, re.I) for p in BOILER_PATTERNS]

    def is_boiler(s: str) -> bool:
        sl = s.lower()
        if any(r.search(s) for r in boiler_res): return True
        if "©" in s or "open access" in sl or "license" in sl:
            return True
        if len(s) < 8 or len(s) > 220:
            return True
        return False

    def find_abstract_idx(lst: List[str]) -> Optional[int]:
        for i, ln in enumerate(lst):
            if re.match(r"^\s*abstract\s*$", ln, re.I):
                return i
        return None

    abs_idx = find_abstract_idx(lines)
    search_lines = lines[:abs_idx] if abs_idx is not None else lines[:40]

    cand_lines = [ln for ln in search_lines if not is_boiler(ln)]

    def alpha_ratio(s: str) -> float:
        letters = sum(c.isalpha() for c in s)
        total = max(1, len(s))
        return letters / total

    def looks_all_caps(s: str) -> bool:
        letters = [c for c in s if c.isalpha()]
        return len(letters) >= 6 and sum(c.isupper() for c in letters) / len(letters) > 0.9

    best_score = -1.0
    best_block = None
    n = len(cand_lines)
    for i in range(n):
        for span in (1, 2, 3):
            if i + span > n: break
            block_lines = cand_lines[i:i+span]
            block = " ".join(block_lines)
            block = re.sub(r"\s+", " ", block).strip()
            if is_boiler(block): continue
            if len(block) < 10 or len(block) > 200: continue
            if looks_all_caps(block): continue
            score = len(block) * 1.0 + alpha_ratio(block) * 50.0
            next_line = cand_lines[i+span] if (i+span) < n else ""
            if re.search(r"\b[A-Z]\.\s*[A-Z]\.|,| and ", next_line):
                score += 10.0
            if score > best_score:
                best_score = score
                best_block = block

    if best_block and not looks_numericish(best_block):
        refined = _pick_best_sentence(best_block)
        if refined and not looks_numericish(refined):
            return refined, None
        return best_block, None

    for ln in cand_lines:
        if not looks_numericish(ln):
            return _pick_best_sentence(ln), None

    return None, None

def fetch_pdf_title_via_slack(file_obj: Dict[str,Any]) -> Tuple[Optional[str], Optional[str]]:
    url_priv = file_obj.get("url_private_download") or file_obj.get("url_private")
    if not url_priv: return None, None
    try:
        r = SESSION.get(url_priv, timeout=30, headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"})
        r.raise_for_status()
        return extract_pdf_title_from_bytes(r.content)
    except Exception:
        return None, None

def fetch_pdf_title_direct(url: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        r = SESSION.get(url, timeout=30)
        r.raise_for_status()
        return extract_pdf_title_from_bytes(r.content)
    except Exception:
        return None, None

# ---------- Title+DOI resolution ----------
def resolve_best_title_and_doi_for_url(url: str) -> Tuple[str, Optional[str]]:
    # Direct PDF?
    if is_direct_pdf_url(url):
        t, d = fetch_pdf_title_direct(url)
        if t: return clean_text_strip_html(t), d

    m = DOI_RE.search(url)
    if m:
        doi = m.group(0)
        if "biorxiv.org" in (urlparse(url).netloc.lower()):
            t = biorxiv_title_from_api(doi) or try_crossref_title(doi)
        else:
            t = try_crossref_title(doi)
        if t: return clean_text_strip_html(t), doi

    if FORCE_API_TITLES:
        host = urlparse(url).netloc.lower()
        parts = [seg for seg in urlparse(url).path.split("/") if seg]

        if "cell.com" in host or "sciencedirect.com" in host or "elsevier.com" in host:
            container = CELL_JOURNAL_MAP.get(parts[0] if parts else "", None)
            leaf = parts[-1] if parts else ""
            if container and leaf:
                t, d = crossref_search_in_container(leaf, container)
                if t: return clean_text_strip_html(t), d
            t, d = crossref_search_title(leaf or url, prefer_domain=host)
            if t: return clean_text_strip_html(t), d

        if "academic.oup.com" in host:
            try:
                j_idx = parts.index("article")
                journal_slug = parts[j_idx-1] if j_idx-1 >= 0 else ""
                vol = parts[j_idx+1] if j_idx+1 < len(parts) else ""
                iss = parts[j_idx+2] if j_idx+2 < len(parts) else ""
                pg  = parts[j_idx+3] if j_idx+3 < len(parts) else ""
                container = OUP_JOURNAL_MAP.get(journal_slug, journal_slug.replace("-", " "))
                t, d = crossref_struct_title(container, vol, iss, pg)
                if not t:
                    t = pubmed_title_by_jvp(container, vol, pg) or pubmed_title_by_jvp(OUP_JOURNAL_ABBREV.get(journal_slug,""), vol, pg)
                if t: return clean_text_strip_html(t), d
            except ValueError:
                pass

        # Fallback
        tt = infer_from_url(url) or url
        return clean_text_strip_html(tt), None

    # HTML fetch (normal path)
    try:
        soup, resp = fetch_and_parse(url)
        current_url = resp.url
    except Exception:
        return clean_text_strip_html(infer_from_url(url) or url), None

    # Canonical hop (once)
    link = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
    if link and link.get("href"):
        href = link["href"].strip()
        if href and href != resp.url:
            canon = canonicalize_url(href)
            if canon and (canon != url):
                try:
                    soup, resp = fetch_and_parse(canon)
                    current_url = resp.url
                except Exception:
                    current_url = resp.url
    else:
        current_url = resp.url

    # Publisher-specific first
    t, d = publisher_specific_title_and_doi(current_url, soup)
    if t: return t, d or find_doi_in_soup(soup)

    # Generic scholarly meta
    for css, attr in SCHOLAR_META_CANDIDATES:
        el = soup.select_one(css)
        if el and el.get(attr):
            tt = clean_text_strip_html(el.get(attr))
            if tt: return tt, find_doi_in_soup(soup)

    # JSON-LD headline
    jl = find_title_in_jsonld(soup)
    if jl:
        jj = clean_text_strip_html(jl)
        if jj: return jj, find_doi_in_soup(soup)

    # <title> / <h1>
    if soup.title and soup.title.string:
        tt = clean_text_strip_html(soup.title.string)
        if tt: return tt, find_doi_in_soup(soup)
    h1 = soup.find("h1")
    if h1:
        tt = clean_text_strip_html(h1.get_text(" "))
        if tt: return tt, find_doi_in_soup(soup)

    # DOI on page → Crossref
    d = find_doi_in_soup(soup)
    if d:
        t2 = try_crossref_title(d)
        if t2:
            return clean_text_strip_html(t2), d

    # Last resort: Crossref freeform by leaf / whole URL
    leaf = (urlparse(current_url).path.split("/") or [""])[-1]
    hint = leaf or current_url
    t3, d3 = crossref_search_title(hint, prefer_domain=urlparse(current_url).netloc.lower())
    if t3:
        return clean_text_strip_html(t3), d3

    return clean_text_strip_html(infer_from_url(current_url) or current_url), None

def infer_from_url(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        q = parse_qs(p.query)
        for key in ("title","headline","paper","article","name"):
            if key in q and q[key]:
                return clean_text_strip_html(unquote(q[key][0]))
        parts = [seg for seg in p.path.split("/") if seg]
        if parts:
            leaf = parts[-1]
            if "." in leaf: leaf = leaf.rsplit(".",1)[0]
            return clean_text_strip_html(unquote(leaf.replace("-"," ").replace("_"," "))).title()
        return (p.netloc or "").split(":")[0]
    except Exception:
        return None

# ---------- Notion upsert with dedupe ----------
def notion_find_existing(db: str, url: str, title: Optional[str], doi: Optional[str]) -> Optional[str]:
    or_filters = [{"property":"URL or Permalink","url":{"equals":url}}]
    if doi:
        or_filters.append({"property":"URL or Permalink","url":{"contains":doi}})
    if title and len(title) <= 200:
        or_filters.append({"property":"Article Name","title":{"equals":title}})
    try:
        r = notion.databases.query(
            database_id=db,
            filter={"or": or_filters},
            page_size=1
        )
        res=r.get("results",[])
        return res[0]["id"] if res else None
    except APIResponseError as e:
        print("[Notion] Query failed:",getattr(e,"message",str(e))); return None

def notion_upsert(db: str, url: str, title: str, shared_by: str, shared_on_iso: str, doi: Optional[str]) -> Optional[str]:
    props={
        "Article Name":{"title":[{"text":{"content":title[:2000]}}]},
        "URL or Permalink":{"url":url},
        "Shared by":{"rich_text":[{"text":{"content":shared_by}}]},
        "Shared on":{"date":{"start":shared_on_iso}},
    }
    try:
        pid = notion_find_existing(db, url, title, doi)
        if pid: return notion.pages.update(page_id=pid,properties=props)["id"]
        else:   return notion.pages.create(parent={"database_id":db},properties=props)["id"]
    except APIResponseError as e:
        print("[Notion] Upsert failed:",getattr(e,"message",str(e))); return None

# ---------- Main ----------
def main():
    slack.auth_test()
    processed=0
    seen_keys=set()  # in-run dedupe (doi or normalized title)

    for m in list_all_messages(SLACK_CHANNEL_ID):
        ts=m.get("ts",time.time())
        iso=iso_from_ts(ts); human=chicago_time_from_ts(ts)
        user=get_user_display(m.get("user") or m.get("bot_id") or "")

        urls = extract_urls(m)
        pdfs = get_pdf_files(m)

        # 1) HTML article links
        for u in urls:
            title, doi = resolve_best_title_and_doi_for_url(u)
            if looks_numericish(title):
                host = urlparse(u).netloc
                title = f"Article on {host}"

            key = (doi or "") or normalize_title_for_key(title)
            if key in seen_keys:
                continue
            seen_keys.add(key)

            notion_upsert(NOTION_DATABASE_ID,u,title,user,iso,doi)
            processed+=1
            print(f"[Upsert] LINK | {u} | {user} | {human} | title={title}")

        # 2) PDFs uploaded → permalink
        if pdfs:
            permalink = pdf_message_permalink(m, SLACK_CHANNEL_ID)
            if permalink:
                fn = (pdfs[0].get("name") or pdfs[0].get("title") or "").strip()
                title = clean_text_strip_html(" ".join(fn.replace("_"," ").replace("-"," ").split())) or "PDF"
                better_title, doi = fetch_pdf_title_via_slack(pdfs[0])
                if better_title: title = clean_text_strip_html(better_title)
                if looks_numericish(title):
                    title = "PDF Article"

                key = (doi or "") or normalize_title_for_key(title)
                if key not in seen_keys:
                    seen_keys.add(key)
                    notion_upsert(NOTION_DATABASE_ID,permalink,title,user,iso,doi)
                    processed+=1
                    print(f"[Upsert] PDF  | {permalink} | {user} | {human} | title={title}")

        # 3) Direct PDF links (in text)
        for u in urls:
            if is_direct_pdf_url(u):
                title, doi = fetch_pdf_title_direct(u)
                if not title: title = "PDF Article"

                key = (doi or "") or normalize_title_for_key(title)
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                notion_upsert(NOTION_DATABASE_ID,u,clean_text_strip_html(title),user,iso,doi)
                processed+=1
                print(f"[Upsert] PDF(URL) | {u} | {user} | {human} | title={title}")

    print(f"Processed {processed} items.")

if __name__=="__main__":
    main()
