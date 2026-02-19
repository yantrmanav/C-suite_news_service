"""
C-Suite Intelligence Engine — FastAPI Backend
Deploy to: Railway / Render / Fly.io (NOT Vercel — needs persistent process)
"""

import os, re, json, hashlib, sqlite3, requests, urllib.parse, feedparser, asyncio
from datetime         import datetime, timedelta
from pathlib          import Path
from contextlib       import asynccontextmanager
from bs4              import BeautifulSoup
from dotenv           import load_dotenv
from fastapi          import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import google.generativeai as genai

load_dotenv()

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
BRIGHTDATA_API_KEY  = os.getenv("SERP_API_KEY", "")
GEMINI_API_KEY      = os.getenv("GEMINI_API_KEY", "")
FRONTEND_URL        = os.getenv("FRONTEND_URL", "*")   # set to your Vercel URL in prod

ZONE                = "serp"
MAX_DAYS            = 30
RELEVANCY_THRESHOLD = 50
MAX_BD_KEYWORDS     = 6
MAX_BD_PER_KEYWORD  = 4
MAX_BD_TOTAL        = 20
CACHE_DB            = "csuite_cache.db"
CACHE_TTL_DAYS      = 7

genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel("gemini-2.0-flash")

# Global job state
job_state = {
    "running":   False,
    "progress":  [],
    "last_run":  None,
    "stats":     {},
}

# ══════════════════════════════════════════════════════════════
# CATEGORIES & KEYWORDS
# ══════════════════════════════════════════════════════════════
VALID_CATEGORIES = [
    "C-Suite", "Funding", "M&A", "Tech", "Startup",
    "HR", "Market", "Business", "Regulation", "Industrialist", "General"
]

KEYWORDS = [
    "CEO CFO CTO appointed resigned promoted fired 2026",
    "tech startup funding Series A B C raised million 2026",
    "AI enterprise strategy Microsoft Google Meta Apple Amazon 2026",
    "Fortune 500 merger acquisition deal announced billion 2026",
    "unicorn IPO public listing valuation startup 2026",
    "corporate layoffs workforce restructuring downsizing 2026",
    "venture capital investment round VC funding 2026",
    "industrialist billionaire business move Tata Adani Ambani 2026",
    "antitrust regulation SEC SEBI investigation penalty 2026",
    "data center semiconductor chip expansion partnership 2026",
]

RSS_FEEDS = {
    "Reuters Business":        "https://feeds.reuters.com/reuters/businessNews",
    "Bloomberg Markets":       "https://feeds.bloomberg.com/markets/news.rss",
    "Financial Times":         "https://www.ft.com/rss/home",
    "WSJ Business":            "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",
    "CNBC Business":           "https://www.cnbc.com/id/10001147/device/rss/rss.html",
    "Forbes Business":         "https://www.forbes.com/business/feed/",
    "Fortune":                 "https://fortune.com/feed/",
    "Harvard Business Review": "https://feeds.hbr.org/harvardbusiness",
    "Mint Markets":            "https://www.livemint.com/rss/markets",
    "Economic Times Markets":  "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "Economic Times HR":       "https://economictimes.indiatimes.com/hr/rssfeeds/1373380680.cms",
    "HR Katha":                "https://www.hrkatha.com/feed/",
    "People Matters":          "https://www.peoplematters.in/rss/rss.xml",
    "CXO Lanes":               "https://cxolanes.com/feed/",
    "CXO Drive":               "https://cxodrive.com/feed/",
    "CEO World":               "https://ceoworld.biz/feed/",
    "YourStory":               "https://yourstory.com/feed",
    "Startupedia":             "https://startupedia.in/feed/",
    "TechCrunch":              "https://techcrunch.com/feed/",
    "VentureBeat":             "https://feeds.feedburner.com/venturebeat/SZYF",
    "Inc42":                   "https://inc42.com/feed/",
    "Entrackr":                "https://entrackr.com/feed/",
    "The Verge":               "https://www.theverge.com/rss/index.xml",
    "Wired Business":          "https://www.wired.com/feed/category/business/latest/rss",
    "PR Newswire":             "https://www.prnewswire.com/rss/business.xml",
    "GlobeNewswire":           "https://www.globenewswire.com/RssFeed/industry/9000/Business%20Technology",
}

PLAYWRIGHT_DOMAINS = [
    "bloomberg.com", "ft.com", "wsj.com", "economist.com",
    "businessinsider.com", "seekingalpha.com", "barrons.com",
    "fortune.com", "hbr.org",
]

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

CONTENT_SELECTORS = [
    "article", "div[itemprop='articleBody']", "div.article-body",
    "div.article__body", "div.post-content", "div.entry-content",
    "div.story-body", "div.story__body", "div.content-body",
    "div[class*='ArticleBody']", "div[class*='article-body']",
    "div[class*='story-body']", "div[class*='main-content']",
    "section[class*='article']", "main",
]

# ══════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════
def init_db():
    conn = sqlite3.connect(CACHE_DB)
    conn.execute("""CREATE TABLE IF NOT EXISTS articles (
        url_hash        TEXT PRIMARY KEY,
        url             TEXT UNIQUE,
        title           TEXT,
        source          TEXT,
        category        TEXT,
        summary         TEXT,
        companies       TEXT,
        key_people      TEXT,
        key_numbers     TEXT,
        relevancy_score INTEGER,
        content         TEXT,
        image_url       TEXT,
        published_at    TEXT,
        fetched_at      TEXT,
        content_hash    TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS scrape_cache (
        url_hash    TEXT PRIMARY KEY,
        content     TEXT,
        image_url   TEXT,
        published_at TEXT,
        scraped_at  TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS analysis_cache (
        url_hash        TEXT PRIMARY KEY,
        summary         TEXT,
        category        TEXT,
        companies       TEXT,
        key_people      TEXT,
        key_numbers     TEXT,
        relevancy_score INTEGER,
        analyzed_at     TEXT
    )""")
    conn.commit()
    conn.close()

def get_conn():
    return sqlite3.connect(CACHE_DB, check_same_thread=False)

def hash_url(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()

def hash_content(title: str, content: str) -> str:
    """Semantic dedup: hash based on title + first 200 chars of content."""
    key = (title.lower().strip()[:100] + content[:200]).encode()
    return hashlib.sha256(key).hexdigest()

# Scrape cache
def scrape_cache_get(uid: str):
    conn = get_conn()
    row = conn.execute(
        "SELECT content, image_url, published_at, scraped_at FROM scrape_cache WHERE url_hash=?",
        (uid,)
    ).fetchone()
    conn.close()
    if not row: return None
    if (datetime.now() - datetime.fromisoformat(row[3])).days > CACHE_TTL_DAYS: return None
    return {"content": row[0], "image_url": row[1], "published_at": row[2]}

def scrape_cache_set(uid: str, url: str, data: dict):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO scrape_cache (url_hash,content,image_url,published_at,scraped_at) VALUES (?,?,?,?,?)",
        (uid, data["content"], data["image_url"], data["published_at"], datetime.now().isoformat())
    )
    conn.commit(); conn.close()

# Analysis cache
def analysis_cache_get(uid: str):
    conn = get_conn()
    row = conn.execute(
        "SELECT summary,category,companies,key_people,key_numbers,relevancy_score FROM analysis_cache WHERE url_hash=?",
        (uid,)
    ).fetchone()
    conn.close()
    if not row: return None
    return {
        "summary": row[0], "category": row[1],
        "companies": json.loads(row[2] or "[]"),
        "key_people": json.loads(row[3] or "[]"),
        "key_numbers": json.loads(row[4] or "[]"),
        "relevancy_score": row[5],
    }

def analysis_cache_set(uid: str, data: dict):
    conn = get_conn()
    conn.execute(
        """INSERT OR REPLACE INTO analysis_cache
           (url_hash,summary,category,companies,key_people,key_numbers,relevancy_score,analyzed_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (uid, data.get("summary",""), data.get("category","General"),
         json.dumps(data.get("companies",[])), json.dumps(data.get("key_people",[])),
         json.dumps(data.get("key_numbers",[])), int(data.get("relevancy_score",0)),
         datetime.now().isoformat())
    )
    conn.commit(); conn.close()

# Article store
def article_exists(url_hash: str) -> bool:
    conn = get_conn()
    row = conn.execute("SELECT 1 FROM articles WHERE url_hash=?", (url_hash,)).fetchone()
    conn.close()
    return bool(row)

def content_duplicate_exists(content_hash: str) -> bool:
    conn = get_conn()
    row = conn.execute("SELECT 1 FROM articles WHERE content_hash=?", (content_hash,)).fetchone()
    conn.close()
    return bool(row)

def save_article(article: dict):
    conn = get_conn()
    conn.execute(
        """INSERT OR REPLACE INTO articles
           (url_hash,url,title,source,category,summary,companies,key_people,key_numbers,
            relevancy_score,content,image_url,published_at,fetched_at,content_hash)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            article["url_hash"], article["url"], article["title"], article["source"],
            article["category"], article["summary"],
            json.dumps(article.get("companies",[])), json.dumps(article.get("key_people",[])),
            json.dumps(article.get("key_numbers",[])), article["relevancy_score"],
            article["content"], article["image_url"],
            article["published_at"], article["fetched_at"], article["content_hash"],
        )
    )
    conn.commit(); conn.close()

def get_articles(limit=200, category=None, min_score=0, search=None) -> list:
    conn = get_conn()
    q = "SELECT * FROM articles WHERE relevancy_score >= ?"
    params = [min_score]
    if category and category != "All":
        q += " AND category=?"; params.append(category)
    if search:
        q += " AND (title LIKE ? OR summary LIKE ? OR companies LIKE ? OR key_people LIKE ?)"
        s = f"%{search}%"
        params += [s, s, s, s]
    q += " ORDER BY relevancy_score DESC, fetched_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(q, params).fetchall()
    conn.close()

    cols = ["url_hash","url","title","source","category","summary","companies",
            "key_people","key_numbers","relevancy_score","content","image_url",
            "published_at","fetched_at","content_hash"]
    result = []
    for row in rows:
        a = dict(zip(cols, row))
        for f in ["companies","key_people","key_numbers"]:
            try: a[f] = json.loads(a[f] or "[]")
            except: a[f] = []
        result.append(a)
    return result

def get_stats() -> dict:
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    avg   = conn.execute("SELECT AVG(relevancy_score) FROM articles").fetchone()[0] or 0
    cats  = conn.execute("SELECT category, COUNT(*) FROM articles GROUP BY category ORDER BY COUNT(*) DESC").fetchall()
    conn.close()
    return {"total": total, "avg_score": round(avg, 1), "by_category": dict(cats)}

# ══════════════════════════════════════════════════════════════
# SCRAPING UTILS
# ══════════════════════════════════════════════════════════════
def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()

def needs_playwright(url: str) -> bool:
    return any(d in url.lower() for d in PLAYWRIGHT_DOMAINS)

def is_recent_struct(ds) -> bool:
    if not ds: return True
    try: return (datetime.now() - datetime(*ds[:6])) <= timedelta(days=MAX_DAYS)
    except: return True

def is_recent_text(text: str) -> bool:
    if not text: return True
    t = text.lower().strip()
    try:
        if any(w in t for w in ["just now","minute","second"]): return True
        if "hour"  in t: return int(re.findall(r'\d+', t)[0]) <= MAX_DAYS * 24
        if "day"   in t: return int(re.findall(r'\d+', t)[0]) <= MAX_DAYS
        if "week"  in t: return int(re.findall(r'\d+', t)[0]) * 7 <= MAX_DAYS
        if "month" in t: return int(re.findall(r'\d+', t)[0]) <= 1
        for fmt in ["%b %d, %Y","%B %d, %Y","%Y-%m-%d","%d %b %Y"]:
            try: return (datetime.now()-datetime.strptime(text.strip()[:20],fmt)).days <= MAX_DAYS
            except: continue
    except: pass
    return True

def _parse_soup(soup) -> dict:
    r = {"content": "", "image_url": "", "published_at": ""}
    for an, av in [("property","article:published_time"),("name","article:published_time"),
                    ("property","og:article:published_time"),("name","datePublished"),
                    ("itemprop","datePublished"),("name","DC.date"),("name","pubdate")]:
        tag = soup.find("meta", {an: av})
        if tag and tag.get("content"):
            r["published_at"] = tag["content"].strip(); break
    if not r["published_at"]:
        t = soup.find("time")
        if t: r["published_at"] = t.get("datetime","") or t.get_text(strip=True)
    for pn, pv in [("property","og:image"),("name","twitter:image"),("property","og:image:secure_url")]:
        tag = soup.find("meta", {pn: pv})
        if tag and tag.get("content"):
            r["image_url"] = tag["content"].strip(); break
    if not r["image_url"]:
        img = soup.find("img", src=True)
        if img and str(img["src"]).startswith("http"): r["image_url"] = img["src"]
    for tag in soup(["script","style","nav","header","footer","aside","figure","noscript","form","iframe","button","svg","picture"]):
        tag.decompose()
    content = ""
    for sel in CONTENT_SELECTORS:
        node = soup.select_one(sel)
        if node:
            paras = [clean_text(p.get_text()) for p in node.find_all("p") if len(p.get_text(strip=True)) > 40]
            content = " ".join(paras)
            if len(content) > 300: break
    if len(content) < 300:
        paras = [clean_text(p.get_text()) for p in soup.find_all("p") if len(p.get_text(strip=True)) > 40]
        content = " ".join(paras)
    if len(content) < 100:
        for prop in [("name","description"),("property","og:description")]:
            tag = soup.find("meta", {prop[0]: prop[1]})
            if tag and tag.get("content"): content = tag["content"].strip(); break
    r["content"] = content[:6000]
    return r

def scrape_requests(url: str) -> dict:
    try:
        resp = requests.get(url, headers=BROWSER_HEADERS, timeout=20, allow_redirects=True)
        if resp.status_code != 200: return {"content":"","image_url":"","published_at":""}
        return _parse_soup(BeautifulSoup(resp.text,"html.parser"))
    except Exception as e:
        return {"content":"","image_url":"","published_at":""}

def scrape_playwright(url: str) -> dict:
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        return {"content":"","image_url":"","published_at":""}
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent=BROWSER_HEADERS["User-Agent"], viewport={"width":1280,"height":900})
            page = ctx.new_page()
            page.route("**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,mp4,mp3}", lambda r: r.abort())
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(2500)
            except PWTimeout: pass
            html = page.content()
            browser.close()
            soup = BeautifulSoup(html,"html.parser")
            result = _parse_soup(soup)
            if len(result["content"]) < 200:
                result["content"] = clean_text(soup.get_text(separator=" "))[:6000]
            return result
    except Exception as e:
        return {"content":"","image_url":"","published_at":""}

def scrape_article(url: str, log) -> dict:
    uid = hash_url(url)
    cached = scrape_cache_get(uid)
    if cached:
        log(f"  [Cache] {url[:60]}")
        return cached
    log(f"  [Scrape] {url[:70]}")
    if needs_playwright(url):
        data = scrape_playwright(url)
    else:
        data = scrape_requests(url)
        if not data.get("content"):
            data = scrape_playwright(url)
    scrape_cache_set(uid, url, data)
    return data

# ══════════════════════════════════════════════════════════════
# GEMINI — FEW-SHOT PROMPTED ANALYSIS
# ══════════════════════════════════════════════════════════════
ANALYSIS_PROMPT = """You are a Chief Intelligence Officer briefing a room of Fortune 500 CEOs, CTOs, CFOs, and senior investors. Your job is to extract maximum signal from news, score relevance ruthlessly, and write summaries that command attention.

=== FEW-SHOT EXAMPLES ===

EXAMPLE 1 — C-Suite / High Score
Title: "OpenAI CFO Sarah Friar resigns after 8 months, COO Brad Lightcap to oversee finance"
Content: "Sarah Friar, who joined OpenAI as its first CFO in early 2024, has resigned after just 8 months..."
Output:
{{
  "summary": "OpenAI CFO Sarah Friar has resigned after only 8 months, with COO Brad Lightcap absorbing finance oversight at a critical period ahead of the company's anticipated $150B valuation fundraise. The abrupt departure signals potential internal tension at the world's most valuable AI company and may spook institutional investors watching for leadership stability.",
  "category": "C-Suite",
  "companies": ["OpenAI"],
  "key_people": ["Sarah Friar - CFO (resigned)", "Brad Lightcap - COO"],
  "key_numbers": ["$150B valuation", "8 months tenure"],
  "relevancy_score": 95
}}

EXAMPLE 2 — Funding / High Score
Title: "Anthropic raises $2.5B Series E led by Google, Amazon deepens commitment"
Content: "AI safety startup Anthropic has closed a $2.5 billion Series E funding round..."
Output:
{{
  "summary": "Anthropic secured $2.5B in Series E funding co-led by Google and Amazon, pushing its total raised past $7B and positioning it as the primary enterprise AI alternative to OpenAI. This signals Big Tech's strategy of funding multiple AI horses rather than betting on a single platform, intensifying competitive pressure across the entire AI stack.",
  "category": "Funding",
  "companies": ["Anthropic", "Google", "Amazon"],
  "key_people": ["Dario Amodei - CEO"],
  "key_numbers": ["$2.5B Series E", "$7B+ total raised"],
  "relevancy_score": 92
}}

EXAMPLE 3 — Low Relevance / Discard
Title: "10 tips for work-life balance in 2026"
Content: "Experts suggest taking breaks, staying hydrated..."
Output:
{{
  "summary": "Generic productivity advice with no specific company actions, executive moves, or financial events of note.",
  "category": "General",
  "companies": [],
  "key_people": [],
  "key_numbers": [],
  "relevancy_score": 12
}}

EXAMPLE 4 — M&A
Title: "Adobe acquires Figma competitor Penpot for $800M to rebuild design dominance"
Content: "Adobe has agreed to acquire open-source design platform Penpot for $800 million..."
Output:
{{
  "summary": "Adobe is acquiring Penpot for $800M, its most significant design-space bet since the blocked $20B Figma deal, targeting enterprise open-source design adoption which grew 300% in 2025. The move reframes Adobe's competitive positioning against Figma and signals aggressive M&A after 18 months of regulatory caution.",
  "category": "M&A",
  "companies": ["Adobe", "Penpot"],
  "key_people": ["Shantanu Narayen - CEO, Adobe"],
  "key_numbers": ["$800M acquisition", "300% growth in 2025"],
  "relevancy_score": 88
}}

=== YOUR TASK ===
Analyze the article below. Return ONLY a valid JSON object — no markdown, no explanation.

Scoring rubric (be strict — most news is below 50):
- 90-100: CEO/board-level exit or hire at major company; deal > $500M; market-moving regulatory action
- 75-89:  Funding > $50M; notable M&A; major product launch by tech giant; significant layoffs (>1000)
- 55-74:  Startup news with named figures; sector regulation; industry partnerships with financial details
- 35-54:  General business trends with named companies but low strategic impact
- 15-34:  Industry commentary, opinion pieces, trend analysis without specific events
- 0-14:   Lifestyle, general advice, entertainment, irrelevant topics

Categories (pick most specific):
C-Suite | Funding | M&A | Tech | Startup | HR | Market | Business | Regulation | Industrialist | General

Title: {title}
Content: {content}

JSON:"""

def analyze_gemini(title: str, content: str, uid: str, log) -> dict:
    cached = analysis_cache_get(uid)
    if cached:
        log(f"  [Analysis Cache] {title[:50]}")
        return cached
    source = content[:4000] if len(content) > 200 else f"[Headline only] {title}"
    prompt = ANALYSIS_PROMPT.format(title=title, content=source)
    try:
        response = gemini_model.generate_content(prompt)
        if not response or not response.text: raise ValueError("Empty")
        raw = response.text.strip()
        raw = re.sub(r'^```(?:json)?\s*','',raw,flags=re.MULTILINE)
        raw = re.sub(r'\s*```$','',raw,flags=re.MULTILINE).strip()
        s, e = raw.find('{'), raw.rfind('}')+1
        if s == -1: raise ValueError("No JSON")
        result = json.loads(raw[s:e])
        if result.get("category") not in VALID_CATEGORIES: result["category"] = "General"
        result["relevancy_score"] = max(0, min(100, int(result.get("relevancy_score",0))))
        analysis_cache_set(uid, result)
        return result
    except Exception as ex:
        log(f"  [Gemini Error] {ex}")
        return {"summary":"","category":_fallback_classify(title,content),
                "companies":[],"key_people":[],"key_numbers":[],
                "relevancy_score":_fallback_score(title,content)}

def _fallback_classify(title: str, content: str) -> str:
    text = (title+" "+content).lower()
    if any(w in text for w in ["ceo","cfo","cto","cmo","chief","appointed","resign","promoted","c-suite"]): return "C-Suite"
    if any(w in text for w in ["merger","acquisition","acquired","takeover","buyout"]): return "M&A"
    if any(w in text for w in ["series a","series b","funding round","raised $","ipo","venture capital","unicorn"]): return "Funding"
    if any(w in text for w in ["layoff","laid off","job cut","workforce reduction"]): return "HR"
    if any(w in text for w in ["oil","stocks","earnings","nasdaq","sensex","nifty","dow","s&p","inflation"]): return "Market"
    if any(w in text for w in ["startup","founder","accelerator","incubator"]): return "Startup"
    if any(w in text for w in ["ai ","artificial intelligence","semiconductor","chip","cloud","data center","saas"]): return "Tech"
    if any(w in text for w in ["antitrust","sec ","sebi","ftc","doj","investigation","lawsuit"]): return "Regulation"
    if any(w in text for w in ["ambani","adani","musk","bezos","gates","tata","birla","billionaire"]): return "Industrialist"
    if any(w in text for w in ["restructuring","partnership","strategy","expansion"]): return "Business"
    return "General"

def _fallback_score(title: str, content: str) -> int:
    text = (title+" "+content).lower()
    s = 20
    if any(w in text for w in ["ceo","cfo","cto","chief"]): s += 25
    if any(w in text for w in ["billion"]): s += 20
    if any(w in text for w in ["million","funded","acquired"]): s += 12
    if any(w in text for w in ["google","microsoft","apple","meta","amazon","nvidia","tata","reliance"]): s += 15
    if any(w in text for w in ["resign","fired","appoint"]): s += 15
    return min(s, 90)

# ══════════════════════════════════════════════════════════════
# BRIGHTDATA
# ══════════════════════════════════════════════════════════════
def fetch_brightdata(query: str, limit: int, log) -> list:
    try:
        resp = requests.get(
            "https://api.brightdata.com/serp",
            headers={"Authorization": f"Bearer {BRIGHTDATA_API_KEY}"},
            params={"engine":"google_news","q":query,"gl":"us","hl":"en","num":str(limit*2)},
            timeout=60,
        )
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("news_results") or data.get("organic_results") or []
            out = []
            for item in results[:limit]:
                title = item.get("title","").strip()
                url   = item.get("link") or item.get("url","")
                pub   = item.get("date") or item.get("published_date","")
                if not title or not url or len(title) < 15: continue
                if not is_recent_text(pub): continue
                out.append({"id":hash_url(url),"title":title,"url":url,
                            "source":"Google News","published_at_raw":pub})
            if out: return out
    except Exception as e:
        log(f"  [BrightData SERP Error] {e}")
    return _bd_html_fallback(query, limit, log)

def _bd_html_fallback(query: str, limit: int, log) -> list:
    try:
        encoded = urllib.parse.quote_plus(query)
        gn_url  = f"https://news.google.com/search?q={encoded}&hl=en&gl=US&ceid=US:en"
        resp = requests.post(
            "https://api.brightdata.com/request",
            headers={"Content-Type":"application/json","Authorization":f"Bearer {BRIGHTDATA_API_KEY}"},
            json={"zone":"serp","url":gn_url,"format":"raw"},
            timeout=60,
        )
        if resp.status_code != 200: return []
        soup = BeautifulSoup(resp.text,"html.parser")
        articles = []
        for block_sel, title_sel in [("article","h3, h4"),("div.SoaBEf","div.MBeuO"),("div[data-hveid]","h3")]:
            for block in soup.select(block_sel)[:limit]:
                t = block.select_one(title_sel)
                a = block.select_one("a[href]")
                if not t or not a: continue
                title_text = t.get_text(strip=True)
                href = a.get("href","")
                if not href or len(title_text) < 15: continue
                if href.startswith("/"): href = "https://news.google.com" + href
                try:
                    r = requests.get(href, headers={"User-Agent":BROWSER_HEADERS["User-Agent"]}, timeout=10, allow_redirects=True)
                    real_url = r.url
                except: real_url = href
                articles.append({"id":hash_url(real_url),"title":title_text,"url":real_url,"source":"Google News","published_at_raw":""})
            if articles: break
        return articles[:limit]
    except Exception as e:
        log(f"  [BrightData HTML Error] {e}"); return []

def fetch_rss(log) -> list:
    all_articles = []
    for source_name, feed_url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(feed_url)
            count = 0
            for entry in feed.entries:
                if not is_recent_struct(entry.get("published_parsed")): continue
                url = getattr(entry,"link","").strip()
                if not url: continue
                pub = getattr(entry,"published","") or getattr(entry,"updated","")
                all_articles.append({
                    "id": hash_url(url),
                    "title": getattr(entry,"title","").strip(),
                    "url": url, "source": source_name, "published_at_raw": pub,
                })
                count += 1
            if count: log(f"  [RSS] {source_name}: {count}")
        except Exception as e:
            log(f"  [RSS Error] {source_name}: {e}")
    return all_articles

# ══════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════
def run_pipeline():
    global job_state
    job_state["running"] = True
    job_state["progress"] = []
    accepted = 0
    discarded = 0
    duplicates = 0

    def log(msg: str):
        print(msg)
        job_state["progress"].append({"time": datetime.now().isoformat(), "msg": msg})

    try:
        log("═" * 50)
        log("  C-SUITE INTELLIGENCE ENGINE — Starting")
        log("═" * 50)

        raw_pool = []
        seen_ids = set()

        # RSS
        log("\n▶ [1/3] Fetching RSS feeds...")
        rss = fetch_rss(log)
        raw_pool.extend(rss)
        log(f"  Total RSS: {len(rss)}")

        # BrightData
        log(f"\n▶ [2/3] BrightData Google News (max {MAX_BD_TOTAL})...")
        bd_total = 0
        per_kw = max(1, MAX_BD_TOTAL // MAX_BD_KEYWORDS)
        for keyword in KEYWORDS[:MAX_BD_KEYWORDS]:
            remaining = MAX_BD_TOTAL - bd_total
            if remaining <= 0: break
            limit = min(per_kw, remaining, MAX_BD_PER_KEYWORD)
            results = fetch_brightdata(keyword, limit, log)
            bd_total += len(results)
            raw_pool.extend(results)
            log(f"  '{keyword[:45]}' → {len(results)}")
        log(f"  BrightData total: {bd_total}")

        # Process
        log(f"\n▶ [3/3] Processing {len(raw_pool)} candidates...")
        for item in raw_pool:
            uid = item["id"]
            if uid in seen_ids: continue
            seen_ids.add(uid)

            title = (item.get("title") or "").strip()
            url   = item.get("url","")
            if not title or not url: continue

            log(f"\n  ◆ {title[:75]}")

            # Scrape
            scraped      = scrape_article(url, log)
            content      = scraped["content"]
            image_url    = scraped["image_url"]
            published_at = scraped["published_at"] or item.get("published_at_raw","")

            # Deduplication — URL level
            if article_exists(uid):
                log(f"     → SKIP: URL already in DB")
                duplicates += 1
                continue

            # Deduplication — Content level (catches same story from diff sources)
            c_hash = hash_content(title, content)
            if content_duplicate_exists(c_hash):
                log(f"     → SKIP: Duplicate content detected")
                duplicates += 1
                continue

            # Gemini
            analysis = analyze_gemini(title, content, uid, log)
            score    = analysis.get("relevancy_score", 0)

            if score < RELEVANCY_THRESHOLD:
                log(f"     → DISCARD (score={score})")
                discarded += 1
                continue

            log(f"     → ACCEPT [{analysis.get('category')}] score={score} ✓")

            save_article({
                "url_hash": uid, "url": url, "title": title,
                "source": item.get("source",""),
                "category": analysis.get("category","General"),
                "summary": analysis.get("summary",""),
                "companies": analysis.get("companies",[]),
                "key_people": analysis.get("key_people",[]),
                "key_numbers": analysis.get("key_numbers",[]),
                "relevancy_score": score,
                "content": content[:1500],
                "image_url": image_url,
                "published_at": published_at,
                "fetched_at": datetime.now().isoformat(),
                "content_hash": c_hash,
            })
            accepted += 1

        stats = get_stats()
        stats.update({"accepted": accepted, "discarded": discarded, "duplicates": duplicates})
        job_state["stats"]    = stats
        job_state["last_run"] = datetime.now().isoformat()

        log(f"\n{'═'*50}")
        log(f"  ✅ Accepted  : {accepted}")
        log(f"  ❌ Discarded : {discarded}")
        log(f"  🔁 Duplicates: {duplicates}")
        log(f"  📊 DB Total  : {stats['total']}")
        log(f"{'═'*50}")

    except Exception as e:
        log(f"[PIPELINE ERROR] {e}")
        import traceback; log(traceback.format_exc())
    finally:
        job_state["running"] = False

# ══════════════════════════════════════════════════════════════
# FASTAPI APP
# ══════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(title="C-Suite Intelligence API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Serve frontend static files ────────────────────────────────
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

@app.get("/")
def root():
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return {"status": "ok", "service": "C-Suite Intelligence Engine"}

@app.post("/api/fetch")
def trigger_fetch(background_tasks: BackgroundTasks):
    if job_state["running"]:
        return JSONResponse({"error": "Job already running"}, status_code=409)
    background_tasks.add_task(run_pipeline)
    return {"status": "started"}

@app.get("/api/status")
def get_status():
    return {
        "running":  job_state["running"],
        "last_run": job_state["last_run"],
        "stats":    job_state["stats"],
        "log":      job_state["progress"][-50:],  # last 50 lines
    }

@app.get("/api/articles")
def api_articles(limit: int = 200, category: str = None, min_score: int = 0, search: str = None):
    return get_articles(limit=limit, category=category, min_score=min_score, search=search)

@app.get("/api/stats")
def api_stats():
    return get_stats()

@app.delete("/api/articles/{url_hash}")
def delete_article(url_hash: str):
    conn = get_conn()
    conn.execute("DELETE FROM articles WHERE url_hash=?", (url_hash,))
    conn.commit(); conn.close()
    return {"status": "deleted"}

@app.delete("/api/articles")
def clear_all():
    conn = get_conn()
    conn.execute("DELETE FROM articles")
    conn.commit(); conn.close()
    return {"status": "cleared"}