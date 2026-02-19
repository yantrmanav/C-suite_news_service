\# C-Suite Intelligence Service



Two-part system:

\- \*\*Backend\*\* (FastAPI, Python) → Deploy to \*\*Railway\*\* or \*\*Render\*\*

\- \*\*Frontend\*\* (HTML, static) → Deploy to \*\*Vercel\*\*



---



\## 🚀 Deploy Backend to Railway



\### Option A: Railway (recommended, free tier available)



1\. Go to \[railway.app](https://railway.app) → New Project → Deploy from GitHub

2\. Point to the `backend/` folder

3\. Set environment variables in Railway dashboard:

&nbsp;  ```

&nbsp;  SERP\_API\_KEY=your\_brightdata\_token

&nbsp;  GEMINI\_API\_KEY=your\_gemini\_key

&nbsp;  FRONTEND\_URL=https://your-app.vercel.app

&nbsp;  PORT=8000

&nbsp;  ```

4\. Add build command: `pip install -r requirements.txt \&\& playwright install chromium`

5\. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`

6\. Copy the Railway public URL (e.g. `https://csuite.up.railway.app`)



\### Option B: Render (also free tier)



1\. Go to \[render.com](https://render.com) → New Web Service

2\. Connect GitHub repo, root dir = `backend/`

3\. Build command: `pip install -r requirements.txt \&\& playwright install chromium`

4\. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`

5\. Set env vars same as above



---



\## 🌐 Deploy Frontend to Vercel



1\. Go to \[vercel.com](https://vercel.com) → New Project → Import GitHub repo

2\. Set root directory to `frontend/`

3\. Framework preset: \*\*Other\*\* (it's static HTML)

4\. No env vars needed

5\. Deploy → get your Vercel URL



---



\## 🔧 Local Development



```bash

\# Backend

cd backend

pip install -r requirements.txt

playwright install chromium

cp .env.example .env   # add your keys

uvicorn main:app --reload --port 8000



\# Frontend

cd frontend

\# Open index.html in browser OR serve with:

python -m http.server 3000

```



---



\## 📡 API Endpoints



| Method | Endpoint | Description |

|--------|----------|-------------|

| GET | `/` | Health check |

| POST | `/api/fetch` | Start fetch pipeline |

| GET | `/api/status` | Job status + live logs |

| GET | `/api/articles` | Get articles (supports `?limit=200\&category=Tech\&min\_score=50\&search=CEO`) |

| GET | `/api/stats` | Category breakdown + totals |

| DELETE | `/api/articles/{hash}` | Delete one article |

| DELETE | `/api/articles` | Clear all articles |



---



\## ⚙️ Configuration (backend/main.py)



```python

MAX\_DAYS            = 30   # Only articles from last N days

RELEVANCY\_THRESHOLD = 50   # Discard articles below this score

MAX\_BD\_KEYWORDS     = 6    # BrightData keyword queries to run

MAX\_BD\_PER\_KEYWORD  = 4    # Articles per keyword

MAX\_BD\_TOTAL        = 20   # Hard cap

CACHE\_TTL\_DAYS      = 7    # Re-scrape articles after N days

```



---



\## 🔑 .env Template



```

SERP\_API\_KEY=your\_brightdata\_api\_token

GEMINI\_API\_KEY=your\_google\_gemini\_key

FRONTEND\_URL=https://your-app.vercel.app

```

