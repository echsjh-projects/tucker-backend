# Tucker Carlson Word Frequency — Backend

FastAPI backend that scrapes Tucker Carlson Show transcripts from HappyScribe,
runs NLP word-frequency analysis, and exposes a REST API for the React frontend.

## Tech stack
- **FastAPI** — REST API
- **feedparser** — parses the Megaphone RSS feed
- **httpx + BeautifulSoup** — scrapes HappyScribe transcript pages
- **nltk** — stopword removal and tokenisation
- **SQLite** — persists episodes and word counts
- **Render.com** — free-tier hosting with a persistent disk

---

## Deploy to Render (cloud-only, no local setup needed)

### Step 1 — Fork / create the GitHub repo
1. Go to [github.com](https://github.com) → **New repository**
2. Name it `tucker-backend`, set it to **Public** (required for Render free tier)
3. Click **Create repository**
4. Use **"creating a new file"** to add each file below into the repo root

Files to create:
- `main.py`
- `db.py`
- `scraper.py`
- `requirements.txt`
- `render.yaml`

### Step 2 — Deploy on Render
1. Go to [render.com](https://render.com) → sign up with GitHub
2. Click **New → Web Service**
3. Connect your `tucker-backend` GitHub repo
4. Render will auto-detect `render.yaml` and configure everything
5. Click **Create Web Service**
6. Wait ~3 minutes for the first deploy

Your API will be live at:
```
https://tucker-backend.onrender.com
```

### Step 3 — Trigger the initial scrape
Once deployed, open your Render URL and call:
```
POST https://tucker-backend.onrender.com/episodes/fetch
```
You can do this from a browser tab using a free tool like [reqbin.com](https://reqbin.com)
or just open:
```
https://tucker-backend.onrender.com/docs
```
The `/docs` page is Swagger UI — click **POST /episodes/fetch → Try it out → Execute**.

The scrape runs in the background (expect 10–30 minutes for all episodes).

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/episodes` | List all episodes |
| POST | `/episodes/fetch` | Trigger scrape (background) |
| GET | `/words/global?limit=50` | Top N words across all episodes |
| GET | `/words/episode/{id}?limit=50` | Top N words for one episode |
| GET | `/words/track?word=ukraine` | Word frequency across episode timeline |
| GET | `/health` | Health check |

---

## Notes on HappyScribe scraping
HappyScribe transcript URLs are built from episode title slugs:
```
https://podcasts.happyscribe.com/the-tucker-carlson-show/{episode-slug}
```
If a transcript page returns no text (HappyScribe may restructure their HTML),
the episode is marked `scraped=1` with empty text so the scraper doesn't retry it.
You can inspect failed episodes via the Render logs.

## Local development (if you later get Python)
```bash
pip install -r requirements.txt
uvicorn main:app --reload
# visit http://localhost:8000/docs
```
