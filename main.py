from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import db, scraper

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    yield

app = FastAPI(title="Tucker Carlson Word Frequency API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Episodes ─────────────────────────────────────────────────────────────────

@app.get("/episodes")
async def list_episodes():
    return await db.get_episodes()

@app.post("/episodes/fetch")
async def fetch_episodes(background_tasks: BackgroundTasks):
    background_tasks.add_task(scraper.run_full_scrape)
    return {"status": "scrape started"}

@app.get("/episodes/{episode_id}")
async def get_episode(episode_id: int):
    ep = await db.get_episode(episode_id)
    if not ep:
        raise HTTPException(404, "Episode not found")
    return ep

# ── Word frequency ────────────────────────────────────────────────────────────

@app.get("/words/global")
async def global_word_freq(limit: int = 50):
    return await db.global_word_freq(limit)

@app.get("/words/episode/{episode_id}")
async def episode_word_freq(episode_id: int, limit: int = 50):
    return await db.episode_word_freq(episode_id, limit)

@app.get("/words/track")
async def track_word(word: str):
    return await db.track_word(word.lower().strip())

# ── Admin ─────────────────────────────────────────────────────────────────────

@app.post("/admin/reset")
async def reset_scraped():
    await db.reset_scraped()
    return {"status": "reset complete"}

@app.post("/admin/nuke")
async def nuke_db():
    await db.nuke_db()
    await db.init_db()
    return {"status": "database wiped and reinitialised"}

@app.get("/admin/debug-rss")
async def debug_rss():
    import feedparser
    feed = feedparser.parse("https://feeds.megaphone.fm/RSV1597324942")
    return {
        "status": feed.get("status", "no status"),
        "entry_count": len(feed.entries),
        "first_entry_title": feed.entries[0].get("title") if feed.entries else None,
    }

@app.get("/health")
async def health():
    return {"ok": True}
