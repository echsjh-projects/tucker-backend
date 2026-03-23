from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import db, scraper

@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield

app = FastAPI(title="Tucker Carlson Word Frequency API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/episodes")
def list_episodes():
    return db.get_episodes()

@app.post("/episodes/fetch")
def fetch_episodes(background_tasks: BackgroundTasks):
    background_tasks.add_task(scraper.run_full_scrape)
    return {"status": "scrape started"}

@app.get("/episodes/{episode_id}")
def get_episode(episode_id: int):
    ep = db.get_episode(episode_id)
    if not ep:
        raise HTTPException(404, "Episode not found")
    return ep

@app.get("/words/global")
def global_word_freq(limit: int = 50):
    return db.global_word_freq(limit)

@app.get("/words/episode/{episode_id}")
def episode_word_freq(episode_id: int, limit: int = 50):
    return db.episode_word_freq(episode_id, limit)

@app.get("/words/track")
def track_word(word: str):
    return db.track_word(word.lower().strip())

@app.post("/admin/reset")
def reset_scraped():
    db.reset_scraped()
    return {"status": "reset complete"}

@app.post("/admin/nuke")
def nuke_db():
    db.nuke_db()
    db.init_db()
    return {"status": "database wiped and reinitialised"}

@app.get("/admin/debug-rss")
def debug_rss():
    import feedparser
    feed = feedparser.parse("https://feeds.megaphone.fm/RSV1597324942")
    return {
        "status": feed.get("status", "no status"),
        "entry_count": len(feed.entries),
        "first_entry_title": feed.entries[0].get("title") if feed.entries else None,
    }

@app.get("/admin/test-db")
def test_db():
    try:
        conn = db._conn()
        conn.close()
        return {"status": "db connection ok"}
    except Exception as e:
        return {"status": "db connection FAILED", "error": str(e)}

@app.get("/health")
def health():
    return {"ok": True}
