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

# ── Episodes ────────────────────────────────────────────────────────────────

@app.get("/episodes")
def list_episodes():
    """Return all episodes ordered by publish date desc."""
    return db.get_episodes()

@app.post("/episodes/fetch")
def fetch_episodes(background_tasks: BackgroundTasks):
    """Trigger RSS + transcript scrape in background."""
    background_tasks.add_task(scraper.run_full_scrape)
    return {"status": "scrape started"}

@app.get("/episodes/{episode_id}")
def get_episode(episode_id: int):
    ep = db.get_episode(episode_id)
    if not ep:
        raise HTTPException(404, "Episode not found")
    return ep

# ── Word frequency ───────────────────────────────────────────────────────────

@app.get("/words/global")
def global_word_freq(limit: int = 50):
    """Top N words across ALL episodes."""
    return db.global_word_freq(limit)

@app.get("/words/episode/{episode_id}")
def episode_word_freq(episode_id: int, limit: int = 50):
    """Top N words for a single episode."""
    return db.episode_word_freq(episode_id, limit)

@app.get("/words/track")
def track_word(word: str):
    """Frequency of a specific word across every episode (timeline)."""
    return db.track_word(word.lower().strip())

@app.post("/admin/reset")
def reset_scraped():
    """Mark all episodes as un-scraped so they get re-processed."""
    with db._conn() as c:
        c.execute("UPDATE episodes SET scraped=0")
        c.execute("DELETE FROM word_freq")
    return {"status": "reset complete"}

@app.post("/admin/nuke")
def nuke_db():
    """Wipe all data and reinitialise — use when schema needs a clean start."""
    with db._conn() as c:
        c.executescript("""
        DROP TABLE IF EXISTS word_freq;
        DROP TABLE IF EXISTS episodes;
        """)
    db.init_db()
    return {"status": "database wiped and reinitialised"}

@app.get("/health")
def health():
    return {"ok": True}
