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

'''
@app.get("/episodes")
def list_episodes():
    return db.get_episodes()
'''
@app.get("/episodes")
def list_episodes():
    with db._conn() as conn:
        with conn.cursor() as c:
            c.execute("""
            SELECT e.id, e.title, e.pub_date, e.description, e.scraped,
                   COUNT(wf.id) as word_count
            FROM episodes e
            LEFT JOIN word_freq wf ON wf.episode_id = e.id
            GROUP BY e.id
            ORDER BY e.pub_date DESC
            """)
            return [dict(r) for r in c.fetchall()]

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

@app.get("/admin/stats")
def stats():
    with db._conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT COUNT(*) as total, SUM(CASE WHEN scraped=1 THEN 1 ELSE 0 END) as done FROM episodes")
            row = c.fetchone()
    return {"total": row["total"], "transcribed": row["done"], "remaining": row["total"] - row["done"]}

@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    return {"ok": True}
