"""
SQLite database layer.
Schema:
  episodes  (id, title, pub_date, description, transcript_url, transcript_text, scraped)
  word_freq (id, episode_id, word, count)
"""
import sqlite3, os

DB_PATH = os.getenv("DB_PATH", "tucker.db")


def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _conn() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS episodes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            title           TEXT NOT NULL,
            pub_date        TEXT,
            description     TEXT,
            transcript_url  TEXT UNIQUE,
            transcript_text TEXT,
            scraped         INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS word_freq (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            episode_id  INTEGER NOT NULL REFERENCES episodes(id),
            word        TEXT NOT NULL,
            count       INTEGER NOT NULL,
            UNIQUE(episode_id, word)
        );
        CREATE INDEX IF NOT EXISTS idx_wf_word ON word_freq(word);
        CREATE INDEX IF NOT EXISTS idx_wf_ep   ON word_freq(episode_id);
        """)


# ── Episodes ─────────────────────────────────────────────────────────────────

def upsert_episode(title, pub_date, description, transcript_url):
    with _conn() as c:
        c.execute("""
        INSERT INTO episodes (title, pub_date, description, transcript_url)
        VALUES (?,?,?,?)
        ON CONFLICT(transcript_url) DO UPDATE SET
            title=excluded.title,
            pub_date=excluded.pub_date,
            description=excluded.description
        """, (title, pub_date, description, transcript_url))


def get_unscraped_episodes():
    with _conn() as c:
        rows = c.execute(
            "SELECT id, transcript_url FROM episodes WHERE scraped=0 AND transcript_url IS NOT NULL"
        ).fetchall()
    return [dict(r) for r in rows]


def save_transcript(episode_id, text, word_counts: dict):
    with _conn() as c:
        c.execute(
            "UPDATE episodes SET transcript_text=?, scraped=1 WHERE id=?",
            (text, episode_id)
        )
        c.executemany(
            "INSERT OR REPLACE INTO word_freq (episode_id, word, count) VALUES (?,?,?)",
            [(episode_id, w, cnt) for w, cnt in word_counts.items()]
        )


def get_episodes():
    with _conn() as c:
        rows = c.execute(
            "SELECT id, title, pub_date, description, scraped FROM episodes ORDER BY pub_date DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_episode(episode_id):
    with _conn() as c:
        row = c.execute(
            "SELECT id, title, pub_date, description, scraped FROM episodes WHERE id=?",
            (episode_id,)
        ).fetchone()
    return dict(row) if row else None


# ── Word frequency ────────────────────────────────────────────────────────────

def global_word_freq(limit=50):
    with _conn() as c:
        rows = c.execute("""
        SELECT word, SUM(count) AS total
        FROM word_freq
        GROUP BY word
        ORDER BY total DESC
        LIMIT ?
        """, (limit,)).fetchall()
    return [{"word": r["word"], "count": r["total"]} for r in rows]


def episode_word_freq(episode_id, limit=50):
    with _conn() as c:
        rows = c.execute("""
        SELECT word, count FROM word_freq
        WHERE episode_id=?
        ORDER BY count DESC
        LIMIT ?
        """, (episode_id, limit)).fetchall()
    return [{"word": r["word"], "count": r["count"]} for r in rows]


def track_word(word):
    with _conn() as c:
        rows = c.execute("""
        SELECT e.id, e.title, e.pub_date, COALESCE(wf.count,0) AS count
        FROM episodes e
        LEFT JOIN word_freq wf ON wf.episode_id=e.id AND wf.word=?
        WHERE e.scraped=1
        ORDER BY e.pub_date ASC
        """, (word,)).fetchall()
    return [{"episode_id": r["id"], "title": r["title"],
             "pub_date": r["pub_date"], "count": r["count"]} for r in rows]
