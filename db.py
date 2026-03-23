"""
Database layer - PostgreSQL via Supabase using asyncpg.
"""
import os
import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL")


async def _conn():
    return await asyncpg.connect(DATABASE_URL)


async def init_db():
    conn = await _conn()
    try:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS episodes (
            id              SERIAL PRIMARY KEY,
            title           TEXT NOT NULL,
            pub_date        TEXT,
            description     TEXT,
            transcript_url  TEXT UNIQUE,
            transcript_text TEXT,
            scraped         INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS word_freq (
            id          SERIAL PRIMARY KEY,
            episode_id  INTEGER NOT NULL REFERENCES episodes(id),
            word        TEXT NOT NULL,
            count       INTEGER NOT NULL,
            UNIQUE(episode_id, word)
        );
        CREATE INDEX IF NOT EXISTS idx_wf_word ON word_freq(word);
        CREATE INDEX IF NOT EXISTS idx_wf_ep   ON word_freq(episode_id);
        """)
    finally:
        await conn.close()


# ── Episodes ──────────────────────────────────────────────────────────────────

async def upsert_episode(title, pub_date, description, transcript_url):
    conn = await _conn()
    try:
        await conn.execute("""
        INSERT INTO episodes (title, pub_date, description, transcript_url)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (transcript_url) DO NOTHING
        """, title, pub_date, description, transcript_url)
    finally:
        await conn.close()


async def get_unscraped_episodes():
    conn = await _conn()
    try:
        rows = await conn.fetch("""
        SELECT id, transcript_url FROM episodes
        WHERE scraped=0 AND transcript_url IS NOT NULL
        """)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def save_transcript(episode_id, text, word_counts: dict):
    conn = await _conn()
    try:
        await conn.execute(
            "UPDATE episodes SET transcript_text=$1, scraped=1 WHERE id=$2",
            text, episode_id
        )
        if word_counts:
            await conn.executemany(
                """INSERT INTO word_freq (episode_id, word, count) VALUES ($1, $2, $3)
                ON CONFLICT (episode_id, word) DO UPDATE SET count=EXCLUDED.count""",
                [(episode_id, w, cnt) for w, cnt in word_counts.items()]
            )
    finally:
        await conn.close()


async def get_episodes():
    conn = await _conn()
    try:
        rows = await conn.fetch("""
        SELECT id, title, pub_date, description, scraped
        FROM episodes ORDER BY pub_date DESC
        """)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def get_episode(episode_id):
    conn = await _conn()
    try:
        row = await conn.fetchrow(
            "SELECT id, title, pub_date, description, scraped FROM episodes WHERE id=$1",
            episode_id
        )
        return dict(row) if row else None
    finally:
        await conn.close()


async def nuke_db():
    conn = await _conn()
    try:
        await conn.execute("""
        DROP TABLE IF EXISTS word_freq;
        DROP TABLE IF EXISTS episodes;
        """)
    finally:
        await conn.close()


# ── Word frequency ────────────────────────────────────────────────────────────

async def global_word_freq(limit=50):
    conn = await _conn()
    try:
        rows = await conn.fetch("""
        SELECT word, SUM(count) AS total
        FROM word_freq
        GROUP BY word
        ORDER BY total DESC
        LIMIT $1
        """, limit)
        return [{"word": r["word"], "count": r["total"]} for r in rows]
    finally:
        await conn.close()


async def episode_word_freq(episode_id, limit=50):
    conn = await _conn()
    try:
        rows = await conn.fetch("""
        SELECT word, count FROM word_freq
        WHERE episode_id=$1
        ORDER BY count DESC
        LIMIT $2
        """, episode_id, limit)
        return [{"word": r["word"], "count": r["count"]} for r in rows]
    finally:
        await conn.close()


async def track_word(word):
    conn = await _conn()
    try:
        rows = await conn.fetch("""
        SELECT e.id, e.title, e.pub_date, COALESCE(wf.count,0) AS count
        FROM episodes e
        LEFT JOIN word_freq wf ON wf.episode_id=e.id AND wf.word=$1
        WHERE e.scraped=1
        ORDER BY e.pub_date ASC
        """, word)
        return [{"episode_id": r["id"], "title": r["title"],
                 "pub_date": r["pub_date"], "count": r["count"]} for r in rows]
    finally:
        await conn.close()


async def reset_scraped():
    conn = await _conn()
    try:
        await conn.execute("UPDATE episodes SET scraped=0")
        await conn.execute("DELETE FROM word_freq")
    finally:
        await conn.close()
