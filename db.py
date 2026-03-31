"""
Database layer - PostgreSQL via Supabase using psycopg2.
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

DATABASE_URL = os.getenv("DATABASE_URL")


def _conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def init_db():
    with _conn() as conn:
        with conn.cursor() as c:
            c.execute("""
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
        conn.commit()


def upsert_episode(title, pub_date, description, transcript_url):
    with _conn() as conn:
        with conn.cursor() as c:
            c.execute("""
            INSERT INTO episodes (title, pub_date, description, transcript_url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (transcript_url) DO NOTHING
            """, (title, pub_date, description, transcript_url))
        conn.commit()


def get_unscraped_episodes():
    with _conn() as conn:
        with conn.cursor() as c:
            c.execute("""
            SELECT id, transcript_url FROM episodes
            WHERE scraped=0 AND transcript_url IS NOT NULL
            """)
            return [dict(r) for r in c.fetchall()]


def save_transcript(episode_id, text, word_counts: dict):
    with _conn() as conn:
        with conn.cursor() as c:
            c.execute(
                "UPDATE episodes SET transcript_text=%s, scraped=1 WHERE id=%s",
                (text, episode_id)
            )
            if word_counts:
                execute_values(c,
                    """INSERT INTO word_freq (episode_id, word, count) VALUES %s
                    ON CONFLICT (episode_id, word) DO UPDATE SET count=EXCLUDED.count""",
                    [(episode_id, w, cnt) for w, cnt in word_counts.items()]
                )
        conn.commit()


def get_episodes():
    with _conn() as conn:
        with conn.cursor() as c:
            c.execute("""
            SELECT id, title, pub_date, description, scraped
            FROM episodes ORDER BY pub_date DESC
            """)
            return [dict(r) for r in c.fetchall()]


def get_episode(episode_id):
    with _conn() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT id, title, pub_date, description, scraped FROM episodes WHERE id=%s",
                (episode_id,)
            )
            row = c.fetchone()
            return dict(row) if row else None


def nuke_db():
    with _conn() as conn:
        with conn.cursor() as c:
            c.execute("DROP TABLE IF EXISTS word_freq;")
            c.execute("DROP TABLE IF EXISTS episodes;")
        conn.commit()


def reset_scraped():
    with _conn() as conn:
        with conn.cursor() as c:
            c.execute("UPDATE episodes SET scraped=0")
            c.execute("DELETE FROM word_freq")
        conn.commit()


def global_word_freq(limit=50):
    with _conn() as conn:
        with conn.cursor() as c:
            c.execute("""
            SELECT word, SUM(count) AS total
            FROM word_freq
            GROUP BY word
            ORDER BY total DESC
            LIMIT %s
            """, (limit,))
            return [{"word": r["word"], "count": r["total"]} for r in c.fetchall()]


def episode_word_freq(episode_id, limit=50):
    with _conn() as conn:
        with conn.cursor() as c:
            c.execute("""
            SELECT word, count FROM word_freq
            WHERE episode_id=%s
            ORDER BY count DESC
            LIMIT %s
            """, (episode_id, limit))
            return [{"word": r["word"], "count": r["count"]} for r in c.fetchall()]


def track_word(word):
    with _conn() as conn:
        with conn.cursor() as c:
            c.execute("""
            SELECT e.id, e.title, e.pub_date, COALESCE(wf.count,0) AS count
            FROM episodes e
            LEFT JOIN word_freq wf ON wf.episode_id=e.id AND wf.word=%s
            WHERE e.scraped=1
            ORDER BY TO_TIMESTAMP(e.pub_date, 'Dy, DD Mon YYYY HH24:MI:SS OF') ASC
            """, (word,))
            return [{"episode_id": r["id"], "title": r["title"],
                     "pub_date": r["pub_date"], "count": r["count"]} for r in c.fetchall()]
