"""
Scraper pipeline:
  1. Parse Megaphone RSS → upsert episodes (with mp3 URL)
  2. For each un-scraped episode → download mp3 → transcribe via Groq Whisper
  3. NLP: tokenise, remove stopwords, count frequencies → save to DB
"""
import re, time, logging, os, tempfile, asyncio
import httpx
import feedparser
import nltk
from nltk.corpus import stopwords
from collections import Counter
from groq import Groq
import db

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

nltk.download("stopwords", quiet=True)
nltk.download("punkt", quiet=True)
STOPWORDS = set(stopwords.words("english"))

EXTRA_STOP = {
    "like", "just", "know", "think", "going", "get", "got",
    "would", "could", "said", "say", "actually", "really",
    "right", "yeah", "okay", "well", "thing", "things",
    "lot", "little", "way", "make", "people", "one", "two",
    "also", "back", "even", "still", "want", "come", "ve",
    "ll", "re", "don", "didn", "isn", "wasn", "aren", "doesn",
    "m", "s", "t", "d",
}
STOPWORDS |= EXTRA_STOP

RSS_URL = "https://feeds.megaphone.fm/RSV1597324942"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    )
}

def get_groq_client():
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise ValueError("GROQ_API_KEY environment variable not set")
    return Groq(api_key=api_key)


async def fetch_rss_episodes():
    log.info("Fetching RSS feed...")
    feed = feedparser.parse(RSS_URL)
    saved = 0
    for entry in feed.entries:
        title = entry.get("title", "").strip()
        pub_date = entry.get("published", "")
        description = entry.get("summary", "")[:500]
        mp3_url = None
        for link in entry.get("links", []):
            if link.get("rel") == "enclosure" and "audio" in link.get("type", ""):
                mp3_url = link["href"]
                break
        if mp3_url:
            await db.upsert_episode(title, pub_date, description, mp3_url)
            saved += 1
    log.info(f"RSS: upserted {saved} episodes")


def download_audio(mp3_url: str, max_mb: int = 25):
    try:
        log.info(f"Downloading audio: {mp3_url}")
        with httpx.Client(headers=HEADERS, timeout=120, follow_redirects=True) as client:
            resp = client.get(mp3_url)
        if resp.status_code != 200:
            log.warning(f"Audio download failed: HTTP {resp.status_code}")
            return None
        size_mb = len(resp.content) / (1024 * 1024)
        log.info(f"Downloaded {size_mb:.1f} MB")
        content = resp.content[:max_mb * 1024 * 1024] if size_mb > max_mb else resp.content
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp3")
        tmp.write(content)
        tmp.close()
        return tmp.name
    except Exception as e:
        log.error(f"Download error: {e}")
        return None


def transcribe_audio(file_path: str):
    try:
        client = get_groq_client()
        with open(file_path, "rb") as f:
            result = client.audio.transcriptions.create(
                file=(os.path.basename(file_path), f),
                model="whisper-large-v3",
                response_format="text",
                language="en",
            )
        return result if isinstance(result, str) else result.text
    except Exception as e:
        log.error(f"Groq transcription error: {e}")
        return None
    finally:
        try:
            os.unlink(file_path)
        except Exception:
            pass


def compute_word_freq(text: str) -> dict:
    tokens = re.findall(r"\b[a-z]{3,}\b", text.lower())
    filtered = [t for t in tokens if t not in STOPWORDS]
    return dict(Counter(filtered))


async def run_full_scrape():
    await fetch_rss_episodes()
    episodes = await db.get_unscraped_episodes()
    log.info(f"Episodes to transcribe: {len(episodes)}")

    for ep in episodes:
        ep_id = ep["id"]
        mp3_url = ep["transcript_url"]
        log.info(f"Processing episode {ep_id}")

        audio_path = await asyncio.to_thread(download_audio, mp3_url)
        if not audio_path:
            log.warning(f"Could not download episode {ep_id}, skipping")
            await db.save_transcript(ep_id, "", {})
            await asyncio.sleep(2)
            continue

        text = await asyncio.to_thread(transcribe_audio, audio_path)
        if not text:
            log.warning(f"Could not transcribe episode {ep_id}, skipping")
            await db.save_transcript(ep_id, "", {})
            await asyncio.sleep(2)
            continue

        word_counts = compute_word_freq(text)
        await db.save_transcript(ep_id, text, word_counts)
        log.info(f"  → {len(word_counts)} unique words saved")
        await asyncio.sleep(3)

    log.info("Scrape complete.")
