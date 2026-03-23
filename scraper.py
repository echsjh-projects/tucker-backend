"""
Scraper pipeline:
  1. Parse Megaphone RSS → upsert episodes
  2. For each un-scraped episode → fetch HappyScribe transcript
  3. NLP: tokenise, remove stopwords, count frequencies → save to DB
"""
import re, time, logging
import httpx
from bs4 import BeautifulSoup
import feedparser
import nltk
from nltk.corpus import stopwords
from collections import Counter
import db

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── NLTK setup ────────────────────────────────────────────────────────────────
# Downloaded once; Render persists /tmp between deploys unless ephemeral FS
nltk.download("stopwords", quiet=True)
nltk.download("punkt", quiet=True)
STOPWORDS = set(stopwords.words("english"))

# Extra domain-specific filler words
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
HAPPYSCRIBE_BASE = "https://podcasts.happyscribe.com/the-tucker-carlson-show"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    )
}


# ── Step 1: RSS ───────────────────────────────────────────────────────────────

def fetch_rss_episodes():
    log.info("Fetching RSS feed...")
    feed = feedparser.parse(RSS_URL)
    saved = 0
    for entry in feed.entries:
        title = entry.get("title", "").strip()
        pub_date = entry.get("published", "")
        description = entry.get("summary", "")[:500]
        # Build HappyScribe URL from slug
        slug = _slugify(title)
        transcript_url = f"{HAPPYSCRIBE_BASE}/{slug}" if slug else None
        if transcript_url:
            db.upsert_episode(title, pub_date, description, transcript_url)
            saved += 1
    log.info(f"RSS: upserted {saved} episodes")


def _slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text


# ── Step 2 & 3: Transcript scrape + NLP ──────────────────────────────────────

def scrape_transcript(url: str) -> str | None:
    """Fetch a HappyScribe transcript page and return clean text."""
    try:
        with httpx.Client(headers=HEADERS, timeout=20, follow_redirects=True) as client:
            resp = client.get(url)
        if resp.status_code != 200:
            log.warning(f"HTTP {resp.status_code} for {url}")
            return None
        soup = BeautifulSoup(resp.text, "html.parser")

        # HappyScribe wraps transcript text in .transcript-text or similar
        # Try multiple selectors in order of specificity
        for selector in [
            ".transcript-text",
            "[class*='transcript']",
            "article",
            "main",
        ]:
            container = soup.select_one(selector)
            if container:
                text = container.get_text(separator=" ", strip=True)
                if len(text) > 500:
                    return text

        # Fallback: all paragraph text
        paras = soup.find_all("p")
        text = " ".join(p.get_text(" ", strip=True) for p in paras)
        return text if len(text) > 500 else None

    except Exception as e:
        log.error(f"Scrape error for {url}: {e}")
        return None


def compute_word_freq(text: str) -> dict:
    """Tokenise, strip stopwords, return word→count dict."""
    tokens = re.findall(r"\b[a-z]{3,}\b", text.lower())
    filtered = [t for t in tokens if t not in STOPWORDS]
    return dict(Counter(filtered))


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_full_scrape():
    fetch_rss_episodes()
    episodes = db.get_unscraped_episodes()
    log.info(f"Episodes to scrape: {len(episodes)}")

    for ep in episodes:
        ep_id = ep["id"]
        url = ep["transcript_url"]
        log.info(f"Scraping episode {ep_id}: {url}")

        text = scrape_transcript(url)
        if not text:
            log.warning(f"No transcript for episode {ep_id}, skipping")
            # Mark as attempted so we don't retry forever
            db.save_transcript(ep_id, "", {})
            time.sleep(1)
            continue

        word_counts = compute_word_freq(text)
        db.save_transcript(ep_id, text, word_counts)
        log.info(f"  → {len(word_counts)} unique words saved")
        time.sleep(1.5)   # polite crawl delay

    log.info("Scrape complete.")
