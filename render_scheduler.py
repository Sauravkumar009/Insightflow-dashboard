import os
import schedule
import time
import httpx
from googleapiclient.discovery import build
from datetime import datetime, timezone
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ── Config from environment variables ────────────────────────
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
SUPABASE_URL    = os.environ.get("SUPABASE_URL", "https://oymwlmbqzvmpwihhhirx.supabase.co")
SUPABASE_KEY    = os.environ.get("SUPABASE_KEY", "sb_publishable_SPQGizmtB8uBcBq6A0kfWw_HOvtQWuQ")
FETCH_INTERVAL  = int(os.environ.get("FETCH_INTERVAL", "30"))
# ─────────────────────────────────────────────────────────────

analyzer = SentimentIntensityAnalyzer()

HEADERS = {
    "apikey"       : SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type" : "application/json",
    "Prefer"       : "return=minimal"
}

CATEGORIES = {
    "1" : "Film & Animation",
    "2" : "Autos & Vehicles",
    "10": "Music",
    "17": "Sports",
    "20": "Gaming",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "28": "Science & Technology",
}

def get_sentiment(text):
    if not text: return "neutral"
    score = analyzer.polarity_scores(str(text))["compound"]
    if score >= 0.05:    return "positive"
    elif score <= -0.05: return "negative"
    else:                return "neutral"

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

def insert_to_supabase(rows):
    url = f"{SUPABASE_URL}/rest/v1/youtube_live"
    with httpx.Client(timeout=30) as client:
        response = client.post(url, headers=HEADERS, json=rows)
        if response.status_code in [200, 201]:
            return True
        else:
            log(f"Supabase insert error: {response.status_code} — {response.text}")
            return False

def get_total_count():
    url = f"{SUPABASE_URL}/rest/v1/youtube_live?select=id"
    headers = {**HEADERS, "Prefer": "count=exact", "Range": "0-0"}
    with httpx.Client(timeout=30) as client:
        response = client.get(url, headers=headers)
        content_range = response.headers.get("content-range", "*/0")
        return content_range.split("/")[-1]

def fetch_and_store():
    log("=== Fetch started ===")
    now = datetime.now(timezone.utc).isoformat()

    try:
        youtube  = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
        new_rows = []

        for cat_id, cat_name in CATEGORIES.items():
            try:
                response = youtube.videos().list(
                    part="snippet,statistics",
                    chart="mostPopular",
                    regionCode="US",
                    videoCategoryId=cat_id,
                    maxResults=10
                ).execute()

                for item in response.get("items", []):
                    snippet    = item.get("snippet", {})
                    statistics = item.get("statistics", {})
                    title      = snippet.get("title", "")
                    new_rows.append({
                        "video_id"  : item["id"],
                        "title"     : title,
                        "channel"   : snippet.get("channelTitle", ""),
                        "category"  : cat_name,
                        "views"     : int(statistics.get("viewCount",    0)),
                        "likes"     : int(statistics.get("likeCount",    0)),
                        "comments"  : int(statistics.get("commentCount", 0)),
                        "published" : snippet.get("publishedAt", "")[:10],
                        "sentiment" : get_sentiment(title),
                        "fetch_time": now,
                    })
            except Exception as e:
                log(f"Category {cat_name} error: {e}")

        if new_rows:
            success = insert_to_supabase(new_rows)
            if success:
                total = get_total_count()
                log(f"Inserted {len(new_rows)} videos. Total in DB: {total}")
        else:
            log("No rows fetched")

    except Exception as e:
        log(f"Fetch failed: {e}")

    log("=== Fetch complete ===\n")

# ── Main ──────────────────────────────────────────────────────
log("InsightFlow Scheduler starting on Render...")
log(f"Fetch interval: {FETCH_INTERVAL} minutes")
log(f"Supabase URL: {SUPABASE_URL}")

fetch_and_store()
schedule.every(FETCH_INTERVAL).minutes.do(fetch_and_store)

while True:
    schedule.run_pending()
    time.sleep(60)
