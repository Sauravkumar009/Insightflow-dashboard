import os
import httpx
from googleapiclient.discovery import build
from datetime import datetime, timezone
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ── Config from environment variables ────────────────────────
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "AIzaSyC214FxsGB1myUJFPgrnV3HnNWH46NDsWM")
SUPABASE_URL    = os.environ.get("SUPABASE_URL", "https://oymwlmbqzvmpwihhhirx.supabase.co")
SUPABASE_KEY    = os.environ.get("SUPABASE_KEY", "sb_publishable_SPQGizmtB8uBcBq6A0kfWw_HOvtQWuQ")
# ─────────────────────────────────────────────────────────────

analyzer = SentimentIntensityAnalyzer()

HEADERS = {
    "apikey"       : SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type" : "application/json",
    "Prefer"       : "return=minimal"
}

# 10 major countries
COUNTRIES = {
    "US": "United States",
    "IN": "India",
    "GB": "United Kingdom",
    "CA": "Canada",
    "AU": "Australia",
    "DE": "Germany",
    "FR": "France",
    "JP": "Japan",
    "KR": "South Korea",
    "BR": "Brazil",
}

# 6 categories per country (to stay within API quota)
CATEGORIES = {
    "10": "Music",
    "20": "Gaming",
    "22": "People & Blogs",
    "24": "Entertainment",
    "25": "News & Politics",
    "28": "Science & Technology",
}

def get_sentiment(text):
    if not text: return "neutral"
    score = analyzer.polarity_scores(str(text))["compound"]
    if score >= 0.05:    return "positive"
    elif score <= -0.05: return "negative"
    else:                return "neutral"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def insert_to_supabase(rows):
    if not rows:
        return True
    url = f"{SUPABASE_URL}/rest/v1/youtube_live"
    with httpx.Client(timeout=30) as client:
        response = client.post(url, headers=HEADERS, json=rows)
        if response.status_code in [200, 201]:
            return True
        else:
            log(f"Insert error: {response.status_code} — {response.text}")
            return False

def get_total_count():
    url     = f"{SUPABASE_URL}/rest/v1/youtube_live?select=id"
    headers = {**HEADERS, "Prefer": "count=exact", "Range": "0-0"}
    with httpx.Client(timeout=30) as client:
        response      = client.get(url, headers=headers)
        content_range = response.headers.get("content-range", "*/0")
        return content_range.split("/")[-1]

def main():
    log("=== InsightFlow Multi-Country Collector ===")
    log(f"Time: {datetime.now(timezone.utc).isoformat()}")
    log(f"Countries: {len(COUNTRIES)} | Categories: {len(CATEGORIES)}")

    if not YOUTUBE_API_KEY:
        log("ERROR: YOUTUBE_API_KEY not set in GitHub Secrets!")
        return

    now      = datetime.now(timezone.utc).isoformat()
    youtube  = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    new_rows = []
    seen_keys = set()

    for country_code, country_name in COUNTRIES.items():
        country_count = 0
        for cat_id, cat_name in CATEGORIES.items():
            try:
                response = youtube.videos().list(
                    part="snippet,statistics",
                    chart="mostPopular",
                    regionCode=country_code,
                    videoCategoryId=cat_id,
                    maxResults=5
                ).execute()

                for item in response.get("items", []):
                    snippet    = item.get("snippet", {})
                    statistics = item.get("statistics", {})
                    title      = snippet.get("title", "")
                    video_id   = item["id"]

                    # Avoid exact duplicate video+country combinations
                    key = f"{video_id}_{country_code}"
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)

                    new_rows.append({
                        "video_id"    : video_id,
                        "title"       : title,
                        "channel"     : snippet.get("channelTitle", ""),
                        "category"    : cat_name,
                        "country"     : country_name,
                        "country_code": country_code,
                        "views"       : int(statistics.get("viewCount",    0)),
                        "likes"       : int(statistics.get("likeCount",    0)),
                        "comments"    : int(statistics.get("commentCount", 0)),
                        "published"   : snippet.get("publishedAt", "")[:10],
                        "sentiment"   : get_sentiment(title),
                        "fetch_time"  : now,
                    })
                    country_count += 1

            except Exception as e:
                log(f"  {country_name}/{cat_name} error: {e}")

        log(f"  {country_name}: {country_count} videos")

    log(f"Total unique videos fetched: {len(new_rows)}")

    if new_rows:
        # Insert in batches of 100
        inserted = 0
        for i in range(0, len(new_rows), 100):
            batch   = new_rows[i:i+100]
            success = insert_to_supabase(batch)
            if success:
                inserted += len(batch)

        total = get_total_count()
        log(f"Inserted {inserted} rows. Total in Supabase: {total}")
    else:
        log("No rows to insert!")

    log("=== Done ===")

if __name__ == "__main__":
    main()