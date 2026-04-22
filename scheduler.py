import os
import schedule
import time
import pandas as pd
import requests
from googleapiclient.discovery import build
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ── API Keys ──────────────────────────────────────────────────
YOUTUBE_API_KEY = "AIzaSyC214FxsGB1myUJFPgrnV3HnNWH46NDsWM"
NEWS_API_KEY    = "11f49ccd07904a7cbaece6fc4d76b952"
# ─────────────────────────────────────────────────────────────

OUTPUT_DIR = "outputs"
YT_FILE    = os.path.join(OUTPUT_DIR, "youtube_api_live.csv")
NEWS_FILE  = os.path.join(OUTPUT_DIR, "news_api_live.csv")
LOG_FILE   = os.path.join(OUTPUT_DIR, "scheduler_log.txt")

analyzer = SentimentIntensityAnalyzer()

def get_sentiment(text):
    if not text: return "neutral"
    score = analyzer.polarity_scores(str(text))["compound"]
    if score >= 0.05:    return "positive"
    elif score <= -0.05: return "negative"
    else:                return "neutral"

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def fetch_and_append():
    log("=== Scheduled fetch started ===")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ── YouTube ───────────────────────────────────────────────
    try:
        youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

        CATEGORIES = {
            "1":"Film & Animation", "2":"Autos & Vehicles",
            "10":"Music", "17":"Sports", "20":"Gaming",
            "22":"People & Blogs", "23":"Comedy",
            "24":"Entertainment", "25":"News & Politics",
            "26":"Howto & Style", "28":"Science & Technology"
        }

        yt_new = []
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
                    yt_new.append({
                        "video_id"  : item["id"],
                        "title"     : title,
                        "channel"   : snippet.get("channelTitle", ""),
                        "category"  : cat_name,
                        "views"     : int(statistics.get("viewCount", 0)),
                        "likes"     : int(statistics.get("likeCount", 0)),
                        "comments"  : int(statistics.get("commentCount", 0)),
                        "published" : snippet.get("publishedAt", "")[:10],
                        "sentiment" : get_sentiment(title),
                        "platform"  : "YouTube",
                        "fetch_time": now
                    })
            except Exception as e:
                log(f"  YouTube category {cat_name} error: {e}")

        yt_df = pd.DataFrame(yt_new)

        # Append to existing file
        if os.path.exists(YT_FILE):
            existing = pd.read_csv(YT_FILE)
            combined = pd.concat([existing, yt_df], ignore_index=True)
            # Keep latest 1000 records to avoid huge files
            combined = combined.tail(1000)
        else:
            combined = yt_df

        combined.to_csv(YT_FILE, index=False)
        log(f"YouTube: {len(yt_df)} new videos fetched. Total: {len(combined)}")

    except Exception as e:
        log(f"YouTube fetch failed: {e}")

    # ── NewsAPI ───────────────────────────────────────────────
    try:
        NEWS_CATEGORIES = ["technology","entertainment","sports",
                           "science","business","health"]
        news_new = []

        for category in NEWS_CATEGORIES:
            try:
                response = requests.get(
                    "https://newsapi.org/v2/top-headlines",
                    params={
                        "apiKey"  : NEWS_API_KEY,
                        "category": category,
                        "country" : "us",
                        "pageSize": 10
                    }
                )
                data = response.json()

                for article in data.get("articles", []):
                    title   = article.get("title", "") or ""
                    content = article.get("description", "") or ""
                    news_new.append({
                        "title"     : title,
                        "source"    : article.get("source", {}).get("name", "Unknown"),
                        "category"  : category.title(),
                        "url"       : article.get("url", ""),
                        "published" : (article.get("publishedAt", "")[:10]),
                        "sentiment" : get_sentiment(title + " " + content),
                        "platform"  : "NewsAPI",
                        "fetch_time": now
                    })
            except Exception as e:
                log(f"  News category {category} error: {e}")

        news_df = pd.DataFrame(news_new)

        if os.path.exists(NEWS_FILE):
            existing_news = pd.read_csv(NEWS_FILE)
            combined_news = pd.concat([existing_news, news_df], ignore_index=True)
            combined_news = combined_news.tail(500)
        else:
            combined_news = news_df

        combined_news.to_csv(NEWS_FILE, index=False)
        log(f"News: {len(news_df)} new articles fetched. Total: {len(combined_news)}")

    except Exception as e:
        log(f"News fetch failed: {e}")

    log("=== Fetch complete ===\n")

# ── Run immediately on start, then every 30 minutes ──────────
print("=" * 50)
print("  Social Media Data Scheduler Started")
print("  Fetching every 30 minutes automatically")
print("  Press Ctrl+C to stop")
print("=" * 50 + "\n")

fetch_and_append()  # Run immediately on start

schedule.every(30).minutes.do(fetch_and_append)

while True:
    schedule.run_pending()
    next_run = schedule.next_run()
    print(f"  Next fetch at: {next_run.strftime('%H:%M:%S')} — waiting...", end="\r")
    time.sleep(60)
