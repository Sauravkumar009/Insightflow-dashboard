import os
import pandas as pd
import requests
from googleapiclient.discovery import build
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ── API Keys — replace with your actual keys ─────────────────
YOUTUBE_API_KEY = "AIzaSyC214FxsGB1myUJFPgrnV3HnNWH46NDsWM"
NEWS_API_KEY    = "11f49ccd07904a7cbaece6fc4d76b952"
# ─────────────────────────────────────────────────────────────

OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

analyzer = SentimentIntensityAnalyzer()

def get_sentiment(text):
    if not text:
        return "neutral"
    score = analyzer.polarity_scores(str(text))["compound"]
    if score >= 0.05:   return "positive"
    elif score <= -0.05: return "negative"
    else:               return "neutral"

# ═══════════════════════════════════════════
# YOUTUBE API — Live trending videos
# ═══════════════════════════════════════════
print("=== Fetching YouTube Live Data ===")

youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

CATEGORIES = {
    "10": "Music",
    "20": "Gaming",
    "22": "People & Blogs",
    "24": "Entertainment",
    "25": "News & Politics",
    "28": "Science & Technology"
}

yt_videos = []
for cat_id, cat_name in CATEGORIES.items():
    print(f"  Fetching: {cat_name}")
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
            yt_videos.append({
                "title"         : title,
                "channel"       : snippet.get("channelTitle", ""),
                "category"      : cat_name,
                "views"         : int(statistics.get("viewCount", 0)),
                "likes"         : int(statistics.get("likeCount", 0)),
                "comments"      : int(statistics.get("commentCount", 0)),
                "published"     : snippet.get("publishedAt", "")[:10],
                "sentiment"     : get_sentiment(title),
                "platform"      : "YouTube",
                "fetch_time"    : datetime.now().strftime("%Y-%m-%d %H:%M")
            })
    except Exception as e:
        print(f"  Error: {e}")

yt_df = pd.DataFrame(yt_videos)
yt_df.to_csv(os.path.join(OUTPUT_DIR, "youtube_api_live.csv"), index=False)
print(f"YouTube live videos fetched: {len(yt_df)}\n")

# ═══════════════════════════════════════════
# NEWSAPI — Live trending news articles
# ═══════════════════════════════════════════
print("=== Fetching NewsAPI Live Data ===")

NEWS_CATEGORIES = ["technology", "entertainment", "sports", "science", "business", "health"]

news_articles = []
for category in NEWS_CATEGORIES:
    print(f"  Fetching: {category}")
    try:
        url = "https://newsapi.org/v2/top-headlines"
        params = {
            "apiKey"  : NEWS_API_KEY,
            "category": category,
            "country" : "us",
            "pageSize": 10
        }
        response = requests.get(url, params=params)
        data     = response.json()

        for article in data.get("articles", []):
            title   = article.get("title", "") or ""
            content = article.get("description", "") or ""
            news_articles.append({
                "title"      : title,
                "source"     : article.get("source", {}).get("name", "Unknown"),
                "category"   : category.title(),
                "url"        : article.get("url", ""),
                "published"  : (article.get("publishedAt", "")[:10]),
                "sentiment"  : get_sentiment(title + " " + content),
                "platform"   : "NewsAPI",
                "fetch_time" : datetime.now().strftime("%Y-%m-%d %H:%M")
            })
    except Exception as e:
        print(f"  Error: {e}")

news_df = pd.DataFrame(news_articles)
news_df.to_csv(os.path.join(OUTPUT_DIR, "news_api_live.csv"), index=False)
print(f"News articles fetched: {len(news_df)}\n")

# ═══════════════════════════════════════════
# COMBINED LIVE SUMMARY
# ═══════════════════════════════════════════
print("=== Live Data Summary ===")
print(f"YouTube videos : {len(yt_df)}")
print(f"News articles  : {len(news_df)}")
print(f"Fetch time     : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
print("\nTop YouTube video:")
if len(yt_df) > 0:
    top = yt_df.nlargest(1, "views").iloc[0]
    print(f"  {top['title']} ({top['category']}) — {int(top['views']):,} views")
print("\nTop News categories:")
if len(news_df) > 0:
    print(f"  {news_df['category'].value_counts().head(3).to_dict()}")

print("\n=== API Fetch Complete ===")
print("Files saved:")
print(f"  outputs/youtube_api_live.csv")
print(f"  outputs/news_api_live.csv")