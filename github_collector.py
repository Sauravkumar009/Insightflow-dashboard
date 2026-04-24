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

# 6 categories per country
CATEGORIES = {
    "10": "Music",
    "20": "Gaming",
    "22": "People & Blogs",
    "24": "Entertainment",
    "25": "News & Politics",
    "28": "Science & Technology",
}

KAFKA_BATCH_SIZE = 50   # Simulate Kafka batch size
KAFKA_DELAY      = 0.05 # Simulate Kafka consumer delay (seconds)

def get_sentiment(text):
    if not text: return "neutral"
    score = analyzer.polarity_scores(str(text))["compound"]
    if score >= 0.05:    return "positive"
    elif score <= -0.05: return "negative"
    else:                return "neutral"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── STAGE 1: Kafka Simulation ─────────────────────────────────
def kafka_produce(rows):
    """
    Simulate Kafka Producer — sends data in batches to a topic.
    In real Kafka: youtube_api → kafka_producer → youtube-trending topic
    Here: we simulate by splitting into batches with delays
    """
    log(f"[KAFKA PRODUCER] Sending {len(rows)} records to topic 'youtube-trending'")
    batches = []
    for i in range(0, len(rows), KAFKA_BATCH_SIZE):
        batch = rows[i:i+KAFKA_BATCH_SIZE]
        batches.append(batch)
        log(f"  [KAFKA] Batch {len(batches)} produced: {len(batch)} records → topic offset {i}")
        time.sleep(KAFKA_DELAY)
    log(f"[KAFKA PRODUCER] {len(batches)} batches sent to topic successfully\n")
    return batches

# ── STAGE 2: PySpark Silver Layer (simulated) ─────────────────
def spark_silver_transform(batch, batch_num):
    """
    Simulate PySpark Silver Layer transformation.
    In real pipeline: PySpark reads from Kafka → transforms → writes to Delta Lake Silver
    Here: we apply same transformations row by row (equivalent logic)

    Silver layer transformations:
    - Cast data types (try_cast equivalent)
    - Calculate derived columns (engagement_rate, like_ratio, etc.)
    - Apply data quality checks (dropna equivalent)
    - Add metadata columns (layer, processed_at)
    """
    processed = []
    nulls_dropped = 0

    for row in batch:
        # Data quality check (equivalent to PySpark dropna)
        if not row.get("title") or not row.get("views"):
            nulls_dropped += 1
            continue

        # Safe cast (equivalent to PySpark try_cast)
        try:
            views    = int(float(row.get("views",    0)))
            likes    = int(float(row.get("likes",    0)))
            comments = int(float(row.get("comments", 0)))
        except (ValueError, TypeError):
            nulls_dropped += 1
            continue

        # Silver layer derived columns
        # (equivalent to PySpark withColumn transformations)
        engagement_rate = round((likes + comments) / (views + 1), 4)
        like_ratio      = round(likes / (likes + 1), 4)
        comment_rate    = round(comments / (views + 1), 6)

        # Composite trending score
        # (equivalent to PySpark UDF)
        max_views = 50_000_000
        trending_score = round(
            min(views / max_views, 1.0)  * 0.4 +
            min(engagement_rate * 100, 1.0) * 0.3 +
            like_ratio                   * 0.2 +
            min(comment_rate * 1000, 1.0)* 0.1,
            4
        )

        # Build Silver layer record
        silver_row = {
            **row,
            "views"          : views,
            "likes"          : likes,
            "comments"       : comments,
            "engagement_rate": engagement_rate,
            "like_ratio"     : like_ratio,
            "comment_rate"   : comment_rate,
            "trending_score" : trending_score,
            "layer"          : "silver",
        }
        processed.append(silver_row)

    log(f"  [SPARK SILVER] Batch {batch_num}: {len(processed)} records transformed "
        f"({nulls_dropped} nulls dropped)")
    return processed

# ── STAGE 3: Insert to Supabase (Gold layer output) ──────────
def insert_to_supabase(rows):
    """Insert processed Silver layer records to Supabase (serving layer)"""
    if not rows:
        return True
    url = f"{SUPABASE_URL}/rest/v1/youtube_live"
    with httpx.Client(timeout=30) as client:
        response = client.post(url, headers=HEADERS, json=rows)
        if response.status_code in [200, 201]:
            return True
        else:
            log(f"Supabase insert error: {response.status_code} — {response.text}")
            return False

def get_total_count():
    url     = f"{SUPABASE_URL}/rest/v1/youtube_live?select=id"
    headers = {**HEADERS, "Prefer": "count=exact", "Range": "0-0"}
    with httpx.Client(timeout=30) as client:
        response      = client.get(url, headers=headers)
        content_range = response.headers.get("content-range", "*/0")
        return content_range.split("/")[-1]

def main():
    log("=" * 55)
    log("  InsightFlow — Big Data Pipeline")
    log("  YouTube API → Kafka → PySpark Silver → Supabase")
    log("=" * 55)
    log(f"Time: {datetime.now(timezone.utc).isoformat()}")
    log(f"Countries: {len(COUNTRIES)} | Categories: {len(CATEGORIES)}")
    log(f"Kafka batch size: {KAFKA_BATCH_SIZE} records\n")

    if not YOUTUBE_API_KEY:
        log("ERROR: YOUTUBE_API_KEY not set!")
        return

    # ── STAGE 0: Data Collection from YouTube API ─────────────
    log("[STAGE 0] Collecting data from YouTube Data API v3...")
    now      = datetime.now(timezone.utc).isoformat()
    youtube  = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    raw_rows = []
    seen_keys= set()

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
                    key        = f"{video_id}_{country_code}"

                    if key in seen_keys:
                        continue
                    seen_keys.add(key)

                    raw_rows.append({
                        "video_id"    : video_id,
                        "title"       : title,
                        "channel"     : snippet.get("channelTitle", ""),
                        "category"    : cat_name,
                        "country"     : country_name,
                        "country_code": country_code,
                        "views"       : statistics.get("viewCount",    0),
                        "likes"       : statistics.get("likeCount",    0),
                        "comments"    : statistics.get("commentCount", 0),
                        "published"   : snippet.get("publishedAt", "")[:10],
                        "sentiment"   : get_sentiment(title),
                        "fetch_time"  : now,
                        "engagement_rate": 0,
                        "like_ratio"     : 0,
                        "comment_rate"   : 0,
                        "trending_score" : 0,
                        "layer"          : "bronze",
                    })
                    country_count += 1

            except Exception as e:
                log(f"  API error {country_name}/{cat_name}: {e}")

        log(f"  Collected {country_count} videos from {country_name}")

    log(f"\n[STAGE 0] Total raw records collected: {len(raw_rows)}")

    if not raw_rows:
        log("No data collected. Exiting.")
        return

    # ── STAGE 1: Kafka Simulation ─────────────────────────────
    log("\n[STAGE 1] Simulating Kafka Streaming...")
    kafka_batches = kafka_produce(raw_rows)

    # ── STAGE 2: PySpark Silver Layer ────────────────────────
    log("[STAGE 2] PySpark Silver Layer Transformation...")
    log("  Applying: dropna → try_cast → withColumn → engagement_rate → trending_score")

    all_silver = []
    for i, batch in enumerate(kafka_batches):
        silver_batch = spark_silver_transform(batch, i+1)
        all_silver.extend(silver_batch)

    log(f"\n[STAGE 2] Silver layer complete: {len(all_silver)} records transformed")
    log(f"  Schema: video_id, title, category, country, views, likes, comments,")
    log(f"          engagement_rate, like_ratio, comment_rate, trending_score, layer")

    # ── STAGE 3: Store in Supabase ────────────────────────────
    log("\n[STAGE 3] Storing Silver layer records in Supabase...")
    inserted = 0
    for i in range(0, len(all_silver), 100):
        batch   = all_silver[i:i+100]
        success = insert_to_supabase(batch)
        if success:
            inserted += len(batch)

    total = get_total_count()
    log(f"\n[STAGE 3] Inserted {inserted} Silver records into Supabase")
    log(f"  Total records in Supabase: {total}")

    log("\n" + "=" * 55)
    log("  Pipeline Complete!")
    log(f"  API → Kafka ({len(kafka_batches)} batches) → PySpark Silver → Supabase")
    log("=" * 55)

if __name__ == "__main__":
    main()