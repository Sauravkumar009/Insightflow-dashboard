import os
import httpx
import time
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

CATEGORIES = {
    "10": "Music",
    "20": "Gaming",
    "22": "People & Blogs",
    "24": "Entertainment",
    "25": "News & Politics",
    "28": "Science & Technology",
}

KAFKA_BATCH_SIZE = 50
KAFKA_DELAY      = 0.05

def get_sentiment(text):
    if not text: return "neutral"
    score = analyzer.polarity_scores(str(text))["compound"]
    if score >= 0.05:    return "positive"
    elif score <= -0.05: return "negative"
    else:                return "neutral"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def insert_to_supabase(table, rows):
    if not rows: return True
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    with httpx.Client(timeout=30) as client:
        response = client.post(url, headers=HEADERS, json=rows)
        if response.status_code in [200, 201]:
            return True
        else:
            log(f"Insert error [{table}]: {response.status_code} — {response.text}")
            return False

def get_total_count(table="youtube_live"):
    url     = f"{SUPABASE_URL}/rest/v1/{table}?select=id"
    headers = {**HEADERS, "Prefer": "count=exact", "Range": "0-0"}
    with httpx.Client(timeout=30) as client:
        response      = client.get(url, headers=headers)
        content_range = response.headers.get("content-range", "*/0")
        return content_range.split("/")[-1]

# ══════════════════════════════════════════
# STAGE 1: Kafka Simulation
# ══════════════════════════════════════════
def kafka_produce(rows):
    """
    Simulate Kafka Producer.
    Real Kafka: YouTube API → kafka_producer → 'youtube-trending' topic
    Simulated: split into batches with delays mimicking network streaming
    """
    log(f"[KAFKA PRODUCER] Sending {len(rows)} records to topic 'youtube-trending'")
    batches = []
    for i in range(0, len(rows), KAFKA_BATCH_SIZE):
        batch = rows[i:i+KAFKA_BATCH_SIZE]
        batches.append(batch)
        log(f"  [KAFKA] Batch {len(batches)} produced: "
            f"{len(batch)} records → offset {i}")
        time.sleep(KAFKA_DELAY)
    log(f"[KAFKA PRODUCER] {len(batches)} batches sent successfully\n")
    return batches

# ══════════════════════════════════════════
# STAGE 2: PySpark Silver Layer
# ══════════════════════════════════════════
def spark_silver_transform(batch, batch_num):
    """
    PySpark Silver Layer — cleaning and feature engineering.
    Equivalent PySpark operations:
      .dropna(subset=["title","views"])
      .withColumn("views", try_cast(col("views") as int))
      .withColumn("engagement_rate", (likes+comments)/(views+1))
      .withColumn("like_ratio", likes/(likes+1))
      .withColumn("trending_score", UDF(...))
      .withColumn("layer", lit("silver"))
    """
    processed   = []
    nulls_dropped = 0

    for row in batch:
        # dropna equivalent
        if not row.get("title") or not row.get("views"):
            nulls_dropped += 1
            continue

        # try_cast equivalent
        try:
            views    = int(float(row.get("views",    0)))
            likes    = int(float(row.get("likes",    0)))
            comments = int(float(row.get("comments", 0)))
        except (ValueError, TypeError):
            nulls_dropped += 1
            continue

        # withColumn derived features
        engagement_rate = round((likes + comments) / (views + 1), 4)
        like_ratio      = round(likes / (likes + 1), 4)
        comment_rate    = round(comments / (views + 1), 6)
        trending_score  = round(
            min(views / 50_000_000, 1.0)        * 0.4 +
            min(engagement_rate * 100, 1.0)     * 0.3 +
            like_ratio                           * 0.2 +
            min(comment_rate * 1000, 1.0)        * 0.1, 4)

        processed.append({
            **row,
            "views"          : views,
            "likes"          : likes,
            "comments"       : comments,
            "engagement_rate": engagement_rate,
            "like_ratio"     : like_ratio,
            "comment_rate"   : comment_rate,
            "trending_score" : trending_score,
            "layer"          : "silver",
        })

    log(f"  [SPARK SILVER] Batch {batch_num}: "
        f"{len(processed)} records transformed ({nulls_dropped} nulls dropped)")
    return processed

# ══════════════════════════════════════════
# STAGE 3: PySpark Gold Layer
# ══════════════════════════════════════════
def spark_gold_aggregate(silver_records, now):
    """
    PySpark Gold Layer — aggregations for serving layer.
    Equivalent PySpark operations:
      silver_df.groupBy("category").agg(
          avg("views"), avg("likes"), avg("engagement_rate"),
          avg("trending_score"), count("video_id"))
      silver_df.groupBy("country").agg(...)
      silver_df.groupBy("sentiment").agg(...)
      silver_df.orderBy("trending_score").limit(20)
    """
    log("[SPARK GOLD] Computing aggregations...")

    # groupBy("category").agg(avg, count)
    cat_groups = defaultdict(list)
    for r in silver_records:
        cat_groups[r["category"]].append(r)

    gold_category = []
    for cat, records in cat_groups.items():
        gold_category.append({
            "category"          : cat,
            "avg_views"         : round(sum(r["views"] for r in records) / len(records), 2),
            "avg_likes"         : round(sum(r["likes"] for r in records) / len(records), 2),
            "avg_comments"      : round(sum(r["comments"] for r in records) / len(records), 2),
            "avg_engagement_rate": round(sum(r["engagement_rate"] for r in records) / len(records), 4),
            "avg_trending_score": round(sum(r["trending_score"] for r in records) / len(records), 4),
            "total_videos"      : len(records),
            "fetch_time"        : now,
        })
    log(f"  [SPARK GOLD] category: {len(gold_category)} groups")

    # groupBy("country").agg(avg, count)
    country_groups = defaultdict(list)
    for r in silver_records:
        country_groups[r["country"]].append(r)

    gold_country = []
    for country, records in country_groups.items():
        gold_country.append({
            "country"           : country,
            "avg_views"         : round(sum(r["views"] for r in records) / len(records), 2),
            "avg_likes"         : round(sum(r["likes"] for r in records) / len(records), 2),
            "avg_engagement_rate": round(sum(r["engagement_rate"] for r in records) / len(records), 4),
            "avg_trending_score": round(sum(r["trending_score"] for r in records) / len(records), 4),
            "total_videos"      : len(records),
            "fetch_time"        : now,
        })
    log(f"  [SPARK GOLD] country: {len(gold_country)} groups")

    # groupBy("sentiment").agg(avg, count)
    sent_groups = defaultdict(list)
    for r in silver_records:
        sent_groups[r["sentiment"]].append(r)

    gold_sentiment = []
    for sent, records in sent_groups.items():
        gold_sentiment.append({
            "sentiment"         : sent,
            "avg_views"         : round(sum(r["views"] for r in records) / len(records), 2),
            "avg_engagement_rate": round(sum(r["engagement_rate"] for r in records) / len(records), 4),
            "total_videos"      : len(records),
            "fetch_time"        : now,
        })
    log(f"  [SPARK GOLD] sentiment: {len(gold_sentiment)} groups")

    # orderBy("trending_score").limit(20)
    gold_trending = sorted(silver_records,
                           key=lambda x: x["trending_score"],
                           reverse=True)[:20]
    gold_trending_rows = [{
        "video_id"      : r["video_id"],
        "title"         : r["title"],
        "channel"       : r["channel"],
        "category"      : r["category"],
        "country"       : r["country"],
        "views"         : r["views"],
        "likes"         : r["likes"],
        "engagement_rate": r["engagement_rate"],
        "trending_score": r["trending_score"],
        "sentiment"     : r["sentiment"],
        "fetch_time"    : now,
    } for r in gold_trending]
    log(f"  [SPARK GOLD] top trending: {len(gold_trending_rows)} videos")

    return gold_category, gold_country, gold_sentiment, gold_trending_rows

# ══════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════
def main():
    log("=" * 60)
    log("  InsightFlow — Complete Big Data Pipeline")
    log("  YouTube API → Kafka → PySpark Bronze → Silver → Gold")
    log("=" * 60)
    log(f"Time: {datetime.now(timezone.utc).isoformat()}")
    log(f"Countries: {len(COUNTRIES)} | Categories: {len(CATEGORIES)}")
    log(f"Kafka batch size: {KAFKA_BATCH_SIZE} records\n")

    if not YOUTUBE_API_KEY:
        log("ERROR: YOUTUBE_API_KEY not set!")
        return

    now     = datetime.now(timezone.utc).isoformat()
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    # ── STAGE 0: Data Collection ──────────────────────────────
    log("[STAGE 0] Collecting from YouTube Data API v3...")
    raw_rows  = []
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
                        "layer"       : "bronze",
                    })
                    country_count += 1

            except Exception as e:
                log(f"  API error {country_name}/{cat_name}: {e}")

        log(f"  Collected {country_count} videos from {country_name}")

    log(f"\n[STAGE 0] Bronze records: {len(raw_rows)}\n")

    if not raw_rows:
        log("No data. Exiting.")
        return

    # ── STAGE 1: Kafka Simulation ─────────────────────────────
    log("[STAGE 1] Kafka Streaming Simulation...")
    kafka_batches = kafka_produce(raw_rows)

    # ── STAGE 2: PySpark Silver ───────────────────────────────
    log("[STAGE 2] PySpark Silver Layer Transformation...")
    log("  dropna → try_cast → engagement_rate → like_ratio → trending_score")

    all_silver = []
    for i, batch in enumerate(kafka_batches):
        silver_batch = spark_silver_transform(batch, i+1)
        all_silver.extend(silver_batch)

    log(f"\n[STAGE 2] Silver complete: {len(all_silver)} records")
    log("  Columns: video_id, title, category, country, views, likes,")
    log("           comments, engagement_rate, like_ratio, trending_score\n")

    # ── STAGE 3: PySpark Gold ─────────────────────────────────
    log("[STAGE 3] PySpark Gold Layer Aggregation...")
    log("  groupBy(category).agg(avg_views, avg_engagement, count)")
    log("  groupBy(country).agg(avg_views, avg_engagement, count)")
    log("  groupBy(sentiment).agg(avg_views, avg_engagement, count)")
    log("  orderBy(trending_score).limit(20)")

    gold_cat, gold_country, gold_sent, gold_trending = \
        spark_gold_aggregate(all_silver, now)

    log(f"\n[STAGE 3] Gold complete:")
    log(f"  category table : {len(gold_cat)} rows")
    log(f"  country table  : {len(gold_country)} rows")
    log(f"  sentiment table: {len(gold_sent)} rows")
    log(f"  trending table : {len(gold_trending)} rows\n")

    # ── STAGE 4: Store in Supabase ────────────────────────────
    log("[STAGE 4] Storing all layers in Supabase...")

    # Silver layer → youtube_live table
    silver_inserted = 0
    for i in range(0, len(all_silver), 100):
        batch = all_silver[i:i+100]
        if insert_to_supabase("youtube_live", batch):
            silver_inserted += len(batch)
    log(f"  Silver → youtube_live: {silver_inserted} records")

    # Gold layer → gold tables
    if insert_to_supabase("gold_category",  gold_cat):
        log(f"  Gold → gold_category: {len(gold_cat)} rows")
    if insert_to_supabase("gold_country",   gold_country):
        log(f"  Gold → gold_country: {len(gold_country)} rows")
    if insert_to_supabase("gold_sentiment", gold_sent):
        log(f"  Gold → gold_sentiment: {len(gold_sent)} rows")
    if insert_to_supabase("gold_trending",  gold_trending):
        log(f"  Gold → gold_trending: {len(gold_trending)} rows")

    total = get_total_count("youtube_live")
    log(f"\n  Total Silver records in Supabase: {total}")

    log("\n" + "=" * 60)
    log("  Pipeline Complete!")
    log(f"  Bronze({len(raw_rows)}) → Kafka({len(kafka_batches)} batches)"
        f" → Silver({len(all_silver)}) → Gold(cat:{len(gold_cat)},"
        f"country:{len(gold_country)},sent:{len(gold_sent)},"
        f"trending:{len(gold_trending)})")
    log("=" * 60)

if __name__ == "__main__":
    main()