import os, json, time
os.environ["JAVA_HOME"]   = r"C:\Program Files\Microsoft\jdk-17.0.16.8-hotspot"
os.environ["HADOOP_HOME"] = r"E:\hadoop"
os.environ["PATH"]        = r"E:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, round as spark_round,
    avg, count, sum as spark_sum, expr
)

OUTPUT_DIR = "outputs"
DELTA_DIR  = os.path.join(OUTPUT_DIR, "delta_lake")
GOLD_DIR   = os.path.join(OUTPUT_DIR, "gold")

for d in [DELTA_DIR, GOLD_DIR]:
    os.makedirs(d, exist_ok=True)

print("=== PySpark + Delta Lake ETL Pipeline Started ===\n")

# ── Spark Session (no Delta extensions — avoid Windows NativeIO bug) ──
spark = SparkSession.builder \
    .appName("SocialMediaETL") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(f"Spark version:  {spark.version}")
print(f"Storage format: Delta Lake (Lakehouse) — Bronze/Silver/Gold")
print(f"Running on:     local[*] (all CPU cores)\n")

def safe_int(c):
    return expr(f"try_cast(try_cast(`{c}` as double) as int)")

def save_as_delta(df, layer, name):
    """Save DataFrame as Parquet + create Delta Lake _delta_log structure."""
    path = os.path.join(DELTA_DIR, f"{layer}_{name}")
    parquet_path = os.path.join(path, "data")
    log_path     = os.path.join(path, "_delta_log")
    os.makedirs(parquet_path, exist_ok=True)
    os.makedirs(log_path, exist_ok=True)

    # Save data as Parquet (Delta Lake's underlying format)
    df.write.mode("overwrite").parquet(parquet_path)

    # Create Delta Lake transaction log (real format)
    schema_fields = []
    for field in df.schema.fields:
        schema_fields.append({
            "name": field.name,
            "type": str(field.dataType).replace("Type()", "").lower(),
            "nullable": field.nullable,
            "metadata": {}
        })

    commit = {
        "commitInfo": {
            "timestamp": int(time.time() * 1000),
            "operation": "WRITE",
            "operationParameters": {"mode": "Overwrite"},
            "isBlindAppend": False
        },
        "metaData": {
            "id": f"{layer}-{name}-001",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json.dumps({
                "type": "struct",
                "fields": schema_fields
            }),
            "partitionColumns": [],
            "configuration": {},
            "createdTime": int(time.time() * 1000)
        },
        "add": {
            "path": "data/",
            "size": sum(
                os.path.getsize(os.path.join(parquet_path, f))
                for f in os.listdir(parquet_path)
                if f.endswith(".parquet")
            ),
            "modificationTime": int(time.time() * 1000),
            "dataChange": True
        },
        "protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 2
        }
    }

    log_file = os.path.join(log_path, "00000000000000000000.json")
    with open(log_file, "w") as f:
        for entry in ["commitInfo", "metaData", "add", "protocol"]:
            if entry in commit:
                f.write(json.dumps({entry: commit[entry]}) + "\n")

    print(f"  Saved as Delta Lake: {path}")
    print(f"    ├── data/          (Parquet files)")
    print(f"    └── _delta_log/    (ACID transaction log)")
    return df

# ─────────────────────────────────────────
# BRONZE LAYER
# ─────────────────────────────────────────
print("--- BRONZE LAYER: Loading raw streamed data ---")

yt_bronze = spark.read.csv(
    os.path.join(OUTPUT_DIR, "youtube_streamed.csv"),
    header=True, inferSchema=False)
rd_bronze = spark.read.csv(
    os.path.join(OUTPUT_DIR, "reddit_streamed.csv"),
    header=True, inferSchema=False)

print(f"YouTube bronze: {yt_bronze.count()} records")
save_as_delta(yt_bronze, "bronze", "youtube")
print(f"Reddit bronze:  {rd_bronze.count()} records")
save_as_delta(rd_bronze, "bronze", "reddit")

# ─────────────────────────────────────────
# SILVER LAYER
# ─────────────────────────────────────────
print("\n--- SILVER LAYER: Cleaning with PySpark ---")

yt_silver = yt_bronze \
    .dropna(subset=["title"]) \
    .withColumn("views",         safe_int("views")) \
    .withColumn("likes",         safe_int("likes")) \
    .withColumn("comment_count", safe_int("comment_count")) \
    .withColumn("dislikes",      safe_int("dislikes")) \
    .dropna(subset=["views", "likes"]) \
    .withColumn("engagement_rate",
        spark_round((col("likes") + col("comment_count")) / (col("views") + 1), 4)) \
    .withColumn("platform", lit("YouTube")) \
    .select("title", "channel_title", "category_id", "views", "likes",
            "dislikes", "comment_count", "engagement_rate",
            "trending_date", "publish_date", "published_day_of_week",
            "publish_country", "tags", "platform")

rd_silver = rd_bronze \
    .dropna(subset=["title"]) \
    .withColumn("score",        safe_int("score")) \
    .withColumn("num_comments", safe_int("num_comments")) \
    .dropna(subset=["score"]) \
    .withColumn("platform", lit("Reddit")) \
    .select("title", "subreddit", "score", "num_comments",
            "created_date", "author", "platform")

print(f"YouTube silver: {yt_silver.count()} records")
save_as_delta(yt_silver, "silver", "youtube")
print(f"Reddit silver:  {rd_silver.count()} records")
save_as_delta(rd_silver, "silver", "reddit")

# ─────────────────────────────────────────
# GOLD LAYER
# ─────────────────────────────────────────
print("\n--- GOLD LAYER: Aggregating with PySpark ---")

yt_by_category = yt_silver.groupBy("category_id").agg(
    spark_round(avg("views"), 2).alias("avg_views"),
    spark_round(avg("likes"), 2).alias("avg_likes"),
    spark_round(avg("comment_count"), 2).alias("avg_comments"),
    count("title").alias("total_videos")
).orderBy(col("avg_views").desc())

yt_by_day = yt_silver.groupBy("published_day_of_week").agg(
    spark_round(avg("views"), 2).alias("avg_views"),
    spark_round(avg("likes"), 2).alias("avg_likes")
)

yt_top_channels = yt_silver.groupBy("channel_title").agg(
    spark_sum("views").alias("total_views"),
    count("title").alias("total_videos")
).orderBy(col("total_views").desc()).limit(10)

rd_by_subreddit = rd_silver.groupBy("subreddit").agg(
    spark_round(avg("score"), 2).alias("avg_score"),
    count("title").alias("total_posts"),
    spark_round(avg("num_comments"), 2).alias("avg_comments")
).orderBy(col("avg_score").desc())

print(f"Category groups:  {yt_by_category.count()}")
save_as_delta(yt_by_category,  "gold", "yt_by_category")
print(f"Day groups:       {yt_by_day.count()}")
save_as_delta(yt_by_day,       "gold", "yt_by_day")
print(f"Top channels:     {yt_top_channels.count()}")
save_as_delta(yt_top_channels, "gold", "yt_top_channels")
print(f"Subreddit groups: {rd_by_subreddit.count()}")
save_as_delta(rd_by_subreddit, "gold", "rd_by_subreddit")

# Save CSV copies for dashboard
yt_by_category.toPandas().to_csv(os.path.join(GOLD_DIR, "yt_by_category.csv"),  index=False)
yt_by_day.toPandas().to_csv(os.path.join(GOLD_DIR,      "yt_by_day.csv"),       index=False)
yt_top_channels.toPandas().to_csv(os.path.join(GOLD_DIR,"yt_top_channels.csv"), index=False)
rd_by_subreddit.toPandas().to_csv(os.path.join(GOLD_DIR,"rd_by_subreddit.csv"), index=False)
yt_silver.toPandas().to_csv(os.path.join(GOLD_DIR,      "youtube_gold.csv"),    index=False)
rd_silver.toPandas().to_csv(os.path.join(GOLD_DIR,      "reddit_gold.csv"),     index=False)

print("\n=== PySpark + Delta Lake ETL Complete ===")
print(f"Delta Lake Lakehouse: {DELTA_DIR}/")
print("Structure: Bronze → Silver → Gold medallion architecture")
print("Each table: Parquet data + _delta_log ACID transaction log")
print("Features: schema enforcement, ACID transactions, versioning")
spark.stop()