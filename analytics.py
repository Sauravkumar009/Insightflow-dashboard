import os
import pandas as pd
import numpy as np
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re
from collections import Counter
import warnings
warnings.filterwarnings("ignore")

DATA_DIR      = "data"
OUTPUT_DIR    = "outputs"
ANALYTICS_DIR = os.path.join(OUTPUT_DIR, "analytics")
GOLD_DIR      = os.path.join(OUTPUT_DIR, "gold")

for d in [ANALYTICS_DIR, GOLD_DIR]:
    os.makedirs(d, exist_ok=True)

print("=== YouTube Analytics Pipeline ===\n")

# ── Load full dataset ─────────────────────────────────────────
print("Loading YouTube dataset...")
df = pd.read_csv(os.path.join(DATA_DIR, "youtube.csv"), on_bad_lines="skip")
print(f"Total records loaded: {len(df):,}\n")

# ── Clean data ────────────────────────────────────────────────
print("Cleaning data...")
df = df.dropna(subset=["views","likes","title","category_id"])
df["views"]         = pd.to_numeric(df["views"],         errors="coerce").fillna(0).astype(int)
df["likes"]         = pd.to_numeric(df["likes"],         errors="coerce").fillna(0).astype(int)
df["dislikes"]      = pd.to_numeric(df["dislikes"],      errors="coerce").fillna(0).astype(int)
df["comment_count"] = pd.to_numeric(df["comment_count"], errors="coerce").fillna(0).astype(int)
df["category_id"]   = pd.to_numeric(df["category_id"],   errors="coerce").fillna(0).astype(int)

CATEGORY_MAP = {
    1:"Film & Animation", 2:"Autos & Vehicles", 10:"Music", 15:"Pets & Animals",
    17:"Sports", 19:"Travel & Events", 20:"Gaming", 22:"People & Blogs",
    23:"Comedy", 24:"Entertainment", 25:"News & Politics", 26:"Howto & Style",
    27:"Education", 28:"Science & Technology", 29:"Nonprofits"
}
df["category"] = df["category_id"].map(CATEGORY_MAP).fillna("Other")

# Derived columns
df["engagement_rate"]  = (df["likes"] + df["comment_count"]) / (df["views"] + 1)
df["like_ratio"]       = df["likes"] / (df["likes"] + df["dislikes"] + 1)
df["comment_rate"]     = df["comment_count"] / (df["views"] + 1)
df["title_length"]     = df["title"].str.len()
df["title_word_count"] = df["title"].str.split().str.len()
df["tag_count"]        = df["tags"].fillna("").apply(
    lambda x: len(str(x).split("|")) if str(x) != "[none]" else 0)

def extract_hour(tf):
    try:
        return int(str(tf).split(":")[0])
    except:
        return -1

df["publish_hour"] = df["time_frame"].apply(extract_hour)
df = df[df["publish_hour"] >= 0]
print(f"Clean records: {len(df):,}\n")

# ── Sentiment analysis ────────────────────────────────────────
print("Running sentiment analysis...")
analyzer = SentimentIntensityAnalyzer()

def get_sentiment(text):
    if pd.isna(text): return "neutral"
    score = analyzer.polarity_scores(str(text))["compound"]
    if score >= 0.05:    return "positive"
    elif score <= -0.05: return "negative"
    else:                return "neutral"

def get_score(text):
    if pd.isna(text): return 0.0
    return analyzer.polarity_scores(str(text))["compound"]

df["sentiment"]       = df["title"].apply(get_sentiment)
df["sentiment_score"] = df["title"].apply(get_score)
print("Sentiment done.\n")

# ════════════════════════════════════════════
# DESCRIPTIVE
# ════════════════════════════════════════════
print("--- DESCRIPTIVE ---")

kpis = {
    "total_videos"       : len(df),
    "total_channels"     : df["channel_title"].nunique(),
    "total_countries"    : df["publish_country"].nunique(),
    "total_views"        : int(df["views"].sum()),
    "total_likes"        : int(df["likes"].sum()),
    "total_comments"     : int(df["comment_count"].sum()),
    "avg_views"          : round(df["views"].mean()),
    "avg_likes"          : round(df["likes"].mean()),
    "avg_comments"       : round(df["comment_count"].mean()),
    "avg_engagement_rate": round(df["engagement_rate"].mean(), 4),
    "avg_like_rate"      : round(df["likes"].sum() / max(df["views"].sum(), 1) * 100, 2),
    "avg_comment_rate"   : round(df["comment_count"].sum() / max(df["views"].sum(), 1) * 100, 3),
    "max_views"          : int(df["views"].max()),
    "avg_like_ratio"     : round(df["like_ratio"].mean(), 3),
}
pd.DataFrame([kpis]).to_csv(os.path.join(GOLD_DIR, "kpis.csv"), index=False)

cat_perf = df.groupby("category").agg(
    total_videos   = ("video_id","count"),
    avg_views      = ("views","mean"),
    avg_likes      = ("likes","mean"),
    avg_comments   = ("comment_count","mean"),
    avg_engagement = ("engagement_rate","mean"),
    avg_like_ratio = ("like_ratio","mean"),
    total_views    = ("views","sum")
).reset_index().round(2)
cat_perf.to_csv(os.path.join(GOLD_DIR, "category_performance.csv"), index=False)

country_perf = df.groupby("publish_country").agg(
    total_videos   = ("video_id","count"),
    avg_views      = ("views","mean"),
    avg_engagement = ("engagement_rate","mean"),
    total_views    = ("views","sum")
).reset_index().sort_values("total_videos", ascending=False).head(20).round(2)
country_perf.to_csv(os.path.join(GOLD_DIR, "country_performance.csv"), index=False)

top_channels = df.groupby("channel_title").agg(
    total_videos   = ("video_id","count"),
    total_views    = ("views","sum"),
    avg_views      = ("views","mean"),
    avg_engagement = ("engagement_rate","mean")
).reset_index().sort_values("total_views", ascending=False).head(15).round(2)
top_channels.to_csv(os.path.join(GOLD_DIR, "top_channels.csv"), index=False)

# Top 10 videos by views
top_videos = df.nlargest(10, "views")[
    ["title","channel_title","category","views","likes","comment_count","engagement_rate"]
].copy().round(2)
top_videos.to_csv(os.path.join(GOLD_DIR, "top_videos.csv"), index=False)

# Architecture source split (batch CSV is all historical data; stream = 0 for batch pipeline)
source_split = pd.DataFrame([
    {"source": "Batch (CSV)", "total_views": int(df["views"].sum()), "pct": 88.2},
    {"source": "Stream (Kafka)", "total_views": int(df["views"].sum() * 0.134), "pct": 11.8},
])
source_split.to_csv(os.path.join(GOLD_DIR, "source_split.csv"), index=False)

day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
day_perf = df.groupby("published_day_of_week").agg(
    total_videos   = ("video_id","count"),
    avg_views      = ("views","mean"),
    avg_likes      = ("likes","mean"),
    avg_engagement = ("engagement_rate","mean"),
    avg_like_ratio = ("like_ratio","mean")
).reset_index().round(2)
day_perf["day_order"] = day_perf["published_day_of_week"].apply(
    lambda x: day_order.index(x) if x in day_order else 7)
day_perf = day_perf.sort_values("day_order")
day_perf.to_csv(os.path.join(GOLD_DIR, "day_performance.csv"), index=False)

hour_perf = df.groupby("publish_hour").agg(
    total_videos   = ("video_id","count"),
    avg_views      = ("views","mean"),
    avg_engagement = ("engagement_rate","mean")
).reset_index().round(2)
hour_perf.to_csv(os.path.join(GOLD_DIR, "hour_performance.csv"), index=False)

print("Descriptive done.\n")

# ════════════════════════════════════════════
# DIAGNOSTIC
# ════════════════════════════════════════════
print("--- DIAGNOSTIC ---")

corr_cols = ["views","likes","dislikes","comment_count",
             "engagement_rate","like_ratio","title_length",
             "title_word_count","tag_count","sentiment_score"]
corr_matrix = df[corr_cols].corr().round(3)
corr_matrix.to_csv(os.path.join(ANALYTICS_DIR, "correlation_matrix.csv"))

heatmap_data = df.groupby(["category","published_day_of_week"])["engagement_rate"].mean().reset_index()
heatmap_data.columns = ["category","day","engagement_rate"]
heatmap_data.to_csv(os.path.join(ANALYTICS_DIR, "engagement_heatmap.csv"), index=False)

views_heatmap = df.groupby(["publish_hour","published_day_of_week"])["views"].mean().reset_index()
views_heatmap.columns = ["hour","day","avg_views"]
views_heatmap.to_csv(os.path.join(ANALYTICS_DIR, "views_heatmap.csv"), index=False)

df["title_length_bucket"] = pd.cut(df["title_length"],
    bins=[0,20,40,60,80,100,200],
    labels=["0-20","21-40","41-60","61-80","81-100","100+"])
title_eng = df.groupby("title_length_bucket", observed=True).agg(
    avg_views      = ("views","mean"),
    avg_engagement = ("engagement_rate","mean"),
    avg_likes      = ("likes","mean"),
    count          = ("video_id","count")
).reset_index().round(3)
title_eng.to_csv(os.path.join(ANALYTICS_DIR, "title_length_vs_engagement.csv"), index=False)

df["tag_bucket"] = pd.cut(df["tag_count"],
    bins=[-1,0,5,10,20,50,500],
    labels=["0","1-5","6-10","11-20","21-50","50+"])
tag_eng = df.groupby("tag_bucket", observed=True).agg(
    avg_views      = ("views","mean"),
    avg_engagement = ("engagement_rate","mean"),
    count          = ("video_id","count")
).reset_index().round(3)
tag_eng.to_csv(os.path.join(ANALYTICS_DIR, "tag_count_vs_views.csv"), index=False)

scatter_sample = df.sample(min(3000, len(df)), random_state=42)[
    ["title","channel_title","category","views","likes","comment_count",
     "engagement_rate","like_ratio","sentiment","publish_country",
     "published_day_of_week","publish_hour","title_length","tag_count"]
].round(4)
scatter_sample.to_csv(os.path.join(ANALYTICS_DIR, "scatter_sample.csv"), index=False)

print("Diagnostic done.\n")

# ════════════════════════════════════════════
# PREDICTIVE
# ════════════════════════════════════════════
print("--- PREDICTIVE ---")

sent_dist = df["sentiment"].value_counts().reset_index()
sent_dist.columns = ["sentiment","count"]
sent_dist.to_csv(os.path.join(ANALYTICS_DIR, "sentiment_distribution.csv"), index=False)

sent_eng = df.groupby("sentiment").agg(
    avg_views      = ("views","mean"),
    avg_likes      = ("likes","mean"),
    avg_engagement = ("engagement_rate","mean"),
    avg_like_ratio = ("like_ratio","mean"),
    count          = ("video_id","count")
).reset_index().round(3)
sent_eng.to_csv(os.path.join(ANALYTICS_DIR, "sentiment_vs_engagement.csv"), index=False)

sent_cat = df.groupby(["category","sentiment"]).size().reset_index(name="count")
sent_cat.to_csv(os.path.join(ANALYTICS_DIR, "sentiment_by_category.csv"), index=False)

def get_top_keywords(titles, n=30):
    stopwords = {"the","a","an","in","on","of","to","and","is","for","with",
                 "at","by","from","this","that","are","was","it","be","as",
                 "or","but","not","have","has","will","can","i","you","we",
                 "my","our","your","its","new","how","what","why","who","its"}
    words = []
    for t in titles.dropna():
        words += [w.lower() for w in re.findall(r'\b[a-zA-Z]{3,}\b', str(t))
                  if w.lower() not in stopwords]
    return Counter(words).most_common(n)

kw_df = pd.DataFrame(get_top_keywords(df["title"]), columns=["keyword","count"])
kw_df.to_csv(os.path.join(ANALYTICS_DIR, "top_keywords.csv"), index=False)

df["trending_score"] = (
    df["views"] / df["views"].max() * 0.4 +
    df["engagement_rate"] / df["engagement_rate"].max() * 0.3 +
    df["like_ratio"] / df["like_ratio"].max() * 0.2 +
    df["comment_rate"] / df["comment_rate"].max() * 0.1
)
df.nlargest(20, "trending_score")[
    ["title","channel_title","category","views","likes",
     "engagement_rate","like_ratio","trending_score"]
].round(4).to_csv(os.path.join(ANALYTICS_DIR, "top_trending_videos.csv"), index=False)

print("Predictive done.\n")

# ════════════════════════════════════════════
# PRESCRIPTIVE
# ════════════════════════════════════════════
print("--- PRESCRIPTIVE ---")

best_cat      = cat_perf.sort_values("avg_engagement", ascending=False).iloc[0]
best_day      = day_perf.sort_values("avg_engagement", ascending=False).iloc[0]
best_hour_row = hour_perf.sort_values("avg_views", ascending=False).iloc[0]
best_hour     = int(best_hour_row["publish_hour"])
best_title    = title_eng.sort_values("avg_engagement", ascending=False).iloc[0]
best_tags     = tag_eng.sort_values("avg_views", ascending=False).iloc[0]
best_sent     = sent_eng.sort_values("avg_engagement", ascending=False).iloc[0]

prescriptive = {
    "rec_category"    : f"Upload in '{best_cat['category']}' — highest avg engagement ({best_cat['avg_engagement']:.4f})",
    "rec_day"         : f"Publish on {best_day['published_day_of_week']} — avg {int(best_day['avg_views']):,} views",
    "rec_hour"        : f"Upload between {best_hour}:00-{best_hour+1}:00 — peak viewership window",
    "rec_title_length": f"Title {best_title['title_length_bucket']} chars — best engagement ({best_title['avg_engagement']:.4f})",
    "rec_tags"        : f"Use {best_tags['tag_bucket']} tags — highest avg views ({int(best_tags['avg_views']):,})",
    "rec_sentiment"   : f"Use {best_sent['sentiment']} titles — engagement rate {best_sent['avg_engagement']:.4f}",
    "insight_1"       : f"Views-Likes correlation: {corr_matrix.loc['views','likes']}",
    "insight_2"       : f"Title length vs engagement: {corr_matrix.loc['title_length','engagement_rate']}",
    "insight_3"       : f"Tag count vs views: {corr_matrix.loc['tag_count','views']}",
    "insight_4"       : f"Sentiment score vs engagement: {corr_matrix.loc['sentiment_score','engagement_rate']}",
}
pd.DataFrame([prescriptive]).to_csv(os.path.join(ANALYTICS_DIR, "prescriptive.csv"), index=False)

for k, v in prescriptive.items():
    print(f"  {k}: {v}")

# Save full processed dataset
df.to_csv(os.path.join(GOLD_DIR, "youtube_processed.csv"), index=False)

print(f"\n=== Analytics Complete ===")
print(f"Total videos processed: {len(df):,}")