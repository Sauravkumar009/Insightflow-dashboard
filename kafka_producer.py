import pandas as pd
import json
import time
import os

DATA_DIR = "data"
OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=== Simulated Kafka Producer Started ===\n")

# Load datasets
print("Loading YouTube dataset...")
youtube_df = pd.read_csv(os.path.join(DATA_DIR, "youtube.csv"), on_bad_lines='skip')
print(f"YouTube records loaded: {len(youtube_df)}")

print("Loading Reddit dataset...")
reddit_df = pd.read_csv(os.path.join(DATA_DIR, "reddit_database.csv"), on_bad_lines='skip')
print(f"Reddit records loaded: {len(reddit_df)}\n")

# Simulate streaming - process in batches
BATCH_SIZE = 100
DELAY = 0.5  # seconds between batches

youtube_stream = []
reddit_stream = []

print("=== Streaming YouTube data ===")
for i in range(0, min(1000, len(youtube_df)), BATCH_SIZE):
    batch = youtube_df.iloc[i:i+BATCH_SIZE]
    youtube_stream.append(batch)
    print(f"YouTube batch {i//BATCH_SIZE + 1} streamed: {len(batch)} records")
    time.sleep(DELAY)

print("\n=== Streaming Reddit data ===")
for i in range(0, min(1000, len(reddit_df)), BATCH_SIZE):
    batch = reddit_df.iloc[i:i+BATCH_SIZE]
    reddit_stream.append(batch)
    print(f"Reddit batch {i//BATCH_SIZE + 1} streamed: {len(batch)} records")
    time.sleep(DELAY)

# Save streamed data as bronze layer input
youtube_streamed = pd.concat(youtube_stream)
reddit_streamed = pd.concat(reddit_stream)

youtube_streamed.to_csv(os.path.join(OUTPUT_DIR, "youtube_streamed.csv"), index=False)
reddit_streamed.to_csv(os.path.join(OUTPUT_DIR, "reddit_streamed.csv"), index=False)

print("\n=== Streaming Complete ===")
print(f"YouTube records streamed: {len(youtube_streamed)}")
print(f"Reddit records streamed: {len(reddit_streamed)}")
print("Files saved to outputs/ folder")
print("\nReady for ETL pipeline!")
