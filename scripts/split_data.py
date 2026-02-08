import pandas as pd
import os

CHUNK_SIZE = 5000  # rows per chunk
INPUT_FILE = "data/india_aqi.csv"
OUTPUT_DIR = "data/raw_chunks"

os.makedirs(OUTPUT_DIR, exist_ok=True)

df = pd.read_csv(INPUT_FILE)

for i, start in enumerate(range(0, len(df), CHUNK_SIZE)):
    chunk = df.iloc[start:start+CHUNK_SIZE]
    chunk.to_csv(f"{OUTPUT_DIR}/aqi_chunk_{i}.csv", index=False)

print(f"Created {i+1} chunks in {OUTPUT_DIR}")
