import pandas as pd
import mysql.connector
import glob
import os

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="aqi_db",
    port=3307
)

cursor = conn.cursor()

CHUNK_DIR = "data/raw_chunks"

files = sorted(glob.glob(f"{CHUNK_DIR}/*.csv"))

if not files:
    print("No new data to ingest.")
    exit()

file = files[0]
print(f"Loading {file}")

df = pd.read_csv(file)

for _, row in df.iterrows():
    cursor.execute(
        """
        INSERT INTO raw_aqi (city, state, date, aqi, pm25, pm10, no2, so2)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        tuple(row)
    )

conn.commit()
conn.close()

os.rename(file, file + ".processed")

print("Ingestion successful.")
