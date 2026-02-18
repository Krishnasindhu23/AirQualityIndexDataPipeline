import pandas as pd
import mysql.connector
import glob
import os
from datetime import datetime

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="aqi_db",
    port=3307  # match your Docker MySQL port
)

cursor = conn.cursor()

CHUNK_DIR = "data/raw_chunks"

# Find unprocessed CSV chunks
chunks = sorted(glob.glob(f"{CHUNK_DIR}/*.csv"))

if not chunks:
    print("No new chunks to ingest.")
    exit()

# Pick the first chunk
file_to_load = chunks[0]
print(f"Ingesting {file_to_load}")

# Read CSV
df = pd.read_csv(file_to_load)

# Insert rows into raw_aqi
for _, row in df.iterrows():

    formatted_date = datetime.strptime(row['date'], "%d-%m-%Y").strftime("%Y-%m-%d")
    cursor.execute(
        """
        INSERT INTO raw_aqi (city, state, date, aqi, pm25, pm10, no2, so2)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            row['area'],               # city
            row['state'],              # state
            formatted_date,              # date
            row['aqi_value'],          # aqi
            None,                      # pm25
            None,                      # pm10
            None,                      # no2
            None                       # so2
        )
    )


conn.commit()
conn.close()

# Mark file as processed
os.rename(file_to_load, file_to_load + ".processed")

print("Ingestion complete.")








