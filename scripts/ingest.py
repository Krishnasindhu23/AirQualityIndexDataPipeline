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

for file_to_load in chunks:
    print(f"Ingesting {file_to_load}")
    
    df = pd.read_csv(file_to_load)

    for _, row in df.iterrows():
        formatted_date = datetime.strptime(row['date'], "%d-%m-%Y").strftime("%Y-%m-%d")
        cursor.execute(
            """
            INSERT IGNORE INTO raw_aqi (city, state, date, aqi, pm25, pm10, no2, so2)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row['area'],
                row['state'],
                formatted_date,
                row['aqi_value'],
                None,
                None,
                None,
                None
            )
        )

    os.rename(file_to_load, file_to_load + ".processed")

conn.commit()
conn.close()

print("All chunks ingested successfully.")







