import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="aqi_db",
    port=3307
)

cursor = conn.cursor()

THRESHOLD = 300  # Hazardous AQI threshold

cursor.execute("""
SELECT city, state, date, avg_aqi, aqi_category
FROM aqi_daily_summary
WHERE avg_aqi > %s
ORDER BY avg_aqi DESC
""", (THRESHOLD,))

results = cursor.fetchall()

if results:
    print("⚠️  ALERT: Hazardous AQI Levels Detected!")
    for row in results:
        print(f"City: {row[0]}, State: {row[1]}, Date: {row[2]}, AQI: {row[3]}, Category: {row[4]}")
else:
    print("No hazardous AQI levels detected.")

conn.close()
