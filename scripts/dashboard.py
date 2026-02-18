import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="aqi_db",
    port=3307
)

query = """
SELECT city, AVG(avg_aqi) as avg_city_aqi
FROM aqi_daily_summary
GROUP BY city
ORDER BY avg_city_aqi DESC
LIMIT 10
"""

df = pd.read_sql(query, conn)

plt.figure(figsize=(10,6))
plt.bar(df['city'], df['avg_city_aqi'])
plt.xticks(rotation=45)
plt.title("Top 10 Cities by Average AQI")
plt.ylabel("Average AQI")
plt.tight_layout()
plt.show()

conn.close()
