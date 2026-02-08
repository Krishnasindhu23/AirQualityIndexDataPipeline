USE aqi_db;

CREATE TABLE IF NOT EXISTS aqi_daily_summary (
    city VARCHAR(100),
    state VARCHAR(100),
    date DATE,
    avg_aqi FLOAT,
    aqi_category VARCHAR(50),
    PRIMARY KEY (city, date)
);

INSERT INTO aqi_daily_summary (city, state, date, avg_aqi, aqi_category)
SELECT
    city,
    state,
    date,
    AVG(aqi) AS avg_aqi,
    CASE
        WHEN AVG(aqi) <= 50 THEN 'Good'
        WHEN AVG(aqi) <= 100 THEN 'Moderate'
        WHEN AVG(aqi) <= 200 THEN 'Poor'
        ELSE 'Severe'
    END AS aqi_category
FROM raw_aqi
GROUP BY city, state, date
ON DUPLICATE KEY UPDATE
    avg_aqi = VALUES(avg_aqi),
    aqi_category = VALUES(aqi_category);
