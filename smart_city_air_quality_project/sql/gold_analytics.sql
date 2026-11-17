-- Run against registered Gold tables in Databricks SQL
-- 1. City ranking
SELECT city, ROUND(avg_aqi,1) AS avg_aqi, max_aqi, avg_pm25, avg_pm10
FROM gold_city_summary ORDER BY avg_aqi DESC;

-- 2. Daily AQI trend
SELECT date, city, ROUND(avg_aqi,1) AS avg_aqi
FROM gold_city_daily_aqi ORDER BY date, city;

-- 3. Monthly trend
SELECT year_month, city, ROUND(avg_aqi,1) AS avg_aqi
FROM gold_monthly_trend ORDER BY year_month, city;

-- 4. Highest pollution day by city
SELECT city, date, avg_aqi,
       ROW_NUMBER() OVER (PARTITION BY city ORDER BY avg_aqi DESC) AS rank_in_city
FROM gold_city_daily_aqi
QUALIFY rank_in_city = 1;

-- 5. Overall city KPI
SELECT ROUND(AVG(avg_aqi),1) AS overall_avg_aqi,
       MAX(max_aqi) AS highest_recorded_aqi,
       COUNT(DISTINCT city) AS cities_monitored
FROM gold_city_summary;
