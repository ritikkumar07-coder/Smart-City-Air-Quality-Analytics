-- Databricks SQL dashboard datasets
-- Register the Gold Delta outputs as tables before running these queries.
-- Update main.default if your catalog/schema is different.

-- QUERY 1: KPI cards
SELECT
  ROUND(AVG(avg_aqi), 1) AS overall_average_aqi,
  MAX(max_aqi) AS highest_recorded_aqi,
  COUNT(DISTINCT city) AS cities_monitored,
  SUM(observations) AS total_observations
FROM main.default.gold_city_summary;

-- QUERY 2: City comparison (horizontal bar chart)
SELECT city, ROUND(avg_aqi, 1) AS average_aqi, max_aqi,
       ROUND(avg_pm25, 1) AS average_pm25,
       ROUND(avg_pm10, 1) AS average_pm10
FROM main.default.gold_city_summary
ORDER BY average_aqi DESC;

-- QUERY 3: Daily AQI trend (line chart; city as series)
SELECT CAST(date AS DATE) AS observation_date, city,
       ROUND(AVG(avg_aqi), 1) AS average_aqi
FROM main.default.gold_city_daily_aqi
GROUP BY CAST(date AS DATE), city
ORDER BY observation_date, city;

-- QUERY 4: Monthly AQI trend (line chart; city as series)
SELECT year_month, city, ROUND(AVG(avg_aqi), 1) AS average_aqi
FROM main.default.gold_monthly_trend
GROUP BY year_month, city
ORDER BY year_month, city;

-- QUERY 5: Pollutant comparison (bar chart)
SELECT 'PM2.5' AS pollutant, ROUND(AVG(avg_pm25), 2) AS average_value
FROM main.default.gold_city_summary
UNION ALL
SELECT 'PM10', ROUND(AVG(avg_pm10), 2) FROM main.default.gold_city_summary
UNION ALL
SELECT 'NO2', ROUND(AVG(avg_no2), 2) FROM main.default.gold_city_summary
UNION ALL
SELECT 'SO2', ROUND(AVG(avg_so2), 2) FROM main.default.gold_city_summary
UNION ALL
SELECT 'CO', ROUND(AVG(avg_co), 2) FROM main.default.gold_city_summary;

-- QUERY 6: City detail table
SELECT city, ROUND(avg_aqi, 1) AS average_aqi, max_aqi,
       ROUND(avg_pm25, 1) AS average_pm25,
       ROUND(avg_pm10, 1) AS average_pm10,
       ROUND(avg_no2, 1) AS average_no2,
       ROUND(avg_so2, 1) AS average_so2,
       ROUND(avg_co, 2) AS average_co,
       observations
FROM main.default.gold_city_summary
ORDER BY average_aqi DESC;
