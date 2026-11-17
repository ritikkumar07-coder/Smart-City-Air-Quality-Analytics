# Industry-Level Dashboard (Databricks SQL)

The assignment asks for an industry-level dashboard but does not prescribe a separate BI product. This project therefore uses **Databricks SQL AI/BI Dashboards**, keeping the required stack limited to Python, Apache Spark and Databricks.

## 1. Prepare Gold tables
Run `notebooks/Smart_City_Air_Quality_Databricks.py` from top to bottom. The notebook writes these Delta datasets:
- `gold/city_summary`
- `gold/city_daily_aqi`
- `gold/monthly_trend`

The notebook also creates temporary views for immediate SQL exploration. For a persistent SQL dashboard, register the Gold Delta datasets as tables in your Unity Catalog schema (or create tables from the Delta locations).

Example (replace catalog/schema and paths with your workspace values):
```sql
CREATE TABLE IF NOT EXISTS main.default.gold_city_summary
USING DELTA
LOCATION '/mnt/smartcity_air_quality/gold/city_summary';

CREATE TABLE IF NOT EXISTS main.default.gold_city_daily_aqi
USING DELTA
LOCATION '/mnt/smartcity_air_quality/gold/city_daily_aqi';

CREATE TABLE IF NOT EXISTS main.default.gold_monthly_trend
USING DELTA
LOCATION '/mnt/smartcity_air_quality/gold/monthly_trend';
```
If your workspace does not allow external locations, create managed tables from the DataFrames in the notebook using `.write.saveAsTable("main.default.gold_city_summary")`, etc.

## 2. Create the dashboard
1. Open **Databricks SQL** in your workspace.
2. Open **SQL Editor** and select your SQL warehouse.
3. Open `sql/dashboard_queries.sql`, run each query separately, and save each query with the indicated title.
4. Open **Dashboards → Create dashboard**.
5. Add the saved queries as datasets/tiles.
6. Arrange the tiles using the layout below and publish/share with your instructor.

## 3. Dashboard layout
- **Top KPI cards:** Average AQI, highest AQI, monitored cities, total observations.
- **City comparison:** Horizontal bar chart — city vs average AQI; sort descending.
- **AQI trend:** Line chart — date vs average AQI, series grouped by city.
- **Monthly trend:** Line chart — month vs average AQI, series grouped by city.
- **Pollutant comparison:** Bar chart — PM2.5, PM10, NO2, SO2, CO average values.
- **City details:** Table — city, average AQI, maximum AQI, PM2.5, PM10, NO2, SO2, CO.

Use dashboard filters for city and date wherever supported. Add a visible note: “Demonstration dataset is synthetic; not official CPCB live measurements.”

## 4. Data interpretation
AQI category labels in the sample project are broad demonstration labels. For a production/public-health dashboard, use the official Indian AQI calculation and breakpoints for each pollutant sub-index.
