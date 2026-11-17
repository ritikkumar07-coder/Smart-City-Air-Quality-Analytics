# Smart City – Air Quality and Pollution Intelligence

An end-to-end Data Engineering MVP for Delhi NCR using Python, PySpark, Databricks and Databricks SQL AI/BI Dashboards.

## What is included
- `data/raw/air_quality_raw.csv`: reproducible demo dataset (synthetic; not official live measurements).
- `src/generate_data.py`: regenerate the demo data.
- `src/local_pipeline.py`: local Python/Pandas Bronze → Silver → Gold pipeline.
- `src/spark_pipeline.py`: equivalent PySpark pipeline for Spark/Databricks.
- `notebooks/Smart_City_Air_Quality_Databricks.py`: Databricks notebook source.
- `sql/gold_analytics.sql`: SQL queries for dashboard metrics.
- `dashboard/databricks_dashboard_guide.md`: Databricks SQL dashboard instructions.
- `docs/project_report.md`: project report outline and architecture.

## Quick start (Windows / VS Code)
1. Install Python 3.10+.
2. Open this folder in VS Code.
3. Create environment: `python -m venv .venv`
4. Activate: `.venv\\Scripts\\activate`
5. Install: `pip install -r requirements.txt`
6. Run: `python src/local_pipeline.py`

Gold outputs will be created in `data/gold/`:
- `city_daily_aqi.csv`
- `city_summary.csv`
- `pollutant_summary.csv`
- `monthly_trend.csv`

## Important data note
The included dataset is synthetic demo data created for testing and presentation. It must not be described as real CPCB observations. For a real deployment, replace the raw CSV with verified CPCB/OpenAQ observations and preserve source, station, timestamp, units and license metadata.

## Databricks
Upload `data/raw/air_quality_raw.csv` to DBFS/Volumes, then adapt the paths in the notebook. Run the notebook top-to-bottom. It writes Delta tables `bronze_air_quality`, `silver_air_quality`, `gold_city_daily_aqi`, `gold_city_summary`, and `gold_pollutant_summary`.

## Suggested architecture
Sources (CPCB/OpenAQ + weather) → Bronze (raw Delta) → Silver (validated/cleaned Delta) → Gold (aggregated Delta) → Databricks SQL AI/BI Dashboards.

## Dashboard
The project uses Databricks SQL AI/BI Dashboards (no separate BI tool is added). Follow `dashboard/databricks_dashboard_guide.md` and run `sql/dashboard_queries.sql` to create KPI cards, city comparison, daily/monthly AQI trends, pollutant comparison and a city detail table.
