# Project Report: Smart City – Air Quality and Pollution Intelligence

## 1. Problem statement
Urban residents and city administrators need timely, understandable information about air pollution. Measurements are distributed across stations and time, making comparison and trend analysis difficult.

## 2. Objective
Build an end-to-end data pipeline that ingests air-quality and weather observations, validates and transforms them at scale, and publishes analytics-ready datasets for a dashboard.

## 3. Scope
MVP coverage: Delhi, Noida, Gurugram, Faridabad and Ghaziabad. Included fields: PM2.5, PM10, NO2, SO2, CO, temperature, humidity, wind speed and AQI.

## 4. Architecture
Source CSV/API → Python ingestion → Bronze raw Delta → Silver cleaned Delta → Gold aggregate Delta → Databricks SQL / Power BI.

## 5. Data quality
- Parse dates and numeric columns.
- Remove duplicate city-date observations.
- Handle missing numeric values using city-level medians.
- Reject invalid AQI outside 0–500 and negative pollutant values.
- Retain raw Bronze data for auditability.

## 6. Gold outputs
- City daily AQI: date/city/location and daily pollutant/AQI metrics.
- City summary: city-level average and maximum metrics.
- Monthly trend: monthly city-level AQI.
- Pollutant summary: overall pollutant averages.

## 7. Tools
Python, Pandas, Apache Spark, PySpark, Databricks Delta Lake, Databricks SQL and Databricks SQL AI/BI Dashboards.

## 8. Limitations and future work
The bundled CSV is synthetic demo data, not official monitoring data. Production extension: ingest CPCB/OpenAQ station observations and a weather API; store source/station IDs, units, timestamps and licensing; schedule incremental jobs; add data-quality checks and alerting; implement official AQI sub-index calculation; publish Power BI refresh.

## 9. Expected outcome
A repeatable pipeline that turns raw environmental observations into clean, aggregated tables and an interactive city comparison dashboard.
