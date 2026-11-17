# Databricks notebook source

from pyspark.sql import functions as F

# Project paths
BASE = "/Volumes/workspace/default/smartcity_data"
RAW_PATH = BASE + "/air_quality_raw.csv"

# Create output directories
for folder in ["bronze", "silver", "gold"]:
    dbutils.fs.mkdirs(f"{BASE}/{folder}")

# 1. BRONZE LAYER - Load raw data
raw_df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(RAW_PATH))

raw_df.write.format("delta").mode("overwrite").save(
    BASE + "/bronze/air_quality"
)

print("Bronze layer completed")
print("Raw records:", raw_df.count())

# 2. SILVER LAYER - Clean data
df = spark.read.format("delta").load(
    BASE + "/bronze/air_quality"
)

numeric_cols = [
    "latitude", "longitude", "temperature_c",
    "humidity_pct", "wind_speed_kmph",
    "pm25", "pm10", "no2", "so2", "co", "aqi"
]

for col in numeric_cols:
    df = df.withColumn(col, F.col(col).cast("double"))

df = df.withColumn("date", F.to_date("date"))

df = df.dropna(subset=["date", "city", "aqi"])
df = df.dropDuplicates(["date", "city"])

# Fill missing values using city-wise median
medians = df.groupBy("city").agg(*[
    F.percentile_approx(c, 0.5).alias(c + "_median")
    for c in numeric_cols
])

df = df.join(medians, "city", "left")

for col in numeric_cols:
    df = df.withColumn(
        col,
        F.coalesce(F.col(col), F.col(col + "_median"))
    ).drop(col + "_median")

# Remove invalid values
df = df.filter(F.col("aqi").between(0, 500))

for col in ["pm25", "pm10", "no2", "so2", "co"]:
    df = df.filter(F.col(col) >= 0)

# AQI categories
df = df.withColumn(
    "aqi_category",
    F.when(F.col("aqi") <= 50, "Good")
     .when(F.col("aqi") <= 100, "Satisfactory")
     .when(F.col("aqi") <= 200, "Moderate")
     .when(F.col("aqi") <= 300, "Poor")
     .when(F.col("aqi") <= 400, "Very Poor")
     .otherwise("Severe")
)

df = df.withColumn(
    "year_month", F.date_format("date", "yyyy-MM")
)

df.write.format("delta").mode("overwrite").save(
    BASE + "/silver/air_quality"
)

print("Silver layer completed")
print("Clean records:", df.count())

# 3. GOLD LAYER - Analytics

silver = spark.read.format("delta").load(
    BASE + "/silver/air_quality"
)

# City-wise daily AQI
daily = silver.groupBy(
    "date", "city", "latitude", "longitude"
).agg(
    F.round(F.avg("aqi"), 2).alias("avg_aqi"),
    F.round(F.avg("pm25"), 2).alias("avg_pm25"),
    F.round(F.avg("pm10"), 2).alias("avg_pm10"),
    F.count("*").alias("records")
)

# City summary
summary = silver.groupBy("city").agg(
    F.round(F.avg("aqi"), 2).alias("avg_aqi"),
    F.max("aqi").alias("max_aqi"),
    F.round(F.avg("pm25"), 2).alias("avg_pm25"),
    F.round(F.avg("pm10"), 2).alias("avg_pm10"),
    F.round(F.avg("no2"), 2).alias("avg_no2"),
    F.round(F.avg("so2"), 2).alias("avg_so2"),
    F.round(F.avg("co"), 2).alias("avg_co"),
    F.count("*").alias("observations")
)

# Monthly AQI trends
monthly = silver.groupBy(
    "year_month", "city"
).agg(
    F.round(F.avg("aqi"), 2).alias("avg_aqi"),
    F.max("aqi").alias("max_aqi")
)

# Pollutant summary
pollutants = silver.agg(
    *[F.avg(c).alias("avg_" + c)
      for c in ["pm25", "pm10", "no2", "so2", "co"]]
)

pollutant_summary = pollutants.selectExpr(
    """stack(5,
    'PM2.5', avg_pm25,
    'PM10', avg_pm10,
    'NO2', avg_no2,
    'SO2', avg_so2,
    'CO', avg_co
    ) as (pollutant, average_value)"""
)

# Save Gold datasets
daily.write.format("delta").mode("overwrite").save(
    BASE + "/gold/city_daily_aqi"
)

summary.write.format("delta").mode("overwrite").save(
    BASE + "/gold/city_summary"
)

monthly.write.format("delta").mode("overwrite").save(
    BASE + "/gold/monthly_trend"
)

pollutant_summary.write.format("delta").mode("overwrite").save(
    BASE + "/gold/pollutant_summary"
)

print("Gold layer completed!")

display(summary.orderBy(F.desc("avg_aqi")))


# COMMAND ----------


# Register Gold datasets as Databricks tables

tables = {
    "gold_city_summary": "city_summary",
    "gold_city_daily_aqi": "city_daily_aqi",
    "gold_monthly_trend": "monthly_trend",
    "gold_pollutant_summary": "pollutant_summary"
}

for table_name, folder in tables.items():
    path = f"{BASE}/gold/{folder}"

    (spark.read.format("delta").load(path)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"workspace.default.{table_name}"))

    print(f"{table_name} created successfully")

