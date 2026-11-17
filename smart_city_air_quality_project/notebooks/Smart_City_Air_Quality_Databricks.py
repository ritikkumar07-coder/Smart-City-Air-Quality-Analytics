# Databricks notebook source
# MAGIC %md
# MAGIC # Smart City – Air Quality & Pollution Intelligence
# MAGIC Demo pipeline: Bronze → Silver → Gold using PySpark and Delta Lake.
# MAGIC **Note:** sample dataset is synthetic, not official live CPCB data.

# COMMAND ----------
raw_path = "/FileStore/tables/air_quality_raw.csv"  # Upload CSV here first
output_path = "/mnt/smartcity_air_quality"

# COMMAND ----------
from pyspark.sql import functions as F
raw = (spark.read.option("header", True).option("inferSchema", True).csv(raw_path))
display(raw.limit(10))
print("Raw rows:", raw.count())

# COMMAND ----------
# Bronze: preserve source data as received
raw.write.format("delta").mode("overwrite").save(f"{output_path}/bronze/air_quality")

# COMMAND ----------
# Silver: type casting, duplicate removal, null handling, validation
df = spark.read.format("delta").load(f"{output_path}/bronze/air_quality")
numeric = ["latitude","longitude","temperature_c","humidity_pct","wind_speed_kmph",
           "pm25","pm10","no2","so2","co","aqi"]
for c in numeric:
    df = df.withColumn(c, F.col(c).cast("double"))
df = df.withColumn("date", F.to_date("date"))
df = df.dropna(subset=["date","city","aqi"]).dropDuplicates(["date","city"])
medians = df.groupBy("city").agg(*[
    F.percentile_approx(c, 0.5).alias(f"{c}_median") for c in numeric
])
df = df.join(medians, "city", "left")
for c in numeric:
    df = df.withColumn(c, F.coalesce(F.col(c), F.col(f"{c}_median"))).drop(f"{c}_median")
df = df.filter(F.col("aqi").between(0,500))
for c in ["pm25","pm10","no2","so2","co"]:
    df = df.filter(F.col(c) >= 0)
df = df.withColumn("aqi_category",
    F.when(F.col("aqi") <= 50,"Good").when(F.col("aqi") <= 100,"Satisfactory")
     .when(F.col("aqi") <= 200,"Moderate").when(F.col("aqi") <= 300,"Poor")
     .when(F.col("aqi") <= 400,"Very Poor").otherwise("Severe"))
df = df.withColumn("year_month", F.date_format("date","yyyy-MM"))
df.write.format("delta").mode("overwrite").save(f"{output_path}/silver/air_quality")
display(df.limit(10))

# COMMAND ----------
# Gold: analytics-ready aggregations
silver = spark.read.format("delta").load(f"{output_path}/silver/air_quality")
city_daily = silver.groupBy("date","city","latitude","longitude").agg(
    F.avg("aqi").alias("avg_aqi"), F.avg("pm25").alias("avg_pm25"),
    F.avg("pm10").alias("avg_pm10"), F.count("*").alias("records"))
city_summary = silver.groupBy("city").agg(
    F.round(F.avg("aqi"),1).alias("avg_aqi"), F.max("aqi").alias("max_aqi"),
    F.round(F.avg("pm25"),1).alias("avg_pm25"),
    F.round(F.avg("pm10"),1).alias("avg_pm10"),
    F.round(F.avg("no2"),1).alias("avg_no2"),
    F.round(F.avg("so2"),1).alias("avg_so2"),
    F.round(F.avg("co"),2).alias("avg_co"), F.count("*").alias("observations"))
monthly = silver.groupBy("year_month","city").agg(
    F.round(F.avg("aqi"),1).alias("avg_aqi"), F.max("aqi").alias("max_aqi"))
city_daily.write.format("delta").mode("overwrite").save(f"{output_path}/gold/city_daily_aqi")
city_summary.write.format("delta").mode("overwrite").save(f"{output_path}/gold/city_summary")
monthly.write.format("delta").mode("overwrite").save(f"{output_path}/gold/monthly_trend")
display(city_summary.orderBy(F.desc("avg_aqi")))

# COMMAND ----------
# Optional: register SQL tables for Databricks SQL / dashboard connection
spark.read.format("delta").load(f"{output_path}/gold/city_summary").createOrReplaceTempView("gold_city_summary")
spark.read.format("delta").load(f"{output_path}/gold/city_daily_aqi").createOrReplaceTempView("gold_city_daily_aqi")
spark.read.format("delta").load(f"{output_path}/gold/monthly_trend").createOrReplaceTempView("gold_monthly_trend")
