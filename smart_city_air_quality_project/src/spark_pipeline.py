"""PySpark implementation for Databricks or a Spark cluster."""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def run_pipeline(spark, raw_path, output_path):
    raw = spark.read.option("header", True).option("inferSchema", True).csv(raw_path)
    raw.write.format("delta").mode("overwrite").save(f"{output_path}/bronze/air_quality")

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
    df = df.filter((F.col("aqi") >= 0) & (F.col("aqi") <= 500))
    for c in ["pm25","pm10","no2","so2","co"]:
        df = df.filter(F.col(c) >= 0)
    df = df.withColumn("aqi_category",
        F.when(F.col("aqi") <= 50, "Good")
         .when(F.col("aqi") <= 100, "Satisfactory")
         .when(F.col("aqi") <= 200, "Moderate")
         .when(F.col("aqi") <= 300, "Poor")
         .when(F.col("aqi") <= 400, "Very Poor").otherwise("Severe"))
    df = df.withColumn("year_month", F.date_format("date", "yyyy-MM"))
    df.write.format("delta").mode("overwrite").save(f"{output_path}/silver/air_quality")

    silver = spark.read.format("delta").load(f"{output_path}/silver/air_quality")
    daily = silver.groupBy("date","city","latitude","longitude").agg(
        F.avg("aqi").alias("avg_aqi"), F.avg("pm25").alias("avg_pm25"),
        F.avg("pm10").alias("avg_pm10"), F.count("*").alias("records"))
    summary = silver.groupBy("city").agg(
        F.avg("aqi").alias("avg_aqi"), F.max("aqi").alias("max_aqi"),
        F.avg("pm25").alias("avg_pm25"), F.avg("pm10").alias("avg_pm10"),
        F.avg("no2").alias("avg_no2"), F.avg("so2").alias("avg_so2"),
        F.avg("co").alias("avg_co"), F.count("*").alias("observations"))
    monthly = silver.groupBy("year_month","city").agg(
        F.avg("aqi").alias("avg_aqi"), F.max("aqi").alias("max_aqi"))
    for name, frame in [("gold_city_daily_aqi",daily),("gold_city_summary",summary),
                        ("gold_monthly_trend",monthly)]:
        frame.write.format("delta").mode("overwrite").save(f"{output_path}/gold/{name}")
    return daily, summary, monthly
