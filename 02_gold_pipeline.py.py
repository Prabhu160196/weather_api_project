# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS weather_API.gold;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

silver_df = spark.table("weather_API.silver.silver_weather_clean")
window_spec = Window.partitionBy("city").orderBy(F.col("event_time").desc())
ranked_df = silver_df.withColumn("rn", F.row_number().over(window_spec))
gold_df = ranked_df.filter(F.col("rn") == 1).drop("rn")


gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("weather_API.gold.gold_city_current")

display(gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 2.gold_hourly_summary

# COMMAND ----------

from pyspark.sql import functions as F

# Read Silver
silver_df = spark.table("weather_API.silver.silver_weather_clean")

# 1) Find max event_time from data
max_ts = silver_df.select(F.max("event_time").alias("max_ts")).collect()[0]["max_ts"]

# 2) Compute threshold = max_ts - 24 hours
threshold = max_ts - F.expr("INTERVAL 24 HOURS")  # not usable directly in Python

# Proper way (use expr inside filter):
filtered_df = silver_df.filter(
    F.col("event_time") >= F.expr(
        f"timestamp('{max_ts}') - INTERVAL 24 HOURS"
    )
)

# View result
display(filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 3.Manual validation and quarantine

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = spark.table("weather_project.silver.silver_weather_clean")

gold_daily_extremes = (
    silver_df
    .groupBy("city", "event_date")
    .agg(
        F.max("temperature").alias("max_temp"),

        F.expr("max_by(event_time, temperature)").alias("max_temp_time"),

        F.min("temperature").alias("min_temp"),

        F.max("wind_speed").alias("peak_wind"),

        F.expr("max_by(event_time, wind_speed)").alias("peak_wind_time"),

        F.sum("rainfall").alias("total_rainfall"),

        F.max("weather_severity_score").alias("max_severity")
    )
)

gold_daily_extremes.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("weather_project.gold.gold_daily_extremes")

display(gold_daily_extremes)

# COMMAND ----------

# MAGIC %md
# MAGIC 4.Add derived fields

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_df = spark.table("weather_project.silver.silver_weather_clean")

# last 2 hours based on latest event_time in data
max_event_time = silver_df.agg(F.max("event_time").alias("max_time")).collect()[0]["max_time"]

recent_df = silver_df.filter(
    F.col("event_time") >= F.expr(f"timestamp('{max_event_time}') - INTERVAL 2 HOURS")
)

alerts_df = (
    recent_df
    .withColumn(
        "alert_type",
        F.when(F.col("weather_severity_score") >= 4, F.lit("heatwave"))
         .when(F.lower(F.col("weather_condition")).contains("storm"), F.lit("storm"))
         .when(F.lower(F.col("weather_condition")).contains("thunder"), F.lit("storm"))
         .when(F.col("wind_speed") > 50, F.lit("high_wind"))
         .when(F.col("rainfall") > 50, F.lit("heavy_rain"))
    )
    .filter(F.col("alert_type").isNotNull())
    .withColumn("alert_triggered_at", F.col("event_time"))
)

w = Window.partitionBy("city").orderBy(F.col("event_time").desc())

gold_severe_weather_alerts = (
    alerts_df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "city",
        "event_time",
        "temperature",
        "wind_speed",
        "rainfall",
        "weather_condition",
        "weather_severity_score",
        "alert_type",
        "alert_triggered_at"
    )
)

gold_severe_weather_alerts.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("weather_project.gold.gold_severe_weather_alerts")

display(gold_severe_weather_alerts)

# COMMAND ----------

# MAGIC %md
# MAGIC 5.Write to silver_weather_clean

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = spark.table("weather_API.bronze.bronze_weather_api2")

gold_api_health = (
    bronze_df
    .groupBy(
        F.col("source_city").alias("city"),
        "load_date"
    )
    .agg(
        F.count("*").alias("total_calls"),

        F.sum(
            F.when(F.col("api_status_code") == 200, 1).otherwise(0)
        ).alias("successful_calls"),

        F.sum(
            F.when(F.col("api_status_code") != 200, 1).otherwise(0)
        ).alias("failed_calls"),

        F.sum(
            F.when(
                (F.col("api_status_code") == 200) & (F.col("raw_json").isNull()),
                1
            ).otherwise(0)
        ).alias("parse_errors"),

        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),

        F.max(
            F.when(F.col("api_status_code") == 200, F.col("ingestion_time"))
        ).alias("latest_success_time")
    )
    .withColumn(
        "success_rate",
        F.round((F.col("successful_calls") / F.col("total_calls")) * 100, 2)
    )
    .withColumn(
        "is_stale",
        F.when(
            F.col("latest_success_time") < F.current_timestamp() - F.expr("INTERVAL 1 HOUR"),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .select(
        "city",
        "load_date",
        "total_calls",
        "successful_calls",
        "failed_calls",
        "parse_errors",
        "avg_latency_ms",
        "success_rate",
        "is_stale"
    )
)

gold_api_health.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("weather_API.gold.gold_api_health")

display(gold_api_health)

# COMMAND ----------

valid_df = spark.table("weather_API.silver.silver_weather_clean")

valid_df.count()

# COMMAND ----------

quarantine_df = spark.table("weather_API.silver.silver_weather_quarantine")

quarantine_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 6.Write a validation summary

# COMMAND ----------

# counts
import json

total_rows_read = valid_df.count() + quarantine_df.count()
valid_rows = valid_df.count()
quarantined_rows = quarantine_df.count()

# quarantine breakdown (reason → count)
breakdown_df = (
    quarantine_df
    .groupBy("quarantine_reason")
    .count()
)

# convert to JSON string
breakdown_dict = {
    row["quarantine_reason"]: row["count"]
    for row in breakdown_df.collect()
}

quarantine_breakdown_json = json.dumps(breakdown_dict)

# create single row dataframe
validation_summary_df = spark.createDataFrame(
    [(
        total_rows_read,
        valid_rows,
        quarantined_rows,
        quarantine_breakdown_json
    )],
    ["total_rows_read", "valid_rows", "quarantined_rows", "quarantine_breakdown"]
).withColumn("run_timestamp", F.current_timestamp())

# write to Delta table (append)
validation_summary_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("weather_project.silver.validation_log")

display(validation_summary_df)    


# COMMAND ----------

