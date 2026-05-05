# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

BRONZE_TABLE = "weather_API.bronze.bronze_weather_api2"

SILVER_CLEAN_TABLE = "weather_API.silver.silver_weather_clean"
SILVER_QUARANTINE_TABLE = "weather_API.silver.silver_weather_quarantine"
VALIDATION_LOG_TABLE = "weather_API.silver.validation_log"

CHECKPOINT_PATH = "/Volumes/weather_api/storage/checkpoints"

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS weather_API.silver")

# COMMAND ----------

weather_schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True)
    ]), True),

    StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("main", StringType(), True),
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True)
    ])), True),

    StructField("base", StringType(), True),

    StructField("main", StructType([
        StructField("temp", DoubleType(), True),
        StructField("feels_like", DoubleType(), True),
        StructField("temp_min", DoubleType(), True),
        StructField("temp_max", DoubleType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("sea_level", IntegerType(), True),
        StructField("grnd_level", IntegerType(), True)
    ]), True),

    StructField("visibility", IntegerType(), True),

    StructField("wind", StructType([
        StructField("speed", DoubleType(), True),
        StructField("deg", IntegerType(), True),
        StructField("gust", DoubleType(), True)
    ]), True),

    StructField("rain", StructType([
        StructField("1h", DoubleType(), True),
        StructField("3h", DoubleType(), True)
    ]), True),

    StructField("clouds", StructType([
        StructField("all", IntegerType(), True)
    ]), True),

    StructField("dt", LongType(), True),

    StructField("sys", StructType([
        StructField("type", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("sunrise", LongType(), True),
        StructField("sunset", LongType(), True)
    ]), True),

    StructField("timezone", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("cod", IntegerType(), True)
])

# COMMAND ----------

# # CREATE TARGET TABLES
# # =========================
# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {SILVER_CLEAN_TABLE} (
#     city STRING,
#     temperature DOUBLE,
#     feels_like DOUBLE,
#     humidity INT,
#     pressure INT,
#     wind_speed DOUBLE,
#     weather_condition STRING,
#     weather_description STRING,
#     event_time TIMESTAMP,
#     ingestion_time TIMESTAMP,
#     api_status_code INT,
#     latency_ms DOUBLE,
#     source_city STRING,
#     rain_fall DOUBLE
# )
# USING DELTA
# """)

# COMMAND ----------

# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {SILVER_QUARANTINE_TABLE} (
#     source_city STRING,
#     api_status_code INT,
#     latency_ms DOUBLE,
#     raw_json STRING,
#     ingestion_time TIMESTAMP,
#     quarantine_reason STRING
# )
# USING DELTA
# """)

# COMMAND ----------

# spark.sql(f"""
# CREATE TABLE IF NOT EXISTS {VALIDATION_LOG_TABLE} (
#     run_time TIMESTAMP,
#     total_rows BIGINT,
#     valid_rows BIGINT,
#     quarantine_rows BIGINT
# )
# USING DELTA
# """)

# COMMAND ----------

# FOREACH BATCH FUNCTION
# =========================
def process_silver_batch(batch_df, batch_id):

    if batch_df.count() == 0:
        return
    
    # Parse JSON
    parsed_df = (
        batch_df
        .withColumn("parsed", F.from_json(F.col("raw_json"), weather_schema))
        .withColumn("ingestion_time", F.current_timestamp())
    )
    extracted_df = (
    parsed_df
    .withColumn("city", F.col("parsed.name"))
    .withColumn("temperature", F.col("parsed.main.temp"))
    .withColumn("feels_like", F.col("parsed.main.feels_like"))
    .withColumn("humidity", F.col("parsed.main.humidity"))
    .withColumn("pressure", F.col("parsed.main.pressure"))
    .withColumn("wind_speed", F.col("parsed.wind.speed"))
    .withColumn("weather_condition", F.expr("parsed.weather[0].main"))
    .withColumn("weather_description", F.expr("parsed.weather[0].description"))
    .withColumn("event_time", F.from_unixtime(F.col("parsed.dt")).cast("timestamp"))
    .withColumn(
        "rain_fall",
        F.coalesce(
            F.col("parsed.rain").getField("1h"),
            F.col("parsed.rain").getField("3h"),
            F.lit(0.0)
        )
    )
)
    checked_df = (
        extracted_df
        .withColumn(
            "quarantine_reason",
            F.when(F.col("api_status_code") != 200, F.lit("api_failed"))
             .when(F.col("raw_json").isNull(), F.lit("raw_json_missing"))
             .when(F.col("parsed").isNull(), F.lit("json_parse_failed"))
             .when(F.col("city").isNull(), F.lit("city_missing"))
             .when(F.col("temperature").isNull(), F.lit("temperature_missing"))
             .when((F.col("temperature") < -50) | (F.col("temperature") > 60), F.lit("out_of_range_temperature"))
             .when(F.col("humidity").isNull(), F.lit("humidity_missing"))
             .when((F.col("humidity") < 0) | (F.col("humidity") > 100), F.lit("out_of_range_humidity"))
             .when(F.col("wind_speed") < 0, F.lit("out_of_range_wind_speed"))
             .when(F.col("event_time").isNull(), F.lit("event_time_missing"))
             .when(F.col("rain_fall").isNull(), F.lit("rain_fall_missing"))
             .otherwise(F.lit(None))
        )
    )
    quarantine_df = (
        checked_df
        .filter(F.col("quarantine_reason").isNotNull())
        .select(
            "source_city",
            "api_status_code",
            "latency_ms",
            "raw_json",
            "ingestion_time",
            "quarantine_reason"
        )
    )
    clean_df = (
        checked_df
        .filter(F.col("quarantine_reason").isNull())
        .drop("quarantine_reason")
    )
    # Data is appended into the table
    clean_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(SILVER_CLEAN_TABLE)
            
    quarantine_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(SILVER_QUARANTINE_TABLE)

    # Validation log
    total_rows = checked_df.count()
    valid_rows = clean_df.count()
    quarantine_rows = quarantine_df.count()

    log_df = spark.createDataFrame(
        [(total_rows, valid_rows, quarantine_rows)],
        ["total_rows", "valid_rows", "quarantine_rows"]
    ).withColumn("run_time", F.current_timestamp()) \
        .select("run_time", "total_rows", "valid_rows", "quarantine_rows")

    log_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(VALIDATION_LOG_TABLE)

# COMMAND ----------

bronze_stream_df = (
    spark.readStream
    .format("delta")
    .table(BRONZE_TABLE)
)

# COMMAND ----------

# Ensure target tables exist before streaming starts
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_CLEAN_TABLE} (
    city STRING,
    temperature DOUBLE,
    feels_like DOUBLE,
    humidity INT,
    pressure INT,
    wind_speed DOUBLE,
    weather_condition STRING,
    weather_description STRING,
    event_time TIMESTAMP,
    ingestion_time TIMESTAMP,
    api_status_code INT,
    latency_ms DOUBLE,
    source_city STRING,
    rain_fall DOUBLE
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_QUARANTINE_TABLE} (
    source_city STRING,
    api_status_code INT,
    latency_ms DOUBLE,
    raw_json STRING,
    ingestion_time TIMESTAMP,
    quarantine_reason STRING
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {VALIDATION_LOG_TABLE} (
    run_time TIMESTAMP,
    total_rows BIGINT,
    valid_rows BIGINT,
    quarantine_rows BIGINT
)
USING DELTA
""")

query = (
    bronze_stream_df.writeStream
    .format("delta")
    .foreachBatch(process_silver_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

display(spark.table(SILVER_CLEAN_TABLE))
display(spark.table("weather_API.silver.silver_weather_quarantine"))
display(spark.table("weather_API.silver.validation_log"))

# COMMAND ----------

# dbutils.fs.rm(CHECKPOINT_PATH, True)
# spark.sql(f"DROP TABLE IF EXISTS {SILVER_CLEAN_TABLE}")
# spark.sql(f"DROP TABLE IF EXISTS {SILVER_QUARANTINE_TABLE}")
# spark.sql(f"DROP TABLE IF EXISTS {VALIDATION_LOG_TABLE}")

# COMMAND ----------

