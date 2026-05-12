from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Dict, Tuple
from urllib.parse import quote_plus

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_weather_url(city: str, base_url: str, api_key: str, units: str = "metric") -> str:
    encoded_city = quote_plus(city)
    return f"{base_url}?q={encoded_city}&appid={api_key}&units={units}"


def fetch_weather_for_city(cities: str, base_url: str, api_key: str, units: str = "metric") -> Dict:
    url = build_weather_url(cities, base_url, api_key, units)
    start_time = time.time()
    call_timestamp = datetime.now()

    try:
        response = requests.get(url, timeout=10)
        latency_ms = int((time.time() - start_time) * 1000)

        return {
            "requested_city": cities,
            "status_code": response.status_code,
            "raw_response": response.text,
            "call_timestamp": call_timestamp,
            "latency_ms": latency_ms,
        }

    except Exception:
        latency_ms = int((time.time() - start_time) * 1000)

        return {
            "requested_city": cities,
            "status_code": 0,
            "raw_response": None,
            "call_timestamp": call_timestamp,
            "latency_ms": latency_ms,
        }


def load_config_file(config_path: str) -> Dict:
    with open(config_path, "r", encoding="utf-8") as handle:
        config = json.load(handle)

    if not config.get("api_key"):
        raise ValueError("config.json must contain api_key")

    if not config.get("cities") or not isinstance(config["cities"], list):
        raise ValueError("config.json must contain cities as a list")

    return config


def get_openweather_schema() -> T.StructType:
    return T.StructType([
        T.StructField("coord", T.StructType([
            T.StructField("lon", T.DoubleType(), True),
            T.StructField("lat", T.DoubleType(), True),
        ]), True),

        T.StructField("weather", T.ArrayType(T.StructType([
            T.StructField("id", T.IntegerType(), True),
            T.StructField("main", T.StringType(), True),
            T.StructField("description", T.StringType(), True),
            T.StructField("icon", T.StringType(), True),
        ])), True),

        T.StructField("main", T.StructType([
            T.StructField("temp", T.DoubleType(), True),
            T.StructField("feels_like", T.DoubleType(), True),
            T.StructField("pressure", T.IntegerType(), True),
            T.StructField("humidity", T.IntegerType(), True),
        ]), True),

        T.StructField("visibility", T.IntegerType(), True),

        T.StructField("wind", T.StructType([
            T.StructField("speed", T.DoubleType(), True),
            T.StructField("deg", T.IntegerType(), True),
        ]), True),

        T.StructField("rain", T.StructType([
            T.StructField("1h", T.DoubleType(), True),
        ]), True),

        T.StructField("sys", T.StructType([
            T.StructField("country", T.StringType(), True),
            T.StructField("sunrise", T.LongType(), True),
            T.StructField("sunset", T.LongType(), True),
        ]), True),

        T.StructField("timezone", T.IntegerType(), True),
        T.StructField("id", T.LongType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("cod", T.IntegerType(), True),
        T.StructField("dt", T.LongType(), True),
    ])


def parse_batch_weather(batch_df: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    api_errors = batch_df.filter(F.col("status_code") != 200)

    successful_rows = batch_df.filter(F.col("status_code") == 200)

    parsed_rows = successful_rows.withColumn(
        "parsed",
        F.from_json(F.col("raw_response"), get_openweather_schema())
    )

    unparseable_rows = parsed_rows.filter(
        F.col("raw_response").isNull()
        | F.col("parsed").isNull()
        | F.col("parsed.name").isNull()
        | F.col("parsed.dt").isNull()
        | F.col("parsed.main.temp").isNull()
    )

    parsed_only_rows = parsed_rows.filter(
        F.col("raw_response").isNotNull()
        & F.col("parsed").isNotNull()
        & F.col("parsed.name").isNotNull()
        & F.col("parsed.dt").isNotNull()
        & F.col("parsed.main.temp").isNotNull()
    )

    return api_errors, unparseable_rows, parsed_only_rows


def extract_and_enrich_weather(batch_df: DataFrame) -> DataFrame:
    return (
        batch_df
        .select(
            "requested_city",
            "status_code",
            "raw_response",
            "call_timestamp",
            "latency_ms",
            "load_date",
            F.col("parsed.name").alias("city"),
            F.col("parsed.sys.country").alias("country"),
            F.col("parsed.coord.lat").alias("latitude"),
            F.col("parsed.coord.lon").alias("longitude"),
            F.from_unixtime(F.col("parsed.dt")).cast("timestamp").alias("event_time"),
            F.col("parsed.main.temp").alias("temperature"),
            F.col("parsed.main.feels_like").alias("feels_like_temperature"),
            F.col("parsed.main.humidity").alias("humidity"),
            F.col("parsed.main.pressure").alias("pressure"),
            F.col("parsed.wind.speed").alias("wind_speed"),
            F.col("parsed.weather")[0]["main"].alias("weather_condition"),
            F.col("parsed.weather")[0]["description"].alias("weather_description"),
            F.coalesce(F.col("parsed.rain.1h"), F.lit(0.0)).alias("rainfall"),
            F.col("parsed.visibility").alias("visibility"),
            F.col("parsed.sys.sunrise").alias("sunrise_epoch"),
            F.col("parsed.sys.sunset").alias("sunset_epoch"),
        )
        .withColumn("event_date", F.col("event_time").cast("date"))
        .withColumn("event_hour", F.hour("event_time"))
        .withColumn(
            "temperature_band",
            F.when(F.col("temperature") < 15, "Cold")
             .when(F.col("temperature") <= 30, "Normal")
             .when(F.col("temperature") <= 40, "Hot")
             .otherwise("Extreme")
        )
        .withColumn(
            "weather_severity_score",
            (
                F.when(F.col("temperature") > 40, 3).otherwise(0)
                + F.when(F.col("rainfall") > 30, 2).otherwise(0)
                + F.when(F.col("wind_speed") > 40, 2).otherwise(0)
            ).cast("int")
        )
        .withColumn(
            "is_daytime",
            F.when(
                F.col("sunrise_epoch").isNull() | F.col("sunset_epoch").isNull(),
                F.lit(None).cast("boolean")
            ).otherwise(
                F.col("event_time").between(
                    F.from_unixtime("sunrise_epoch").cast("timestamp"),
                    F.from_unixtime("sunset_epoch").cast("timestamp")
                )
            )
        )
        .withColumn("source_system", F.lit("openweathermap"))
        .withColumn("ingestion_time", F.current_timestamp())
    )


def build_weather_validation_rules(df_weather: DataFrame) -> DataFrame:
    return df_weather.withColumn(
        "quarantine_reason",
        F.when(F.col("status_code") != 200, "api_error")
         .when(F.col("city").isNull() | (F.trim(F.col("city")) == ""), "null_city")
         .when((F.col("temperature") < -50) | (F.col("temperature") > 60), "out_of_range_temperature")
         .when((F.col("humidity") < 0) | (F.col("humidity") > 100), "out_of_range_humidity")
         .when(F.col("wind_speed") < 0, "out_of_range_wind_speed")
         .when(F.col("latitude").isNull() | F.col("longitude").isNull(), "missing_coordinates")
         .when(F.col("event_time") > F.current_timestamp() + F.expr("INTERVAL 1 HOUR"), "future_event_time")
         .otherwise(F.lit(None))
    )


def quarantine_breakdown_to_json(quarantine_df: DataFrame) -> str:
    rows = quarantine_df.groupBy("quarantine_reason").count().collect()
    data = {row["quarantine_reason"]: row["count"] for row in rows}
    return json.dumps(data)


def build_validation_run_summary(
    spark: SparkSession,
    total_rows_read: int,
    valid_rows: int,
    quarantined_rows: int,
    quarantine_breakdown_json: str,
) -> DataFrame:

    return spark.createDataFrame(
        [(
            datetime.now(),
            total_rows_read,
            valid_rows,
            quarantined_rows,
            quarantine_breakdown_json,
        )],
        ["run_timestamp", "total_rows_read", "valid_rows", "quarantined_rows", "quarantine_breakdown"]
    )