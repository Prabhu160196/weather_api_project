from __future__ import annotations

import json
import unittest
from datetime import datetime, date

import weather_pipeline_helpers as helpers
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F


def get_spark():
    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("silver-unit-tests")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )

    return spark


def build_sample_weather_json(city="Chennai", temp=30.0, humidity=50):
    return json.dumps({
        "coord": {"lon": 80.0, "lat": 13.0},
        "weather": [
            {
                "id": 801,
                "main": "Clouds",
                "description": "scattered clouds",
                "icon": "03d"
            }
        ],
        "main": {
            "temp": temp,
            "feels_like": temp + 1,
            "pressure": 1005,
            "humidity": humidity
        },
        "visibility": 6000,
        "wind": {
            "speed": 4.5,
            "deg": 120
        },
        "rain": {
            "1h": 0.0
        },
        "sys": {
            "country": "IN",
            "sunrise": 1620000000,
            "sunset": 1620040000
        },
        "timezone": 19800,
        "id": 1273194,
        "name": city,
        "cod": 200,
        "dt": 1620003600
    })


class SilverPipelineTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark()

    def make_raw_row(self, city="Chennai", status_code=200, raw_json=None):
        if raw_json is None:
            raw_json = build_sample_weather_json(city)

        return Row(
            requested_city=city,
            status_code=status_code,
            raw_response=raw_json,
            call_timestamp=datetime(2026, 5, 11, 10, 0, 0),
            latency_ms=10,
            load_date=date(2026, 5, 11)
        )

    def test_parse_batch_weather_splits_rows(self):
        good_json = build_sample_weather_json("Chennai", temp=30.0)

        bad_json = json.dumps({
            "cod": 200,
            "message": "bad response"
        })

        rows = [
            self.make_raw_row("Chennai", status_code=200, raw_json=good_json),
            self.make_raw_row("BadCity", status_code=500, raw_json=None),
            self.make_raw_row("BrokenJson", status_code=200, raw_json=bad_json),
        ]

        df = self.spark.createDataFrame(rows)

        api_errors, unparseable_rows, parsed_only_rows = helpers.parse_batch_weather(df)

        self.assertEqual(api_errors.count(), 1)
        self.assertEqual(unparseable_rows.count(), 1)
        self.assertEqual(parsed_only_rows.count(), 1)

    def test_extract_and_enrich_weather_adds_expected_columns(self):
        raw_json = build_sample_weather_json("Madurai", temp=35.0)

        rows = [
            self.make_raw_row("Madurai", status_code=200, raw_json=raw_json)
        ]

        df = self.spark.createDataFrame(rows)

        _, _, parsed_only = helpers.parse_batch_weather(df)
        enriched = helpers.extract_and_enrich_weather(parsed_only)

        cols = enriched.columns

        self.assertIn("city", cols)
        self.assertIn("temperature", cols)
        self.assertIn("event_time", cols)
        self.assertIn("event_date", cols)
        self.assertIn("event_hour", cols)
        self.assertIn("temperature_band", cols)
        self.assertIn("weather_severity_score", cols)
        self.assertIn("is_daytime", cols)
        self.assertIn("source_system", cols)
        self.assertIn("ingestion_time", cols)

    def test_build_validation_rules_flags_quarantine(self):
        raw_json = build_sample_weather_json("HotTown", temp=99.0)

        rows = [
            self.make_raw_row("HotTown", status_code=200, raw_json=raw_json)
        ]

        df = self.spark.createDataFrame(rows)

        _, _, parsed_only = helpers.parse_batch_weather(df)
        enriched = helpers.extract_and_enrich_weather(parsed_only)
        validated = helpers.build_weather_validation_rules(enriched)

        row = validated.collect()[0]

        self.assertEqual(row["quarantine_reason"], "out_of_range_temperature")

    def test_validation_splits_valid_and_quarantine_rows(self):
        good_json = build_sample_weather_json("Chennai", temp=30.0)
        bad_json = build_sample_weather_json("HotTown", temp=99.0)

        rows = [
            self.make_raw_row("Chennai", status_code=200, raw_json=good_json),
            self.make_raw_row("HotTown", status_code=200, raw_json=bad_json),
        ]

        df = self.spark.createDataFrame(rows)

        _, _, parsed_only = helpers.parse_batch_weather(df)
        enriched = helpers.extract_and_enrich_weather(parsed_only)
        validated = helpers.build_weather_validation_rules(enriched)

        valid_count = validated.filter(F.col("quarantine_reason").isNull()).count()
        quarantine_count = validated.filter(F.col("quarantine_reason").isNotNull()).count()

        self.assertEqual(valid_count, 1)
        self.assertEqual(quarantine_count, 1)

    def test_quarantine_breakdown_to_json(self):
        rows = [
            Row(quarantine_reason="api_error"),
            Row(quarantine_reason="api_error"),
            Row(quarantine_reason="null_city"),
        ]

        df = self.spark.createDataFrame(rows)

        result = helpers.quarantine_breakdown_to_json(df)
        parsed = json.loads(result)

        self.assertEqual(parsed["api_error"], 2)
        self.assertEqual(parsed["null_city"], 1)

    def test_build_validation_run_summary(self):
        summary_df = helpers.build_validation_run_summary(
            spark=self.spark,
            total_rows_read=10,
            valid_rows=8,
            quarantined_rows=2,
            quarantine_breakdown_json='{"api_error": 2}'
        )

        self.assertEqual(summary_df.count(), 1)

        row = summary_df.collect()[0]

        self.assertEqual(row["total_rows_read"], 10)
        self.assertEqual(row["valid_rows"], 8)
        self.assertEqual(row["quarantined_rows"], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)