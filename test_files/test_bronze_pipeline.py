
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import weather_pipeline_helpers as helpers

build_weather_url = helpers.build_weather_url
fetch_weather_for_city = helpers.fetch_weather_for_city
load_config_file = helpers.load_config_file


class BronzePipelineTests(unittest.TestCase):
    def test_build_weather_url_encodes_city_name(self):
        """Unit test for URL construction.

        Input: city name, base API URL, API key, and units.
        Output: a formatted URL string containing the expected query parameters.
        Built-ins used: unittest.TestCase assertions.
        """
        url = build_weather_url(
            city="Tiruchirappalli",
            base_url="https://api.openweathermap.org/data/2.5/weather",
            api_key="demo-key",
            units="metric",
        )

        self.assertIn("q=Tiruchirappalli", url)
        self.assertIn("appid=demo-key", url)
        self.assertIn("units=metric", url)

    def test_load_config_file_reads_cities_list(self):
        """Unit test for reading config JSON.

        Input: a temporary JSON file on disk.
        Output: a Python dictionary loaded from that file.
        Built-ins used: tempfile.NamedTemporaryFile, json.dump, Path.unlink.
        """
        sample_config = {
            "api_key": "demo-key",
            "api_base_url": "https://api.openweathermap.org/data/2.5/weather",
            "units": "metric",
            "cities": ["Chennai", "Madurai", "Coimbatore"],
        }

        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(sample_config, handle)
            temp_path = handle.name

        try:
            loaded = load_config_file(temp_path)
            self.assertEqual(loaded["api_key"], "demo-key")
            self.assertEqual(len(loaded["cities"]), 3)
        finally:
            Path(temp_path).unlink(missing_ok=True)

    @patch.object(helpers.requests, "get")
    def test_fetch_weather_for_city_success(self, mock_get):
        """Unit test for the happy-path API call.

        Input: mocked HTTP 200 response from requests.get.
        Output: a result dictionary with requested_city, status_code, raw_response,
        and latency_ms.
        Built-ins used: unittest.mock.patch, Mock.
        """
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"name":"Chennai","cod":200}'
        mock_get.return_value = mock_response

        result = fetch_weather_for_city(
            cities="Chennai",
            base_url="https://api.openweathermap.org/data/2.5/weather",
            api_key="demo-key",
            units="metric",
        )

        self.assertEqual(result["requested_city"], "Chennai")
        self.assertEqual(result["status_code"], 200)
        self.assertIn("Chennai", result["raw_response"])
        self.assertGreaterEqual(result["latency_ms"], 0)

    @patch.object(helpers.requests, "get")
    def test_fetch_weather_for_city_network_error(self, mock_get):
        """Unit test for the network-failure path.

        Input: mocked exception from requests.get.
        Output: a safe fallback dictionary with status_code 0 and raw_response None.
        Built-ins used: unittest.mock.patch, Mock side_effect handling.
        """
        mock_get.side_effect = Exception("network down")

        result = fetch_weather_for_city(
            cities="Chennai",
            base_url="https://api.openweathermap.org/data/2.5/weather",
            api_key="demo-key",
            units="metric",
        )

        self.assertEqual(result["status_code"], 0)
        self.assertIsNone(result["raw_response"])
        self.assertEqual(result["requested_city"], "Chennai")


if __name__ == "__main__":
    unittest.main(verbosity=2)