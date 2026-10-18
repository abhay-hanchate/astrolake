"""Unit tests for ingestion clients."""
import unittest
from unittest.mock import patch, MagicMock
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))


class TestExoplanetClient(unittest.TestCase):
    @patch("ingestion.exoplanets.client.requests.get")
    def test_fetch_returns_records(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            {"pl_name": "Kepler-22b", "hostname": "Kepler-22", "disc_year": 2011,
             "discoverymethod": "Transit", "pl_rade": 2.4, "pl_bmasse": None, "pl_orbper": 289.9, "sy_dist": None, "rowupdate": "2023-01-01"}
        ]
        from ingestion.exoplanets.client import fetch_exoplanets
        records = fetch_exoplanets()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].pl_name, "Kepler-22b")
        self.assertEqual(records[0].disc_year, 2011)
        self.assertAlmostEqual(records[0].pl_rade, 2.4)

    @patch("ingestion.exoplanets.client.requests.get")
    def test_fetch_handles_empty(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = []
        from ingestion.exoplanets.client import fetch_exoplanets
        records = fetch_exoplanets()
        self.assertEqual(records, [])

    @patch("ingestion.exoplanets.client.requests.get")
    def test_incremental_fetch(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = []
        from ingestion.exoplanets.client import fetch_exoplanets
        fetch_exoplanets(since_date="2024-01-01")
        call_args = mock_get.call_args[1]["params"]["query"]
        self.assertIn("rowupdate", call_args)


class TestAPODClient(unittest.TestCase):
    @patch("ingestion.nasa_apod.client.requests.get")
    def test_fetch_apod_range(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [{
            "date": "2024-01-01", "title": "Test", "media_type": "image",
            "url": "https://science.nasa.gov/test", "hdurl": "https://example.com/img.jpg",
            "explanation": "<strong>Test</strong> explanation"
        }]
        from ingestion.nasa_apod.client import fetch_apod_range
        records = fetch_apod_range("2024-01-01", "2024-01-01")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].media_type, "image")
        self.assertEqual(records[0].url, "https://example.com/img.jpg")
        self.assertEqual(records[0].explanation, "Test explanation")


class TestSpaceXClient(unittest.TestCase):
    @patch("ingestion.spacex.client.requests.get")
    def test_fetch_launches(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        def side_effect(url, **kw):
            m = MagicMock()
            m.status_code = 200
            m.json.return_value = {
                "count": 1,
                "results": [{
                    "id": "abc123",
                    "name": "Falcon 9 | CRS-1",
                    "net": "2012-10-08T00:35:00Z",
                    "status": {"abbrev": "Success", "name": "Launch Successful"},
                    "rocket": {"configuration": {"id": 164, "full_name": "Falcon 9 Block 5"}},
                    "pad": {"id": 39, "name": "KSC", "location": {"name": "Florida"}},
                    "mission": {"description": "Cargo mission"},
                    "agency_launch_attempt_count": 1,
                }],
            }
            return m
        mock_get.side_effect = side_effect
        from ingestion.spacex.client import fetch_launches
        records = fetch_launches()
        self.assertGreater(len(records), 0)
        self.assertEqual(records[0].outcome, "SUCCESS")
        self.assertEqual(records[0].rocket_name, "Falcon 9 Block 5")
        self.assertEqual(records[0].launchpad_locality, "Florida")


if __name__ == "__main__":
    unittest.main()
