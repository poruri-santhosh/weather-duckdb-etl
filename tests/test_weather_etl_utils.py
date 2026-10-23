import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from weather_etl_utils import (  # noqa: E402
    CITIES,
    build_current_records,
    build_historical_records,
    validate_backfill_days,
    validate_records,
)


def current_payload(time: str = "2026-10-23T12:00") -> dict[str, object]:
    return {
        "time": time,
        "temperature_2m": 25.5,
        "relative_humidity_2m": 70,
        "surface_pressure": 1012.3,
        "wind_speed_10m": 8.2,
        "weather_code": 2,
    }


class WeatherETLUtilsTests(unittest.TestCase):
    def test_build_current_record_parses_all_metrics(self) -> None:
        record = build_current_records(CITIES[0], current_payload())

        self.assertEqual(record["city"], "Vijayawada")
        self.assertEqual(record["timestamp"], datetime(2026, 10, 23, 12))
        self.assertEqual(record["temperature_celsius"], 25.5)
        self.assertEqual(record["weather_code"], 2)

    def test_historical_records_filter_future_hours(self) -> None:
        hourly = {
            "time": [
                "2026-10-23T09:00",
                "2026-10-23T10:00",
                "2026-10-23T11:00",
                "2026-10-23T12:00",
            ],
            "temperature_2m": [23, 24, 25, 26],
            "relative_humidity_2m": [55, 60, 65, 70],
            "surface_pressure": [1009, 1010, 1011, 1012],
            "wind_speed_10m": [4, 5, 6, 7],
            "weather_code": [0, 1, 2, 3],
        }

        records = build_historical_records(
            CITIES[0],
            hourly,
            datetime(2026, 10, 23, 10),
            datetime(2026, 10, 23, 11),
        )

        self.assertEqual(len(records), 2)
        self.assertEqual(records[-1]["timestamp"], datetime(2026, 10, 23, 11))

    def test_thirty_day_backfill_contains_exactly_seven_hundred_twenty_hours(self) -> None:
        cutoff = datetime(2026, 10, 23, 11)
        start_at = cutoff - timedelta(days=30) + timedelta(hours=1)
        timestamps = [
            (start_at + timedelta(hours=hour)).isoformat(timespec="minutes")
            for hour in range(30 * 24)
        ]
        hourly = {
            "time": timestamps,
            "temperature_2m": [25] * len(timestamps),
            "relative_humidity_2m": [60] * len(timestamps),
            "surface_pressure": [1012] * len(timestamps),
            "wind_speed_10m": [5] * len(timestamps),
            "weather_code": [1] * len(timestamps),
        }

        records = build_historical_records(CITIES[0], hourly, start_at, cutoff)

        self.assertEqual(len(records), 30 * 24)
        self.assertEqual(records[0]["timestamp"], start_at)
        self.assertEqual(records[-1]["timestamp"], cutoff)

    def test_rejects_temperature_outside_physical_range(self) -> None:
        payload = current_payload()
        payload["temperature_2m"] = 61

        with self.assertRaisesRegex(ValueError, "Temperature is outside"):
            build_current_records(CITIES[0], payload)

    def test_rejects_duplicate_city_timestamps(self) -> None:
        record = build_current_records(CITIES[0], current_payload())

        with self.assertRaisesRegex(ValueError, "Duplicate weather reading"):
            validate_records([record, record])

    def test_rejects_incomplete_hourly_series(self) -> None:
        hourly = {
            "time": ["2026-10-23T10:00", "2026-10-23T11:00"],
            "temperature_2m": [24],
            "relative_humidity_2m": [60, 65],
            "surface_pressure": [1010, 1011],
            "wind_speed_10m": [5, 6],
            "weather_code": [1, 2],
        }

        with self.assertRaisesRegex(ValueError, "temperature_2m is incomplete"):
            build_historical_records(
                CITIES[0],
                hourly,
                datetime(2026, 10, 23, 10),
                datetime(2026, 10, 23, 11),
            )

    def test_backfill_days_must_be_between_zero_and_thirty(self) -> None:
        self.assertEqual(validate_backfill_days(30), 30)
        self.assertEqual(validate_backfill_days(0), 0)
        for value in (-1, 31, True, 1.5, "30"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "between 0 and 30"):
                    validate_backfill_days(value)


if __name__ == "__main__":
    unittest.main()
