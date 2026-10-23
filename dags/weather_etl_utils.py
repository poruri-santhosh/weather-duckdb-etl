from datetime import datetime
from math import isfinite
from numbers import Real
from typing import Any, TypedDict


class City(TypedDict):
    city: str
    country: str
    latitude: float
    longitude: float


class WeatherRecord(City):
    timestamp: datetime
    temperature_celsius: float
    humidity_pct: float
    pressure_hpa: float
    wind_speed_kmh: float
    weather_code: int


CITIES: tuple[City, ...] = (
    {"city": "Vijayawada", "country": "India", "latitude": 16.5062, "longitude": 80.6480},
    {"city": "Hyderabad", "country": "India", "latitude": 17.3850, "longitude": 78.4867},
    {"city": "Bengaluru", "country": "India", "latitude": 12.9716, "longitude": 77.5946},
    {"city": "London", "country": "UK", "latitude": 51.5074, "longitude": -0.1278},
    {"city": "New York", "country": "USA", "latitude": 40.7128, "longitude": -74.0060},
)

METRIC_KEYS = {
    "temperature_celsius": "temperature_2m",
    "humidity_pct": "relative_humidity_2m",
    "pressure_hpa": "surface_pressure",
    "wind_speed_kmh": "wind_speed_10m",
    "weather_code": "weather_code",
}


def validate_backfill_days(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 30:
        raise ValueError("backfill_days must be an integer between 0 and 30.")
    return value


def _parse_timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Weather timestamp is missing or invalid.")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError("Weather timestamp is not a valid ISO timestamp.") from error
    if parsed.tzinfo is not None:
        raise ValueError("Weather timestamps must be in UTC without a timezone offset.")
    return parsed


def _parse_metric(value: Any, name: str) -> float | int:
    if isinstance(value, bool) or not isinstance(value, Real) or not isfinite(value):
        raise ValueError(f"Weather metric {name} is missing or invalid.")
    if name == "weather_code":
        if not isinstance(value, int):
            raise ValueError("Weather code must be an integer.")
        return value
    return float(value)


def _build_record(
    city: City,
    timestamp: Any,
    values: dict[str, Any],
) -> WeatherRecord:
    record: WeatherRecord = {
        **city,
        "timestamp": _parse_timestamp(timestamp),
        "temperature_celsius": _parse_metric(
            values.get(METRIC_KEYS["temperature_celsius"]), "temperature_celsius"
        ),
        "humidity_pct": _parse_metric(
            values.get(METRIC_KEYS["humidity_pct"]), "humidity_pct"
        ),
        "pressure_hpa": _parse_metric(
            values.get(METRIC_KEYS["pressure_hpa"]), "pressure_hpa"
        ),
        "wind_speed_kmh": _parse_metric(
            values.get(METRIC_KEYS["wind_speed_kmh"]), "wind_speed_kmh"
        ),
        "weather_code": _parse_metric(
            values.get(METRIC_KEYS["weather_code"]), "weather_code"
        ),
    }
    return record


def build_current_records(city: City, current: Any) -> WeatherRecord:
    if not isinstance(current, dict):
        raise ValueError(f"Current weather is missing for {city['city']}.")
    record = _build_record(city, current.get("time"), current)
    validate_records([record])
    return record


def build_historical_records(
    city: City,
    hourly: Any,
    start_at: datetime,
    cutoff: datetime,
) -> list[WeatherRecord]:
    if not isinstance(hourly, dict):
        raise ValueError(f"Hourly weather is missing for {city['city']}.")

    timestamps = hourly.get("time")
    if not isinstance(timestamps, list):
        raise ValueError(f"Hourly timestamps are missing for {city['city']}.")
    metric_values: dict[str, list[Any]] = {}
    for key in METRIC_KEYS.values():
        values = hourly.get(key)
        if not isinstance(values, list) or len(values) != len(timestamps):
            raise ValueError(f"Hourly metric {key} is incomplete for {city['city']}.")
        metric_values[key] = values

    records = []
    for index, timestamp in enumerate(timestamps):
        observed_at = _parse_timestamp(timestamp)
        if observed_at < start_at or observed_at > cutoff:
            continue
        values = {key: data[index] for key, data in metric_values.items()}
        records.append(_build_record(city, timestamp, values))

    if not records:
        raise ValueError(f"No historical weather readings were returned for {city['city']}.")
    validate_records(records)
    return records


def validate_records(records: Any) -> None:
    if not isinstance(records, list) or not records:
        raise ValueError("Weather extraction returned no readings.")

    seen: set[tuple[str, datetime]] = set()
    required_fields = {
        "city",
        "country",
        "latitude",
        "longitude",
        "timestamp",
        "temperature_celsius",
        "humidity_pct",
        "pressure_hpa",
        "wind_speed_kmh",
        "weather_code",
    }

    for record in records:
        if not isinstance(record, dict) or not required_fields.issubset(record):
            raise ValueError("Weather reading is missing required fields.")
        if not isinstance(record["city"], str) or not record["city"].strip():
            raise ValueError("Weather reading has an invalid city.")
        if not isinstance(record["country"], str) or not record["country"].strip():
            raise ValueError("Weather reading has an invalid country.")
        if not isinstance(record["timestamp"], datetime):
            raise ValueError("Weather reading has an invalid timestamp.")

        key = (record["city"], record["timestamp"])
        if key in seen:
            raise ValueError(
                f"Duplicate weather reading for {record['city']} at {record['timestamp']}."
            )
        seen.add(key)

        for name in (
            "latitude",
            "longitude",
            "temperature_celsius",
            "humidity_pct",
            "pressure_hpa",
            "wind_speed_kmh",
        ):
            value = record[name]
            if isinstance(value, bool) or not isinstance(value, Real) or not isfinite(value):
                raise ValueError(f"Weather metric {name} is missing or invalid.")
        code = record["weather_code"]
        if isinstance(code, bool) or not isinstance(code, int):
            raise ValueError("Weather code is missing or invalid.")
        if not -50 <= record["temperature_celsius"] <= 60:
            raise ValueError("Temperature is outside the allowed range of -50°C to 60°C.")
        if not 0 <= record["humidity_pct"] <= 100:
            raise ValueError("Humidity is outside the allowed range of 0% to 100%.")
        if record["pressure_hpa"] <= 0:
            raise ValueError("Pressure must be greater than zero.")
        if record["wind_speed_kmh"] < 0:
            raise ValueError("Wind speed cannot be negative.")
