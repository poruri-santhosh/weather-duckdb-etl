import os
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import requests
from airflow import DAG
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator

from weather_etl_utils import (
    CITIES,
    build_current_records,
    build_historical_records,
    validate_backfill_days,
    validate_records,
)


API_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_METRICS = (
    "temperature_2m,relative_humidity_2m,surface_pressure,"
    "wind_speed_10m,weather_code"
)
DATABASE_PATH = Path(
    os.environ.get(
        "WEATHER_DB_PATH",
        str(Path(__file__).resolve().parents[1] / "data" / "weather_data.db"),
    )
)

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def _get_backfill_days(context: dict[str, Any]) -> int:
    parameters = context.get("params", {})
    if not isinstance(parameters, Mapping):
        raise ValueError("DAG parameters must be a mapping.")
    return validate_backfill_days(parameters.get("backfill_days", 0))


def extract_weather_data(**context: Any) -> list[dict[str, Any]]:
    """Fetch either the latest reading or an explicitly requested hourly backfill."""
    backfill_days = _get_backfill_days(context)
    records: list[dict[str, Any]] = []
    cutoff = datetime.now(timezone.utc).replace(
        minute=0, second=0, microsecond=0, tzinfo=None
    )

    for city in CITIES:
        params: dict[str, Any] = {
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "timezone": "UTC",
        }
        if backfill_days:
            params.update(
                {
                    "hourly": HOURLY_METRICS,
                    "past_days": backfill_days,
                    "forecast_days": 1,
                }
            )
        else:
            params["current"] = HOURLY_METRICS

        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Open-Meteo returned an invalid response for {city['city']}.")

        if backfill_days:
            start_at = cutoff - timedelta(days=backfill_days) + timedelta(hours=1)
            records.extend(
                build_historical_records(
                    city, payload.get("hourly"), start_at, cutoff
                )
            )
        else:
            records.append(build_current_records(city, payload.get("current")))

    validate_records(records)
    return records


def validate_weather_data(ti: TaskInstance) -> list[dict[str, Any]]:
    """Reject incomplete, out-of-range, or duplicate city readings."""
    records = ti.xcom_pull(task_ids="extract_weather_data")
    validate_records(records)
    print(f"Data validation passed: {len(records)} weather readings verified.")
    return records


def load_weather_data(ti: TaskInstance) -> None:
    """Idempotently upsert readings and refresh the DuckDB analytics views."""
    records = ti.xcom_pull(task_ids="validate_weather_data")
    validate_records(records)
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(DATABASE_PATH))
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS city_weather_metrics (
                city VARCHAR NOT NULL,
                country VARCHAR NOT NULL,
                latitude DOUBLE NOT NULL,
                longitude DOUBLE NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                temperature_celsius DOUBLE NOT NULL,
                humidity_pct DOUBLE NOT NULL,
                pressure_hpa DOUBLE NOT NULL,
                wind_speed_kmh DOUBLE NOT NULL,
                weather_code INTEGER NOT NULL,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (city, timestamp)
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO city_weather_metrics (
                city, country, latitude, longitude, timestamp,
                temperature_celsius, humidity_pct, pressure_hpa,
                wind_speed_kmh, weather_code
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (city, timestamp) DO UPDATE SET
                country = EXCLUDED.country,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                temperature_celsius = EXCLUDED.temperature_celsius,
                humidity_pct = EXCLUDED.humidity_pct,
                pressure_hpa = EXCLUDED.pressure_hpa,
                wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                weather_code = EXCLUDED.weather_code,
                ingested_at = CURRENT_TIMESTAMP
            """,
            [
                (
                    record["city"],
                    record["country"],
                    record["latitude"],
                    record["longitude"],
                    record["timestamp"],
                    record["temperature_celsius"],
                    record["humidity_pct"],
                    record["pressure_hpa"],
                    record["wind_speed_kmh"],
                    record["weather_code"],
                )
                for record in records
            ],
        )
        connection.execute(
            """
            CREATE OR REPLACE VIEW hourly_weather_summary AS
            SELECT
                city,
                country,
                date_trunc('hour', timestamp) AS hour,
                ROUND(AVG(temperature_celsius), 2) AS avg_temp_c,
                ROUND(AVG(humidity_pct), 2) AS avg_humidity_pct,
                ROUND(AVG(pressure_hpa), 2) AS avg_pressure_hpa,
                ROUND(AVG(wind_speed_kmh), 2) AS avg_wind_speed_kmh,
                COUNT(*) AS readings_count
            FROM city_weather_metrics
            GROUP BY city, country, date_trunc('hour', timestamp)
            """
        )
        connection.execute(
            """
            CREATE OR REPLACE VIEW daily_city_weather_summary AS
            SELECT
                city,
                country,
                CAST(timestamp AS DATE) AS date,
                ROUND(AVG(temperature_celsius), 2) AS avg_temp_c,
                ROUND(MIN(temperature_celsius), 2) AS min_temp_c,
                ROUND(MAX(temperature_celsius), 2) AS max_temp_c,
                ROUND(AVG(humidity_pct), 2) AS avg_humidity_pct,
                ROUND(AVG(pressure_hpa), 2) AS avg_pressure_hpa,
                ROUND(AVG(wind_speed_kmh), 2) AS avg_wind_speed_kmh,
                COUNT(*) AS readings_count
            FROM city_weather_metrics
            GROUP BY city, country, CAST(timestamp AS DATE)
            """
        )
        connection.execute(
            """
            CREATE OR REPLACE VIEW hourly_city_weather_comparison AS
            SELECT
                left_city.timestamp,
                left_city.city AS city,
                right_city.city AS comparison_city,
                ROUND(
                    left_city.temperature_celsius
                    - right_city.temperature_celsius,
                    2
                ) AS temperature_delta_c,
                ROUND(
                    left_city.humidity_pct - right_city.humidity_pct,
                    2
                ) AS humidity_delta_pct
            FROM city_weather_metrics AS left_city
            JOIN city_weather_metrics AS right_city
                ON left_city.timestamp = right_city.timestamp
                AND left_city.city < right_city.city
            """
        )
        total_rows = connection.execute(
            "SELECT COUNT(*) FROM city_weather_metrics"
        ).fetchone()[0]
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()

    print(
        f"Upserted {len(records)} readings into {DATABASE_PATH}; "
        f"the table now contains {total_rows} unique city-hour readings."
    )


with DAG(
    dag_id="weather_duckdb_etl",
    default_args=default_args,
    description=(
        "Multi-city weather ingestion with validation, idempotent DuckDB upserts, "
        "and analytics views."
    ),
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    params={
        "backfill_days": Param(
            0,
            type="integer",
            minimum=0,
            maximum=30,
            description="Set to 1-30 for an hourly historical backfill; 0 loads current readings.",
        )
    },
    tags=["etl", "duckdb", "weather", "multi-city"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data,
    )

    validate_task = PythonOperator(
        task_id="validate_weather_data",
        python_callable=validate_weather_data,
    )

    load_task = PythonOperator(
        task_id="load_weather_data",
        python_callable=load_weather_data,
    )

    extract_task >> validate_task >> load_task
