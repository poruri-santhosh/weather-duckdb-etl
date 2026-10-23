import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import requests
from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator


API_URL = "https://api.open-meteo.com/v1/forecast"
DATABASE_PATH = Path(
    os.environ.get(
        "WEATHER_DB_PATH",
        str(Path(__file__).resolve().parents[1] / "data" / "weather_data.db"),
    )
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_weather_data() -> dict[str, Any]:
    """Fetch current weather for the Vijayawada area."""
    response = requests.get(
        API_URL,
        params={
            "latitude": 16.5062,
            "longitude": 80.6480,
            "current": "temperature_2m,relative_humidity_2m,weather_code",
            "timezone": "UTC",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Open-Meteo returned an unexpected response.")
    return payload


def _parse_current_weather(
    raw_data: dict[str, Any],
) -> tuple[datetime, float, float, int]:
    current = raw_data.get("current")
    if not isinstance(current, dict):
        raise ValueError("Open-Meteo response is missing the current weather data.")

    timestamp = current.get("time")
    temperature = current.get("temperature_2m")
    humidity = current.get("relative_humidity_2m")
    weather_code = current.get("weather_code")

    if not isinstance(timestamp, str):
        raise ValueError("Current weather timestamp is missing or invalid.")
    if isinstance(temperature, bool) or not isinstance(temperature, (int, float)):
        raise ValueError("Current temperature is missing or invalid.")
    if isinstance(humidity, bool) or not isinstance(humidity, (int, float)):
        raise ValueError("Current humidity is missing or invalid.")
    if isinstance(weather_code, bool) or not isinstance(weather_code, int):
        raise ValueError("Current weather code is missing or invalid.")

    try:
        observed_at = datetime.fromisoformat(timestamp)
    except ValueError as error:
        raise ValueError("Current weather timestamp is not a valid ISO timestamp.") from error

    return observed_at, float(temperature), float(humidity), weather_code


def transform_and_load_weather_data(ti: TaskInstance) -> None:
    """Validate the API response and insert the current reading into DuckDB."""
    raw_data = ti.xcom_pull(task_ids="extract_weather_data")
    if not isinstance(raw_data, dict):
        raise ValueError("No valid weather response was received from the extraction task.")

    observed_at, temperature, humidity, weather_code = _parse_current_weather(raw_data)
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(DATABASE_PATH))
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_logs (
                timestamp TIMESTAMP NOT NULL,
                temperature DOUBLE NOT NULL,
                humidity DOUBLE NOT NULL,
                weather_code INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            INSERT INTO weather_logs (timestamp, temperature, humidity, weather_code)
            VALUES (?, ?, ?, ?)
            """,
            [observed_at, temperature, humidity, weather_code],
        )
    finally:
        connection.close()

    print(
        f"Loaded weather reading into {DATABASE_PATH}: "
        f"temperature={temperature}°C, humidity={humidity}%"
    )


with DAG(
    dag_id="weather_duckdb_etl",
    default_args=default_args,
    description="Fetch current weather and store it in DuckDB.",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["etl", "duckdb", "weather"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data,
    )

    load_task = PythonOperator(
        task_id="transform_and_load_weather_data",
        python_callable=transform_and_load_weather_data,
    )

    extract_task >> load_task
