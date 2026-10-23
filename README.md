# Weather DuckDB ETL

An hourly Apache Airflow pipeline that fetches weather for five cities,
validates each reading, and stores it in DuckDB. Manual runs can also request
up to 30 days of hourly history. DuckDB upserts prevent duplicate city-hour
records when a run is repeated.

See the [one-page project summary](project_summary.html) for an overview of the
pipeline, data checks, analytics, and local run steps.

## Requirements

- Windows with Docker Desktop installed and its WSL 2 engine enabled.
- At least 4 GB of memory available to Docker Desktop.

Airflow is not supported as a native Windows service. This project runs Airflow
in the official Linux-based Airflow image with Python 3.12; a local Python
installation is not needed.

## Start the project

Open PowerShell in the project directory and run:

```powershell
docker compose up --build -d
docker compose logs -f airflow
```

Wait for the standalone Airflow service to start. Its logs include the generated
admin username and password. Open [http://localhost:8080](http://localhost:8080)
and sign in. The `weather_duckdb_etl` DAG is enabled and scheduled hourly; its
scheduled runs load the latest reading for each city.

### Run a 30-day backfill

In Airflow, select **Trigger DAG** and set the `backfill_days` parameter in the
trigger form to `30`. The equivalent JSON configuration is:

```json
{"backfill_days": 30}
```

Choose any integer from `1` to `30`. The backfill retrieves hourly readings for
the selected rolling window, excludes timestamps outside that window, and
upserts by `(city, timestamp)`. A 30-day window is approximately 3,600
city-hour readings across five cities. Re-running a backfill updates existing
keys rather than duplicating them. Leave the value at `0` for a normal current
weather run.

To stop the services without deleting stored data:

```powershell
docker compose down
```

## Data quality and analytics

The pipeline checks for missing or malformed values, duplicate city-hour keys,
temperatures outside `-50°C` to `60°C`, humidity outside `0%` to `100%`,
non-positive pressure, and negative wind speeds. It creates these DuckDB views:

- `hourly_weather_summary` - hourly metric averages by city.
- `daily_city_weather_summary` - daily temperature min/average/max and other
  metric averages by city.
- `hourly_city_weather_comparison` - same-hour temperature and humidity
  differences between each pair of cities.

## Verify the stored readings

The DuckDB database is persisted at `data/weather_data.db`. Query it from the
Airflow container:

```powershell
docker compose exec airflow python -c "import duckdb; con = duckdb.connect('/opt/airflow/data/weather_data.db', read_only=True); print(con.execute('SELECT * FROM city_weather_metrics ORDER BY timestamp DESC, city').fetchdf().to_string(index=False)); con.close()"
```

For a summary:

```powershell
docker compose exec airflow python -c "import duckdb; con = duckdb.connect('/opt/airflow/data/weather_data.db', read_only=True); print(con.execute('SELECT COUNT(*) AS total_readings, ROUND(AVG(temperature_celsius), 2) AS avg_temp_c, ROUND(AVG(humidity_pct), 2) AS avg_humidity_pct FROM city_weather_metrics').fetchdf().to_string(index=False)); con.close()"
```

To query the analytics views, replace the query string above with
`SELECT * FROM daily_city_weather_summary ORDER BY date DESC, city`.

The project folder `airflow_home` holds Airflow's local configuration, logs,
and metadata database. Both it and the weather database are excluded from Git.
