# Weather DuckDB ETL

An hourly Apache Airflow pipeline that fetches current weather for Vijayawada,
validates the response, and appends it to a local DuckDB database.

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
and sign in. The `weather_duckdb_etl` DAG is enabled and scheduled hourly; use
the Trigger DAG button to run it immediately.

To stop the services without deleting stored data:

```powershell
docker compose down
```

## Verify the stored readings

The DuckDB database is persisted at `data/weather_data.db`. Query it from the
Airflow container:

```powershell
docker compose exec airflow python -c "import duckdb; con = duckdb.connect('/opt/airflow/data/weather_data.db', read_only=True); print(con.execute('SELECT * FROM weather_logs ORDER BY created_at DESC').fetchdf().to_string(index=False)); con.close()"
```

For a summary:

```powershell
docker compose exec airflow python -c "import duckdb; con = duckdb.connect('/opt/airflow/data/weather_data.db', read_only=True); print(con.execute('SELECT COUNT(*) AS total_runs, ROUND(AVG(temperature), 2) AS avg_temp, ROUND(AVG(humidity), 2) AS avg_humidity FROM weather_logs').fetchdf().to_string(index=False)); con.close()"
```

The project folder `airflow_home` holds Airflow's local configuration, logs,
and metadata database. Both it and the weather database are excluded from Git.
