FROM apache/airflow:2.10.2-python3.12

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt" \
    -r /requirements.txt
