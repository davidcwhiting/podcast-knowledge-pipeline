FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install main dependencies (excludes dbt to avoid dependency conflicts)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install dbt in an isolated virtual environment
# Airflow triggers dbt via BashOperator using this venv's python
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-bigquery>=1.8.0

ENV DBT_VENV_PATH=/home/airflow/dbt-venv/bin

WORKDIR /opt/airflow
