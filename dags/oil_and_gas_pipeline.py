import os

from airflow import DAG
from datetime import datetime, timedelta, timezone
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


DB_NAME = os.getenv("PIPELINE_DB_NAME")
DB_USER = os.getenv("PIPELINE_DB_USER")
DB_PASSWORD = os.getenv("PIPELINE_DB_PASSWORD")
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")

mounts = [
    Mount(
        source="C:/ETL Pipeline Project/oil-and-gas-pipeline/ingestion/data",
        target="/app/data",
        type="bind",
    )
]

with DAG(
    dag_id="oil_and_gas_pipeline",
    schedule="@daily",
    description="A simple oil and gas pipeline DAG",
    start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    ingest_task = DockerOperator(
        network_mode="oil-and-gas-pipeline_default",
        task_id="ingest_data",
        image="oil-and-gas-pipeline-ingestion:latest",
        docker_url="unix://var/run/docker.sock",
        mounts=mounts,
        environment={
            "PIPELINE_DB_HOST": "postgres-pipeline",   
            "PIPELINE_DB_PORT": "5432",                
            "PIPELINE_DB_NAME": DB_NAME,
            "PIPELINE_DB_USER": DB_USER,
            "PIPELINE_DB_PASSWORD": DB_PASSWORD,
            "KAGGLE_USERNAME": KAGGLE_USERNAME,
            "KAGGLE_KEY": KAGGLE_KEY,
        },
        auto_remove="success",
    )