from datetime import datetime
import os
import subprocess

import boto3

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "de-7-parkjoohyung")

BRONZE_KEY = "bronze/netflix_titles.csv"

LOCAL_INPUT = "/opt/airflow/data/netflix_titles.csv"
LOCAL_OUTPUT = "/opt/airflow/data/silver_output"


def download_from_s3():
    s3 = boto3.client("s3")

    os.makedirs("/opt/airflow/data", exist_ok=True)

    print(
        f"Downloading s3://{BUCKET_NAME}/{BRONZE_KEY} "
        f"to {LOCAL_INPUT}"
    )

    s3.download_file(
        BUCKET_NAME,
        BRONZE_KEY,
        LOCAL_INPUT,
    )

    print("Download complete")


def run_spark_transform():
    command = [
        "spark-submit",
        "/opt/airflow/jobs/transform.py",
        "2015",
    ]

    print("Running:", " ".join(command))

    subprocess.run(
        command,
        check=True,
    )


def upload_to_s3(**context):
    s3 = boto3.client("s3")

    logical_date = context["logical_date"]
    date_str = logical_date.strftime("%Y-%m-%d")

    prefix = f"silver/{date_str}/"

    uploaded = 0

    for root, dirs, files in os.walk(LOCAL_OUTPUT):
        for filename in files:

            if not filename.endswith(".parquet"):
                continue

            local_path = os.path.join(root, filename)
            key = prefix + filename

            print(f"Uploading {local_path} -> s3://{BUCKET_NAME}/{key}")

            s3.upload_file(
                local_path,
                BUCKET_NAME,
                key,
            )

            uploaded += 1

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=prefix,
    )

    objects = response.get("Contents", [])

    print(f"Uploaded parquet files: {uploaded}")
    print(f"S3 object count under {prefix}: {len(objects)}")

    for obj in objects:
        print(obj["Key"])


with DAG(
    dag_id="weekly_pipeline_parkjoohyung",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id="download_from_s3",
        python_callable=download_from_s3,
    )

    transform_task = PythonOperator(
        task_id="spark_transform",
        python_callable=run_spark_transform,
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    download_task >> transform_task >> upload_task
