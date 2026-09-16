from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


OUTPUT_DIR = Path("/opt/airflow/logs/q7_backfill")


def write_daily_file(**context):
    logical_date = context["logical_date"]

    date_str = logical_date.strftime("%Y-%m-%d")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    file_path = OUTPUT_DIR / f"daily_{date_str}.txt"

    file_path.write_text(
        f"logical_date={date_str}\n",
        encoding="utf-8",
    )

    print(f"created: {file_path}")


def verify_file(**context):
    logical_date = context["logical_date"]
    date_str = logical_date.strftime("%Y-%m-%d")

    file_path = OUTPUT_DIR / f"daily_{date_str}.txt"

    if not file_path.exists():
        raise FileNotFoundError(file_path)

    print(f"verified: {file_path}")


with DAG(
    dag_id="backfill_demo_parkjoohyung",

    # 2026-09-16 기준 7일 전
    start_date=datetime(2026, 9, 9),

    schedule="0 0 * * *",
    catchup=True,
) as dag:

    write_daily_file_task = PythonOperator(
        task_id="write_daily_file",
        python_callable=write_daily_file,
    )

    verify_file_task = PythonOperator(
        task_id="verify_file",
        python_callable=verify_file,
    )

    write_daily_file_task >> verify_file_task
