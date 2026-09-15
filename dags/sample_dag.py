from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def hello_task():
    return "Hello Airflow"


def goodbye_task():
    return "Goodbye Airflow"


with DAG(
    dag_id="sample_dag",
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    hello = PythonOperator(
        task_id="hello_task",
        python_callable=hello_task
    )

    goodbye = PythonOperator(
        task_id="goodbye_task",
        python_callable=goodbye_task
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> hello >> goodbye >> end
