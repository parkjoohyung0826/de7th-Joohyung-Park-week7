from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def count_lines(**context):
    """
    첫 번째 시도에서는 실패시키고,
    재시도에서는 값을 return 한다.
    """
    ti = context["ti"]

    if ti.try_number == 1:
        raise ValueError("Intentional failure on first attempt")

    result = 42
    return result


def use_value(**context):
    ti = context["ti"]

    value = ti.xcom_pull(
        task_ids="count_lines",
        key="return_value",
    )

    print(f"received from XCom: {value}")
    print(f"doubled = {value * 2}")


with DAG(
    dag_id="xcom_demo_parkjoohyung",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
) as dag:

    count_lines_task = PythonOperator(
        task_id="count_lines",
        python_callable=count_lines,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    use_value_task = PythonOperator(
        task_id="use_value",
        python_callable=use_value,
    )

    count_lines_task >> use_value_task
