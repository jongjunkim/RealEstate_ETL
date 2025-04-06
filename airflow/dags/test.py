from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("✅ Hello Airflow!")

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="test_hello_world_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["test"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world
    )
