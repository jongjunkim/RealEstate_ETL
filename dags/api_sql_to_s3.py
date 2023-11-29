from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator



dag = DAG(
    dag_id = 'Real_estate_MySQL_to_S3',
    start_date = datetime(2023,11,27), 
    schedule = '15 9 * * *', #minute hour day_of_month month day_of_week
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 5,
        'retry_delay': timedelta(minutes=5),
    }
)

sql = "SELECT * FROM real_estate_transaction;"
current_date = datetime.now().strftime("%Y-%m-%d")
file_with_date = f'Seoul_gu_transaction_{current_date}.csv'


mysql_to_s3_nps = SqlToS3Operator(
    task_id = 'my',
    query = sql,
    s3_bucket = 'jongjun',
    s3_key = file_with_date,
    sql_conn_id = "mysql_localhost",
    aws_conn_id = "aws_s3",
    verify = False,
    replace = True,
    pd_kwargs={"index": False, "header": False},    
    dag = dag
)