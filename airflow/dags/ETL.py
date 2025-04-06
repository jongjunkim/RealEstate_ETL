from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import configparser
import os
import subprocess

# 📌 API 호출 + 저장 함수
def fetch_and_save_to_local():
    config = configparser.ConfigParser(interpolation=None)
    config.read("/opt/airflow/config/config.ini")

    api_key = config["Realestate_API_Key"]["api_key"]

    base_url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
    params = {
        "LAWD_CD": "11110",
        "DEAL_YMD": "202401",
        "serviceKey": api_key
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        os.makedirs("/opt/airflow/data", exist_ok=True)
        with open("/opt/airflow/data/apt_trade_data.xml", "w", encoding="utf-8") as f:
            f.write(response.text)
        print("데이터 저장 완료")
    else:
        raise Exception(f"Request failed with status: {response.status_code}")

def upload_to_hdfs():
    local_path = "/opt/airflow/data/apt_trade_data.xml"
    hdfs_path = "/user/airflow/apt_trade_data.xml"

    result = subprocess.run(
        ["hdfs", "dfs", "-put", "-f", local_path, hdfs_path],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print("HDFS 업로드 성공")
    else:
        print("HDFS 업로드 실패")
        print(result.stderr)
        raise Exception("HDFS upload failed")

# DAG 정의
default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="get_apt_trade_and_upload_hdfs",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["real_estate", "API", "HDFS"],
) as dag:

    fetch_data_task = PythonOperator(
        task_id="fetch_and_save_to_local",
        python_callable=fetch_and_save_to_local
    )

    upload_hdfs_task = PythonOperator(
        task_id="upload_to_hdfs",
        python_callable=upload_to_hdfs
    )

    fetch_data_task >> upload_hdfs_task
