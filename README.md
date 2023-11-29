# RealEstate_ETL

# Stack
1. Airflow, Docker, Python, Amazon S3, Slack

# 프로젝트 설명
1. 한국 부동산 공공 api를 이용해 매일 전날의 데이터를 MySQL에 ETL하는 DAG (Korean Real Estate Public API to MySQL ETL) Done
2. Pyspark이용해서 데이터 데이터 처리  To be done
3. 그리고 MySQL에 데이터를 Amazon S3에 적재 (MySQL to Amazon S3)  To be done
4. Data Visualation BI tools사용 (Data Visualization with BI Tools) To be done
5. Slack으로 에러 확인 (Slack Integration for Error Notification) To be done


# DAG Pipeline
Public RESTful API -> MySQL -> S3 -> SPARK -> Imagefile


# Note
## Issue
* ModuleNotFoundError No module named 'pyspark'
* Solution
* docker.yaml -> _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:- yfinance pandas numpy oauth2client gspread pyspark}

# MySQL 
![image](https://github.com/jongjunkim/RealEstate_ETL/blob/main/image/mysql%20image.PNG)

# S3 bucket
![image](https://github.com/jongjunkim/RealEstate_ETL/blob/main/image/s3done.PNG)
