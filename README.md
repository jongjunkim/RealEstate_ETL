# RealEstate_ETL

# Stack
1. Airflow, Docker, Python, Amazon S3, Slack

# 프로젝트 설명
1. 한국 부동산 공공 api를 이용해 매일 전날의 데이터를 MySQL에 ETL하는 DAG (Korean Real Estate Public API to MySQL ETL) Done
2. 그리고 MySQL에 데이터를 Amazon S3에 적재 (MySQL to Amazon S3)  Done
3. S3에 저장되어이 있는 데이터를 Pyspark이용해서 ELT (Meaningful data) To be done
4. Data Visualation BI tools사용 (Data Visualization with BI Tools) To be done
5. Slack으로 에러 확인 (Slack Integration for Error Notification) To be done


# DAG Pipeline
Public RESTful API -> MySQL -> S3 -> SPARK(ELT) -> Imagefile


# Note
## Issue
### ModuleNotFoundError No module named 'pyspark'
#### Solution
* docker.yaml -> _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:- yfinance pandas numpy oauth2client gspread pyspark}

### Airflow Connection type missing Spark
#### Solution
* Dockerfile에 아래와 같은 형식으로 저장 뒤에 airflow와 spark는 컴퓨터 다운로드된 버전으로
  
![image](https://github.com/jongjunkim/RealEstate_ETL/blob/main/image/dockerfile.PNG)


* docker-compose.yaml 파일에 가서 #build: . -> build .

![image](https://github.com/jongjunkim/RealEstate_ETL/blob/main/image/docker.PNG)

* docker-compose build 하고 docker compose up하면 끝

# MySQL 
![image](https://github.com/jongjunkim/RealEstate_ETL/blob/main/image/mysql%20image.PNG)

# S3 bucket
![image](https://github.com/jongjunkim/RealEstate_ETL/blob/main/image/s3done.PNG)


