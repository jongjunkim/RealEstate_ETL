# RealEstate_ETL

# Stack
1. Airflow, Docker, Python, Amazon S3, Slack, MySQL

# 초기 프로젝트 설명
1. 한국 부동산 공공 api를 이용해 매일 전날의 데이터를 MySQL에 ETL하는 DAG (Korean Real Estate Public API to MySQL ETL) Done
2. 그리고 MySQL에 데이터를 Amazon S3에 적재 (MySQL to Amazon S3)  Done
3. S3에 저장되어이 있는 데이터를 Pyspark이용해서 ELT (Meaningful data) Done
4. Slack으로 에러 확인 (Slack Integration for Error Notification) Done
5. Data Visualation BI tools사용 (Data Visualization with BI Tools) X

# 프로젝트 변경사항
1. 맨처음 Window 환경에서 돌렸으나 현업에서 linux를 많이 쓰기에 linux환경에 익숙해 지기 위해 우분투를 이용해 서버 구축
2. Data processing을 기존에는 python Dataframe을 이용했으나 추후에 빅데이터를 처리하기 위해서는 Spark를 이용해야하기 때문에 Spark Dataframe을 이용해서 Data preprocessing 및 analysis
3. BI tools를 이용하려 했지만 Node.js를 이용해서 사이트에 가격 그래프를 만들어보는게 더 개발에 있어서 의미 있다고 생각되어서 Visualization 구현

# DAG Pipeline
Public RESTful API -> Spark -> MySQL -> S3 -> Data visulaization(Node.js)
- **Volume**
# Note
## Issue
1. **ModuleNotFoundError No module named 'pyspark'**
    - docker.yaml -> _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:- yfinance pandas numpy oauth2client gspread pyspark}

2. **Airflow Connection type missing Spark**
    - Dockerfile에 아래와 같은 형식으로 저장 뒤에 airflow와 spark는 컴퓨터 다운로드된 버전으로
  
![image](https://github.com/jongjunkim/RealEstate_ETL/blob/main/image/dockerfile.PNG)


    - docker-compose.yaml 파일에 가서 #build: . -> build .

![image](https://github.com/jongjunkim/RealEstate_ETL/blob/main/image/docker.PNG)

    - docker-compose build 하고 docker compose up하면 끝

3. **root@localhost access denied error**
    - 이번 에러는 window에서 ubuntu로 환경을 옮겨줬을때 생겨난 이슈
    - Ubuntu환경에 있는 Mysql과 Airflow를 connect하려했지만 자꾸 이 에러가 뜸
    - 아무래도 ubuntu 와 window에 둘다 MySQL 있고 이미 Window에 설치되어있는 MySQL port가 3306을 쓰고있고 ubuntu MySQL도 3306을 쓰려하니 ubuntu MySQL계정을 인식을 못한다고 생각
    - ubuntu에 있는 MySQL port를 3307로 변경하고 host도 내 아이피 주소가 아닌 127.01.01로 바꿔주니 연결됨

# MySQL 
![image](https://github.com/jongjunkim/RealEstate_ETL/blob/main/image/mysql%20image.PNG)

# S3 bucket
![image](https://github.com/jongjunkim/RealEstate_ETL/blob/main/image/s3done.PNG)

# Ubuntu로 환경옮기고 난후 Airflow 스케쥴링해서 MySQL에 데이터 적재
![image](https://github.com/jongjunkim/RealEstate_ETL/blob/main/image/spark%20data.PNG)

# Node.Js 와 Express 이용해서 간단한 가격 그래프 구현
![image](https://github.com/jongjunkim/RealEstate_ETL/blob/main/image/%EB%91%90%EC%82%B0%EC%95%84%ED%8C%8C%ED%8A%B8%20%EA%B0%80%EA%B2%A9.PNG)

