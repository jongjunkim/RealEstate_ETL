import pandas as pd
from sqlalchemy import create_engine
import requests
from datetime import datetime, date, timedelta
from bs4 import BeautifulSoup
from datetime import date
import xml.etree.ElementTree
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType, DateType
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator


'''
DEAL_DATE,
"SERIAL"        = 일련번호,
"DEAL_TYPE"     = 거래유형,
"BUILD_NM"      = 아파트,
"FLOOR"         = 층,
"BUILD_YEAR"    = 건축년도,
"AREA"          = 전용면적,
"AMOUNT"        = 거래금액,
"ROAD_CD"       = 도로명코드,
"ROAD_NM"       = 도로명,
"BUILD_MAJOR"   = 도로명건물본번호코드,
"BUILD_MINOR"   = 도로명건물부번호코드,
"ROAD_SEQ"      = 도로명일련번호코드,
"BASEMENT_FLAG" = 도로명지상지하코드,
"LAND_NO"       = 지번,
"DONG_NM"       = 법정동,
"DONG_MAJOR"    = 법정동본번코드,
"DONG_MINOR"    = 법정동부번코드,
"EUBMYNDONG_CD" = 법정동읍면동코드,
"DONG_LAND_NO"  = 법정동지번코드,
"DEALER_ADDR"   = 중개사소재지,
"CANCEL_DEAL"   = 해제여부,
"CANCEL_DATE"   = 해제사유발생일

'''


spark = SparkSession.builder.master("local[*]").config("spark.driver.extraClassPath","/home/rkdlem196/mysql-connector-java-8.0.28/mysql-connector-java-8.0.28.jar").appName("pyspark").getOrCreate()

#RestAPI로 부터 데이터를 받은뒤 데이터 타입에 맞게 Parsing한후 SparkDataframe으로 변환
def getRealEstateData():

    gu_code_dict = getcitycode()
    base_date_list = getdates()

    gu_transcation = []

    for gu_code, gu_name in gu_code_dict.items():
        print(f"------------{gu_name}---------------")
        #print("------------종로구---------------")
        for base_date in base_date_list:
            response = get_data(11110, base_date)
            # Check if API response is successful
            
            if response.status_code != 200:
                print(f"Error: API request failed with status code {response.status_code}")
                continue
            
            formatted_date = datetime.strptime(str(base_date), '%Y%m').strftime('%Y-%m')
            print(f"{formatted_date}: data extracting")
            soup = BeautifulSoup(response.text, 'lxml-xml', from_encoding='euc-kr')
            items = soup.find_all("item")
            for item in items:
                transaction_data = parse(item)
                transaction_data['구'] = gu_name
                #transaction_data['구'] = "종로구"
                gu_transcation.append(transaction_data)

    print("Extract Data finished")

    real_estate_schema = StructType([
        StructField("DealYMD", StringType(), True),
        StructField("거래금액", IntegerType(), True),
        StructField("건축년도", StringType(), True),
        StructField("도로명", StringType(), True),
        StructField("법정동", StringType(), True),
        StructField("아파트", StringType(), True),
        StructField("전용면적", FloatType(), True),
        StructField("지번", StringType(), True),
        StructField("지역코드", IntegerType(), True),
        StructField("층", IntegerType(), True),
        StructField("구", StringType(), True)
    ])

    spark_dataframe = spark.createDataFrame(gu_transcation, real_estate_schema)

    
    return spark_dataframe

#URL을 통해 데이터 호출
def get_data(gu_code, date):
    url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev' 
    #api_key_utf8 = 'TeJ7AtefZIJBvZ7XigcoXmd8XmSMW8ZabeVo%2FXKoNfIu5p6gSBNJd5UU9DnWaNOEJdzK6ljdV2pjXVAmYq6QYQ%3D%3D' 

    api_key_utf8 = 'J7oQJv0rlTQKMULGGutegrlWz3H1jz%2FMJYmvuFzU5jUKgLGj6EIzbxr%2FxJQaBBSUctSSoMZm7LV8R2vZLpvGig%3D%3D' #네이버

    api_key_decode = requests.utils.unquote(api_key_utf8, encoding='utf-8') 
    params ={'serviceKey' : api_key_decode, 'LAWD_CD' : gu_code, 'DEAL_YMD' : date}
    response = requests.get(url, params=params)

    return response

#parse data 
def parse(item):
    try:
        거래금액 = int(item.find("거래금액").get_text().replace(',', '').strip())
        건축년도 = item.find("건축년도").get_text()
        거래년도 = item.find("년").get_text()
        도로명 = item.find("도로명").get_text()
        법정동 = item.find("법정동").get_text()
        아파트 = item.find("아파트").get_text()
        거래월 = str(item.find("월").get_text()).zfill(2)
        거래일 = str(item.find("일").get_text()).zfill(2)
        전용면적 = float(item.find("전용면적").get_text())
        지번 = item.find("지번").get_text()
        지역코드 = int(item.find("지역코드").get_text())
        층 = int(item.find("층").get_text()) 
        DealYMD = datetime.strptime(거래년도 + 거래월 + 거래일, '%Y%m%d').strftime('%Y-%m-%d')
        return {
            "DealYMD":DealYMD,
            "거래금액": 거래금액,
            "건축년도": 건축년도,
            "거래년도": 거래년도,
            "도로명": 도로명,
            "법정동": 법정동,
            "아파트": 아파트,
            "거래월": 거래월,
            "거래일": 거래일,
            "전용면적": 전용면적,
            "지번": 지번,
            "지역코드": 지역코드,
            "층": 층
        }
    except AttributeError as e:
        return {
            "DealYMD": None,
            "거래금액": None,
            "건축년도": None,
            "거래년도": None,
            "도로명": None,
            "법정동": None,
            "아파트": None,
            "거래월": None,
            "거래일": None,
            "전용면적": None,
            "지번": None,
            "지역코드": None,
            "층": None
        }
    except AttributeError as e:
        return None 

#구 코드를 local파일에 저장되어있는 법정동 텍스트파일에서 읽기
def getcitycode():
    code_file = r"/home/rkdlem196/airflow/data/법정동코드.txt"
    code = pd.read_csv(code_file, sep='\t')
    code.columns = ['code', 'name', 'is_exist']
    code = code[code['is_exist'] == '존재']
    code['code'] = code['code'].astype(str)

    city = '서울특별시'
    city_mask = code['name'].str.contains(city)

    #Gu_code and Dong name
    dong_to_code_dict = {row['code'][:5]: row['name'].split()[1] for index, row in code[city_mask].iloc[1:].iterrows()}

    return dong_to_code_dict

# 불러올 데이터의 날짜들을 만들어내기
def getdates():

    current_date = date.today()
    current_year = current_date.year
    current_month = current_date.month
    year = [int("%02d" % y) for y in range(2023, current_year + 1)]
    month = ["%02d"% m for m in range(1, 13)]
    base_date_list = ["%s%s" % (y, m) for y in year for m in month if (y == current_year and int(m) <= current_month) or (y < current_year)]

    return base_date_list


def get_MySQL_connection():
    # autocommit is False by default
    hook = MySqlHook(mysql_conn_id='mysql_localhost')
    return hook.get_conn().cursor()


#데이터를 MySQL로 load
def loadToMySQL():
    dataframe = getRealEstateData()

    dataframe.show(10)

    if type(dataframe) == tuple:
        dataframe[0].cache()#cache의 수명은 함수가 호출되서 끝나기까지 이므로 매 함수마다 적용
    else:
        dataframe.cache()

    if dataframe.count() == 0:
        raise ValueError("Error: DataFrame is empty. No data to insert.")
        
    return dataframe.coalesce(1).write.format("jdbc").options(
            url='jdbc:mysql://localhost:3307/airflow',
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='real_estate_transaction',
            user='root',
            password='root'
        ).mode('overwrite').save()
    


sql = """
    SELECT * FROM real_estate_transaction;
"""

current_date = datetime.now().strftime("%Y-%m-%d")
file_with_date = f'Seoul_gu_transaction_{current_date}.csv'



with DAG(
    'get_api_data',
    schedule_interval='* 9 * * *',  
    start_date=datetime(2023, 11, 21),
    catchup=False,
    tags=['RealEstate'],
    default_args={
        'retries': 12,
        'retry_delay': timedelta(minutes=1)
    }
) as dag:
    
    ETL_DATA_Mysql = PythonOperator(
        task_id="ETL_data",
        python_callable=loadToMySQL
    )

    mysql_to_s3 = SqlToS3Operator(
        task_id = 'loadS3',
        query = sql,
        s3_bucket = 'jongjun',
        s3_key = file_with_date,
        sql_conn_id = "mysql_localhost",
        aws_conn_id = "aws_s3",
        verify = False,
        replace = True,
        pd_kwargs={"index": False, "header": False, "encoding":"utf-8-sig"},    
        dag = dag
    )

    ETL_DATA_Mysql >> mysql_to_s3
