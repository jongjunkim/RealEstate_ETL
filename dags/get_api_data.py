from datetime import datetime, date, timedelta
import pandas as pd
from sqlalchemy import create_engine
import requests
from datetime import datetime, date, timedelta
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date
import xml.etree.ElementTree
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from plugins import slack 
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

def extract():

    gu_code_dict = getcitycode()
    base_date_list = getdates()

    gu_transcation = []

    for gu_code, gu_name in gu_code_dict.items():
        print(f"------------{gu_name}---------------")
        for base_date in base_date_list:
            response = get_data(gu_code, base_date)
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
                gu_transcation.append(transaction_data)
    
    print("Extract Data finished")
    
    df = pd.DataFrame(gu_transcation)
    
    return df

#Extract
def get_data(gu_code, date):
    url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev' 
    api_key_utf8 = 'TeJ7AtefZIJBvZ7XigcoXmd8XmSMW8ZabeVo%2FXKoNfIu5p6gSBNJd5UU9DnWaNOEJdzK6ljdV2pjXVAmYq6QYQ%3D%3D' 
    api_key_decode = requests.utils.unquote(api_key_utf8, encoding='utf-8') 
    params ={'serviceKey' : api_key_decode, 'LAWD_CD' : gu_code, 'DEAL_YMD' : date}
    response = requests.get(url, params=params)

    return response

#Transform data 
def parse(item):
    try:
        거래금액 = item.find("거래금액").get_text()
        건축년도 = item.find("건축년도").get_text()
        거래년도 = item.find("년").get_text()
        도로명 = item.find("도로명").get_text()
        법정동 = item.find("법정동").get_text()
        아파트 = item.find("아파트").get_text()
        거래월 = item.find("월").get_text()
        거래일 = item.find("일").get_text()
        전용면적 = item.find("전용면적").get_text()
        지번 = item.find("지번").get_text()
        지역코드 = item.find("지역코드").get_text()
        층 = item.find("층").get_text()
        return {
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


#get gu code in Seoul from text file
def getcitycode():
    code_file = r"/opt/airflow/data/법정동코드.txt"
    #code_file = r"C:\Users\JONGJUN KIM\OneDrive\Desktop\ETL프로젝트\법정동코드.txt"
    code = pd.read_csv(code_file, sep='\t')
    code.columns = ['code', 'name', 'is_exist']
    code = code[code['is_exist'] == '존재']
    code['code'] = code['code'].astype(str)

    city = '서울특별시'
    city_mask = code['name'].str.contains(city)

    #Gu_code and Dong name
    dong_to_code_dict = {row['code'][:5]: row['name'].split()[1] for index, row in code[city_mask].iloc[1:].iterrows()}

    return dong_to_code_dict


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

def load():
    df = extract()
    cur = get_MySQL_connection()
    print(df.head())

    # Save DataFrame to a CSV file
    #df.to_csv(r"/opt/airflow/data/gu_transaction_data.csv", encoding='utf-8-sig', index=False)
    print(f"DataFrame saved to gu_transaction_data")

    if len(df) == 0:
        raise ValueError("Error: DataFrame is empty. No data to insert.")
        

    rows_to_insert = []

    for index, row in df.iterrows():

        if any(pd.isnull(row)):
            print(f"Skipped row {index + 1} because it contains None values.")
            continue

        거래금액 = int(row['거래금액'].replace(',', '').replace(' ', ''))
        건축년도 = (row['건축년도'])
        거래년도 = (row['거래년도'])
        도로명 = (row['도로명'])
        법정동 = (row['법정동'])
        아파트 = (row['아파트'])
        거래월 = str(row['거래월']).zfill(2)  # Zero-padding the month
        거래일 = str(row['거래일']).zfill(2)  # Zero-padding the day
        전용면적 = float((row['전용면적']))
        지번 = (row['지번'])
        지역코드 = (row['지역코드'])
        층 = int((row['층']))
        구 = (row['구'])
        DealYMD = datetime.strptime(str(거래년도) + str(거래월) + str(거래일), '%Y%m%d').strftime('%Y-%m-%d')
        values = (DealYMD, 거래금액, 건축년도, 도로명, 법정동, 아파트, 전용면적, 지번, 지역코드, 층, 구)
        rows_to_insert.append(values)
    
    sql = "INSERT INTO Real_Estate_Transaction (DealYMD, 거래금액, 건축년도, 도로명, 법정동, 아파트, 전용면적, 지번, 지역코드, 층, 구) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cur.executemany(sql, rows_to_insert)

    cur.execute("COMMIT;") #commit안하면 위에 있는 것들 insert하더라도 MYSQL테이블에 들어가지 못함
    print(f"{len(df)} rows inserted into Real_Estate_Transaction table.")

    

def table_exists(cur, table_name):
    return cur.execute("SHOW TABLES LIKE 'real_estate_transaction'")


#To manage data with Full-Refresh
def truncate_table(table_name, **kwargs):
    # Use MySqlHook for MySQL connection
    hook = MySqlHook(mysql_conn_id='mysql_localhost')
    cur = hook.get_sqlalchemy_engine()

    # Check if the table exists
    if table_exists(cur, table_name):
        try:
            # Truncate the existing table
            cur.execute(f"TRUNCATE TABLE {table_name}")
            cur.execute("COMMIT;")
            print(f"Table '{table_name}' truncated successfully.")
        except Exception as truncate_error:
            print(f"Error truncating table: {truncate_error}")
    else:
        print(f"Table '{table_name}' does not exist.")



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
        'retry_delay': timedelta(minutes=1),
        'on_failure_callback': slack.on_failure_callback,
    }
) as dag:
    
    truncate_existing_data_task = PythonOperator(
        task_id="truncate_existing_data",
        python_callable=truncate_table,
        op_args=['Real_Estate_Transaction'],
        provide_context=True
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load
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

    truncate_existing_data_task >> load_task >> mysql_to_s3

