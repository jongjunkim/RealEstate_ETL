
import findspark
findspark.init()

import urllib.request
from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType,  IntegerType, StringType, FloatType
from pyspark.sql.functions import year, month
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pytz



# Initialize Spark session
current_datetime_utc = datetime.utcnow()

# 한국 시간대로 변환
korea_timezone = pytz.timezone('Asia/Seoul')
current_datetime_korea = current_datetime_utc.replace(tzinfo=pytz.utc).astimezone(korea_timezone)

# 포맷팅
current_date_korea = current_datetime_korea.strftime("%Y-%m-%d")

# 파일 URL 생성
file_url = f"https://jongjun.s3.us-east-2.amazonaws.com/Seoul_gu_transaction_{current_date_korea}.csv"
local_file_path = "Seoul_gu_transaction_2023-11-29.csv"

urllib.request.urlretrieve(file_url, local_file_path)

MAX_MEMORY="5g"
spark = SparkSession.builder.appName("Real-Estate-Dataframe")\
            .config("spark.executor.memory", MAX_MEMORY)\
            .config("spark.driver.memory", MAX_MEMORY)\
            .getOrCreate()



my_schema = StructType([
    StructField("TransactionID", IntegerType(), True),
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

df = spark.read.csv(local_file_path, header=True, schema=my_schema)
df.createOrReplaceTempView("real_estate_transaction")

df.show(10)

# Corrected column name in the SQL query
#이거 할때 일반 ' ' 이걸로 하는게 아니라 ` ` 1번키 옆에 있는걸로 해야함
query1 = spark.sql("""
    SELECT `DealYMD`, `거래금액`, `법정동`, `아파트`, `전용면적`, `층`, `구`
    FROM real_estate_transaction order by `DealYMD` desc, `구`
""")

query1.show()

# Stop the Spark session
spark.stop()

