# spark 
import findspark
findspark.init()

import urllib.request
from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType,  IntegerType, StringType, FloatType



# Initialize Spark session

file_url = "https://jongjun.s3.us-east-2.amazonaws.com/Seoul_gu_transaction_2023-11-29.csv"
local_file_path = "Seoul_gu_transaction_2023-11-29.csv"

urllib.request.urlretrieve(file_url, local_file_path)

MAX_MEMORY="5g"
spark = SparkSession.builder.appName("Real-Estate-Dataframe")\
            .config("spark.executor.memory", MAX_MEMORY)\
            .config("spark.driver.memory", MAX_MEMORY)\
            .getOrCreate()



my_schema = StructType([
    StructField("TransactionID", IntegerType(), True),
    StructField("DEAL-YMD", StringType(), True),
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

df.show(10)


# Stop the Spark session
spark.stop()

