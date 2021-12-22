


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from datetime import datetime, timedelta, timezone,date
import yfinance as yf
import pandas as pd

from pyspark.sql import Row

#conf=SparkConf() \
#    .setAppName("sparkdf") \
#    .set("spark.jars", "/opt/airflow/dags/gcs-connector-hadoop2-2.1.6.jar") \
#    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
#    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/dags/vyshgk999_key.json")

spark=SparkSession.builder.appName('sparkdf').config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta').config("spark.jars","https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar").config("spark.sql.warehouse.dir", "gs://hive_warehouse1/datasets").enableHiveSupport().getOrCreate()

spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/dags/vyshgk999_key.json")
spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")



#.config('spark.jars','gs://hive-warehouse1/datasets/crypto_code/gcs-connector-hadoop2-2.1.6.jar')


#schema = StructType([StructField('a', IntegerType(), True),
#                     StructField('b', IntegerType(), True),
#                     StructField('c', StringType(), True)])
#df1 = spark.createDataFrame([Row(a=1, b=4, c='GFG1'),Row(a=2, b=8, c='GFG2'),Row(a=4, b=5, c='GFG3')],schema=schema)


def get_data():
	crypto=['BTC-USD','ETH-USD','BNB-USD','USDT-USD','ADA-USD','XRP-USD','HEX-USD','USDC-USD']
	df=pd.DataFrame()
	for c in crypto:
		data=yf.download(tickers=c,period='15m',interval='1m')
		df1=pd.DataFrame(data)
		df1['Crypto']=c
		df=df.append(df1)

	df.reset_index(inplace=True)
	

	sdf=spark.createDataFrame(df)
	sdf=sdf.withColumnRenamed('Datetime','Dte')
	sdf=sdf.withColumn('Dte',to_timestamp('Dte','yyyy-MM-dd HH:mm:ss'))
	
	temp=sdf.groupby('Crypto').agg(max('Dte').alias('Dte'))
	sdf=sdf.join(temp,['Crypto','Dte'])
	sdf=sdf.withColumnRenamed('Adj Close','Adj_Close')
	sdf.show()
	
	current_day=datetime.today().strftime("%Y-%m-%d")
	#sdf.write.mode('overwrite').option('header','False').parquet('gs://hive-warehouse1/datasets/crypto.db/crypto_parq/'+current_day)
	sdf.write.mode('overwrite').option('header','False').parquet('gs://hive-warehouse1/datasets/crypto.db/crypto_parq/'+current_day)

	#sdf.write.parquet('/usr/local/airflow/dags/cryp.parquet')
	#sdfp=sdf.toPandas()
	#sdfp.head()
	#sdfp.to_parquet('gs://hive-warehouse1/datasets/crypto.db/crypto_parq/'+current_day+'/'+current_day+'.parquet')




get_data()