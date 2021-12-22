

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyarrow

from datetime import datetime, timedelta, timezone,date
import yfinance as yf
import pandas as pd

from pyspark.sql import Row

#conf=SparkConf() \
#    .setAppName("sparkdf") \
#    .set("spark.jars", "/opt/airflow/dags/gcs-connector-hadoop2-2.1.6.jar") \
#    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
#    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/dags/vyshgk999_key.json")

spark=SparkSession.builder.appName('sparkdf').config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta').config("spark.jars","https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar").config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem").config("spark.sql.warehouse.dir", "gs://hive_warehouse1/datasets").enableHiveSupport().getOrCreate()


spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/dags/vyshgk999_key.json")
spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")
spark.conf.set("mapred.input.dir.recursive","true")
spark.conf.set("spark.sql.parquet.binaryAsString", "true")







def write_to_bigquery():
	## Text file
	#df=spark.sql("select * from trans.temp")

	## parquet file
	#spark.sql("use crypto")
	#df=spark.sql("select * from crypto.cryp_parq")
	schema = StructType([StructField("crypto", StringType(), True),StructField("dte", TimestampType(), True),StructField("open", DoubleType(), True),StructField("high", DoubleType(), True),\
	StructField("low", DoubleType(), True),StructField("close", DoubleType(), True),StructField("adj_close", DoubleType(), True),StructField("volume", IntegerType(), True)])
	df=spark.read.parquet('gs://hive-warehouse1/datasets/crypto.db/crypto_parq/*')
	df.show()

	## ORC file
	#df=spark.sql("select * from trans.temp_o")


	## Text file
	#df.write.format("bigquery").option("credentialsFile","/home/vyshgk99/bank_code/twit-service-acnt-cred.json").option("table", "hive_dataset.hive_text_table").option("temporaryGcsBucket","hive_alldata_bucket/bigquery_temp_data").mode('append').save()
	
	## Parquet file
	df=df.withColumn('Volume', df['Volume'].cast(IntegerType()))
	df.write.format("bigquery").option("credentialsFile","/opt/airflow/dags/vyshgk999_key.json").option("table", "crypto.crypto").option("temporaryGcsBucket","hive_all_data_warehouse/temp_data").mode('append').save()



write_to_bigquery()