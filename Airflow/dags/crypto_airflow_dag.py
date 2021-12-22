
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta, timezone,date
from airflow.utils.dates import days_ago
from airflow import DAG

import pandas as pd
#import pymysql
from airflow.models import BaseOperator


import random
import string
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyarrow
from pyarrow import hdfs
#import yfinance as yf

from pyspark.sql import Row

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator



default_args = {
	'owner': 'Vysh',
	'depends_on_past': False,
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 5,
	'retry_delay': timedelta(minutes=5),
}



dag = DAG('crypto_dag_bash',
			start_date=datetime(2021, 12, 1, 0, 0, 0, 0, timezone.utc),#days_ago(2),
			schedule_interval='@once',
			concurrency=5,
			max_active_runs=1,
			default_args=default_args)







def get_data():
	crypto=['BTC-USD','ETH-USD','BNB-USD','USDT-USD','SOL1-USD','ADA-USD','XRP-USD','DOT1-USD','HEX-USD','USDC-USD']
	df=pd.DataFrame()
	for c in crypto:
		data=yf.download(tickers=c,period='15m',interval='1m')
		df1=pd.DataFrame(data)
		df1['Crypto']=c
		df=df.append(df1)

	df.reset_index(inplace=True)
	df.dropna()




	
	#sdf=spark.createDataFrame(df)
	#sdf=sdf.withColumnRenamed('Datetime','Dte')
	#sdf=sdf.withColumn('Dte',to_timestamp('Dte','yyyy-MM-dd HH:mm:ss'))
	#temp=sdf.groupby('Crypto').agg(max('Dte').alias('Dte'))
	#sdf=sdf.join(temp,['Crypto','Dte'])
	#sdf=sdf.withColumnRenamed('Adj Close','Adj_Close')
	#sdf.show()
	#current_day=datetime.today().strftime("%Y-%m-%d")
	#sdf.write.mode('overwrite').option('header','False').parquet('gs://hive-warehouse1/datasets/crypto.db/crypto_parq/'+current_day)





start_pipeline = DummyOperator(
	task_id = 'start_pipeline',
	dag = dag
)


with dag:

	get_data_bash=BashOperator(

		task_id='get_data_bash',
    	bash_command='python /opt/airflow/dags/scripts/sp.py'
    	#depends_on_past=False

	)



with dag:

	write_to_bigquery=BashOperator(

		task_id='write_to_bigquery',
    	bash_command='python /opt/airflow/dags/scripts/bq.py'
    	#depends_on_past=False

	)



'''
with dag:
	get_data=SparkSubmitOperator(

		task_id='get_data_sp',
		conn_id='spark_conn',
		application='./scripts/sp.py',
		execution_timeout=timedelta(minutes=10)

	)

'''


'''
with dag:

    Cov_DeathsToS3=PythonOperator(

        task_id='Cov_DeathsToS3',
        python_callable=data_to_S3,
        op_kwargs={'csv_name': 'cov_deaths', 'folder_name': 'covid-data','url':'https://raw.githubusercontent.com/CSSEGISandData/\
        COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv'}
        #depends_on_past=False

    )


load_conf=S3ToMySqlOperator(
	task_id="load_conf",
	s3_source_key='s3://aflow-bucket/covid-data/cov_conf.csv',
	mysql_table='Analysisdb.cov_conf',
	aws_conn_id=S3_CONN_ID,
	mysql_conn_id="RDS_connection",
	mysql_extra_options="FIELDS TERMINATED BY ','",
	dag=dag
	)


analysis = MySqlOperator(
	task_id="analysis",
	sql='./sql/Queries.sql',
	mysql_conn_id="RDS_connection",
	autocommit=True,
	dag=dag
	)


export_results_1=MySQLToS3Operator(
	task_id="export_results_1",
	query="select * from Analysisdb.r_n_job;",
	s3_bucket=S3_BUCKET,
	s3_key='Results/result1.csv',
	mysql_conn_id="RDS_connection",
	aws_conn_id=S3_CONN_ID,
	pd_csv_kwargs={'index': True, 'header': True},
	dag=dag


	)
'''

start_pipeline >> get_data_bash >> write_to_bigquery
#start_pipeline >> data_to_S3 >> drop_db >> create_db #>> data_to_table