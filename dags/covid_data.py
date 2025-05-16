from datetime import datetime, timedelta
from io import BytesIO
import boto3
import kagglehub
from airflow import DAG
from airflow.operators.python import PythonOperator
from kagglehub import KaggleDatasetAdapter
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

default_args = {
    'owner': 'o_savinov',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def upload_dataset():
    df = kagglehub.dataset_load(adapter=KaggleDatasetAdapter.PANDAS,
                                handle="fireballbyedimyrnmom/us-counties-covid-19-dataset",
                                path='us-counties.csv')
    df = df.drop('fips', axis=1)
    aws_access_key_id = 'ILjHRBpiGoHuBXwdp9zG'
    aws_secret_access_key = '61iwwaFS6nIiDCULv8o4PcUUxqCAQInnvcJXRwC8'
    minio_endpoint = 'http://localhost:9000'
    bucket_name = 'covid'
    s3 = boto3.client('s3', endpoint_url=minio_endpoint,
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow')

    s3.put_object(Bucket=bucket_name,
                  Key='test.parquet',
                  Body=parquet_buffer.getvalue())


with DAG(dag_id='covid_data',
         schedule="@daily",
         start_date=datetime(2024, 1, 1),
         default_args=default_args):
    to_s3 = PythonOperator(task_id='get_and_upload_to_s3',
                           python_callable=upload_dataset)
    lala = ClickHouseOperator(sql="""
CREATE TABLE covid
(date Date,
county String,
state String,
cases Int64,
deaths Int32)
ENGINE = MergeTree()
ORDER BY tuple(date, state)
PARTITION BY state
""")
    to_s3 >> lala
