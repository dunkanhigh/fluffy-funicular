from datetime import datetime
from io import BytesIO, StringIO

import boto3
import kagglehub
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from kagglehub import KaggleDatasetAdapter

from airflow import DAG

default_args = {
    'owner': 'o_savinov'
}

delete_table = """DROP TABLE IF EXISTS covid"""

create_table = """
CREATE TABLE covid
(
date Date,
county String,
state String,
cases Int64,
deaths Int32,
population Int32,
ISO FixedString(5),
density_per_sq_mi Int32
)
ENGINE = MergeTree()
ORDER BY tuple(date, state)
PARTITION BY state
"""

insert_from_s3 = """
insert into covid select * FROM s3(
    'http://minio:9000/covid/test.parquet',
    'QOuSfMAuVOStCpyrUgmq',
    'dr5jwQf7qf50yTCwdR96FLPgoFV5hR2JOc9mNv3M',
    'Parquet'
)
"""


def upload_dataset():
    # download data from Kaggla
    df = kagglehub.dataset_load(
        adapter=KaggleDatasetAdapter.PANDAS,
        handle="fireballbyedimyrnmom/us-counties-covid-19-dataset",
        path='us-counties.csv', pandas_kwargs={'on_bad_lines': 'skip'})

    # minIOdata (idealy should be covered and located in .env)
    aws_access_key_id = 'QOuSfMAuVOStCpyrUgmq'
    aws_secret_access_key = 'dr5jwQf7qf50yTCwdR96FLPgoFV5hR2JOc9mNv3M'
    minio_endpoint = 'http://minio:9000'
    bucket_name = 'covid'

    # Calling boto3 client
    s3 = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
        )

    # Get extra data from s3 as pd-dataframe (It was put there in advance)
    response = s3.get_object(
        Bucket=bucket_name,
        Key='exstra_data.csv'
    )
    csv_content = response['Body'].read().decode('utf-8')
    extra_data = pd.read_csv(StringIO(csv_content))

    # Data clearing and enrichment
    df = df.drop('fips', axis=1)
    df = pd.merge(df, extra_data, on='state', how='left')
    df['date'] = pd.to_datetime(df['date'],  errors='coerce')
    df['county'] = df['county'].astype('string')
    df['state'] = df['state'].astype('string')
    df['ISO'] = df['ISO'].astype('string')

    # Put data to s3 as parquet
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow')
    s3.put_object(Bucket=bucket_name,
                  Key='test.parquet',
                  Body=parquet_buffer.getvalue())


with DAG(dag_id='covid_data',
         schedule="@daily",
         start_date=datetime(2025, 5, 15),
         default_args=default_args):

    to_s3 = PythonOperator(
        task_id='get_and_upload_to_s3',
        python_callable=upload_dataset
        )

    drop_table = ClickHouseOperator(
        task_id='drop_table',
        database='default',
        sql=delete_table
        )

    create_table = ClickHouseOperator(
        task_id='create_table',
        database='default',
        sql=create_table
        )

    test = ClickHouseOperator(
        task_id="upload_to_clickhouse",
        database='default',
        sql=insert_from_s3)

    drop_table >> create_table
    [create_table, to_s3] >> test
