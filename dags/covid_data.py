import logging
from datetime import datetime
from io import BytesIO, StringIO
from pathlib import Path

import boto3
import kagglehub
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from kagglehub import KaggleDatasetAdapter

from airflow import DAG

logger = logging.getLogger(__name__)


def load_sql_query(filename):
    sql_path = Path(__file__).parent.parent / "sql" / filename
    with open(sql_path, "r") as file:
        return file.read()


default_args = {
    'owner': 'o_savinov'
}


def get_minio_client():
    """Initialize and return MinIO client using Airflow connections."""
    logger.info("Getting minio connection")
    from airflow.hooks.base import BaseHook
    conn = BaseHook.get_connection('minio')
    return boto3.client(
        's3',
        endpoint_url=conn.host+':'+str(conn.port),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password
        )


def upload_dataset():
    try:
        # download data from Kaggla
        logger.info("Downloading data from Kaggle")
        df = kagglehub.dataset_load(
            adapter=KaggleDatasetAdapter.PANDAS,
            handle="fireballbyedimyrnmom/us-counties-covid-19-dataset",
            path='us-counties.csv', pandas_kwargs={'on_bad_lines': 'skip'})
        assert not df.empty, "Downloaded DataFrame is empty"
        required_columns = {'date', 'county', 'state', 'cases', 'deaths'}
        assert required_columns.issubset(df.columns), \
            f"Missing required columns: {required_columns - set(df.columns)}"

        # Calling boto3 client
        logger.info("Calling boto3 client")
        s3 = get_minio_client()
        bucket_name = 'covid'

        try:
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
            logger.info("Pushing data to Minio")
            # Put data to s3 as parquet
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow')
            s3.put_object(Bucket=bucket_name,
                          Key='test.parquet',
                          Body=parquet_buffer.getvalue())
        except Exception as e:
            logger.error(f"Error processing or uploading data: {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Failed to download data from Kaggle: {str(e)}")
        raise


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
        sql=load_sql_query('delete_covid_table.sql')
        )

    create_table = ClickHouseOperator(
        task_id='create_table',
        database='default',
        sql=load_sql_query('create_covid_table.sql')
        )

    test = ClickHouseOperator(
        task_id="upload_to_clickhouse",
        database='default',
        sql=load_sql_query('insert_to_covid_table.sql'))

    drop_table >> create_table
    [create_table, to_s3] >> test
