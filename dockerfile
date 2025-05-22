FROM apache/airflow:2.10.5 as airflow
RUN pip install --no-cache-dir \
    kagglehub
RUN pip install -U airflow-clickhouse-plugin==1.4.0
RUN pip install -U pandas==2.2.3