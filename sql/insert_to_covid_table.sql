INSERT INTO covid
SELECT * FROM s3(
    'http://minio:9000/covid/test.parquet',
    '{{ conn.minio.login }}',
    '{{ conn.minio.password }}',
    'Parquet'
)