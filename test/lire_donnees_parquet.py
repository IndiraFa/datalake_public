import pandas as pd
import pyarrow.parquet as pq
import s3fs
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection("aws_s3")
aws_access_key_id = conn.login
aws_secret_access_key = conn.password

# Chemin S3
s3_path = ''

fs = s3fs.S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)

# lecture du résultat
with fs.open(s3_path, 'rb') as f:
    table = pq.read_table(f)
    df = table.to_pandas()

print(df.head(10))

print(df.info())