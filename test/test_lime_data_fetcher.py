import sys
import os
from datetime import datetime
import requests
import boto3
from airflow.hooks.base import BaseHook
from airflow.models import Variable
# add the parent directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dags.lib.lime_data_fetcher import fetch_store_lime_s3


# Identifiants AWS
S3_BUCKET = Variable.get("S3_BUCKET", default_var="datalake-bgd705")
# Dossier cible dans le Data Lake S3
RAW_LIME_BS = Variable.get(
    "RAW_LIME_BS", default_var="raw/lime/free_bike_status/"
)

fetch_store_lime_s3()