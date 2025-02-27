import sys
import os
from datetime import datetime
import requests
import boto3
from airflow.hooks.base import BaseHook
from airflow.models import Variable
# add the parent directory to the sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dags.lib.velib_data_fetcher import fetch_store_velib_ss


# Identifiants AWS
S3_BUCKET = "datalake-bgd705"
# Dossier cible dans le Data Lake S3
RAW_VELIB_SS = Variable.get(
    "RAW_VELIB_SS"
)

fetch_store_velib_ss()