import boto3
from pyspark.sql import SparkSession
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import concat, col, lit, array
import requests
from requests.auth import HTTPBasicAuth


# Identifiants AWS
S3_BUCKET = Variable.get("S3_BUCKET", default_var="datalake-bgd705")
# Dossier à partir duquel lire les fichiers Parquet
ENRICHED_VELIB_LIME = Variable.get("ENRICHED_VELIB_LIME", default_var="enriched/default_velib_lime/enriched_join_velib_lime/")

def list_subfolders(bucket, prefix, conn):
    """
    Liste les sous-dossiers d'un dossier S3

    Args:
        bucket: Nom du bucket S3
        prefix: Préfixe du dossier
        conn: Connexion Airflow à S3

    Returns:
        Liste des sous-dossiers
    """
    aws_access_key_id = conn.login
    aws_secret_access_key = conn.password
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    if 'CommonPrefixes' in response:
        return [common_prefix['Prefix'] for common_prefix in response['CommonPrefixes']]
    return []

def verify_indexed_data(host, user, password):
    """
    Vérifie que les données ont été correctement indexées dans Elasticsearch.

    Args:
        host: Adresse IP ou nom de domaine d'Elasticsearch
        user: Nom d'utilisateur pour l'authentification
        password: Mot de passe pour l'auth

    Returns:
        None
    """
    url = f"{host}/all_bike_data/_search"

    auth = HTTPBasicAuth(user, password)
    headers = {"Content-Type": "application/json"}
    query = {
        "query": {
            "match_all": {}
        }
    }

    response = requests.get(url, auth=auth, headers=headers, json=query)

    if response.status_code == 200:
        results = response.json()
        print(f"Total documents indexed: {results['hits']['total']['value']}")
    else:
        print(f"Failed to verify indexed data: {response.status_code}, {response.text}")



def index_to_elastic(elastic_conn_id='elastic_kibana', s3_conn_id='aws_s3', **kwargs):
    """
    Indexe un fichier Parquet dans Elasticsearch

    Args:
        elastic_conn_id: Connexion Airflow à Elasticsearch
        s3_conn_id: Connexion Airflow à S3
        kwargs: Arguments supplémentaires

    Returns:
        None
    """
    # Récupérer les identifiants de connexion
    conn_elastic = BaseHook.get_connection(elastic_conn_id)
    elastic_host = conn_elastic.host
    elastic_port = conn_elastic.port
    elastic_user = conn_elastic.login
    elastic_password = conn_elastic.password

    conn_s3 = BaseHook.get_connection(s3_conn_id)
    aws_access_key_id = conn_s3.login
    aws_secret_access_key = conn_s3.password

    # Lister les sous-dossiers
    subfolders = list_subfolders(S3_BUCKET, ENRICHED_VELIB_LIME, conn_s3)
    if not subfolders:
        raise ValueError(f"No subfolders found in {ENRICHED_VELIB_LIME}")
    
    # sélectionner le permier sous-dossier (il n'y en a qu'un)
    selected_subfolder = subfolders[0]
    print(f"Lecture des données depuis le sous-dossier: {selected_subfolder}")

    # Créer une session Spark avec Elasticsearch et S3
    spark = SparkSession.builder \
        .appName("IndexParquetToElasticsearch") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.10.2,org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    # Définir le schéma du DataFrame
    schema = StructType([
        StructField("provider", StringType(), True),
        StructField("id", StringType(), True),
        StructField("time", TimestampType(), True),
        StructField("lat", FloatType(), True),
        StructField("lon", FloatType(), True),
        StructField("num_bikes", IntegerType(), True),
        StructField("num_docks", IntegerType(), True),
    ])

    # Lire le fichier Parquet depuis S3
    s3_path = f"s3a://{S3_BUCKET}/{selected_subfolder}"
    df = spark.read.schema(schema).parquet(s3_path)

    # Afficher les 5 premières lignes du DataFrame pour vérifier les données
    df.show(5)

    # Ajouter une colonne pour l'ID concaténé
    df = df.withColumn("id_concat", concat(col("id"), lit("_"), col("time").cast("string")))
    df = df.withColumn("location", array(col("lon"), col("lat")))

    df = df.drop("lat", "lon")

    # Indexer dans Elasticsearch
    df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", elastic_host) \
        .option("es.port", elastic_port) \
        .option("es.net.http.auth.user", elastic_user) \
        .option("es.net.http.auth.pass", elastic_password) \
        .option("es.nodes.wan.only", "true") \
        .option("es.resource", "all_bike_data") \
        .option("es.mapping.id", "id_concat") \
        .option("es.index.auto.create", "false") \
        .mode("overwrite") \
        .save()

    spark.stop()

    # Vérifier les données indexées
    verify_indexed_data(elastic_host, elastic_user, elastic_password)


if __name__ == "__main__":
    index_to_elastic()