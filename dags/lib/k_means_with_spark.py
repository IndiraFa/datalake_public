from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from airflow.models import Variable
from pyspark.sql.functions import col, concat, lit, expr, array
from datetime import datetime, timedelta
from index_to_elastic_with_spark import list_subfolders
from airflow.hooks.base import BaseHook
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    TimestampType
)

# Identifiants AWS
S3_BUCKET = Variable.get("S3_BUCKET", default_var="datalake-bgd705")
# Dossier à partir duquel lire les fichiers Parquet
ENRICHED_VELIB_LIME = Variable.get(
    "ENRICHED_VELIB_LIME",
    default_var="enriched/default_velib_lime/enriched_join_velib_lime/"
)

def calculate_time_window():
    """
    Calculate the time window for filtering data
    
    Args:
        None

    Returns:
        Tuple[datetime, datetime]: start_time, end_time
    """
     # Calculer l'heure actuelle et l'heure de début (1h 30 minutes avant)
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=90)
    return start_time, end_time


def run_kmeans(elastic_conn_id='elastic_kibana', s3_conn_id='aws_s3'):
    """
    Runs a K-Means clustering algorithm on bike station data stored in an 
    S3 bucket, filters the data based on a time window, and indexes the 
    results in Elasticsearch.

    Args:
    - elastic_conn_id (str): Airflow connection ID for Elasticsearch.
    - s3_conn_id (str): Airflow connection ID for AWS S3.

    Returns:
    - None
    """
    conn_elastic = BaseHook.get_connection(elastic_conn_id)
    elastic_host = conn_elastic.host
    elastic_port = conn_elastic.port
    elastic_user = conn_elastic.login
    elastic_password = conn_elastic.password
    print(f"Connexion à Elasticsearch")

    conn_s3 = BaseHook.get_connection(s3_conn_id)
    aws_access_key_id = conn_s3.login
    aws_secret_access_key = conn_s3.password
    print(f"Connexion à AWS S3")

    # Lister les sous-dossiers
    subfolders = list_subfolders(S3_BUCKET, ENRICHED_VELIB_LIME, conn_s3)
    if not subfolders:
        raise ValueError(f"No subfolders found in {ENRICHED_VELIB_LIME}")
    
    # sélectionner le permier sous-dossier (il n'y en a qu'un)
    selected_subfolder = subfolders[0]
    print(f"Lecture des données depuis le sous-dossier: {selected_subfolder}")


    # Créer une session Spark avec Elasticsearch et S3
    spark = SparkSession.builder \
        .appName("WeightedKMeansClustering") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.10.2,org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    print("Session Spark démarrée")

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
    print("Données lues avec succès")

    # calculer la fenêtre de temps
    start_date, end_date = calculate_time_window()

    # filtrer les données par période
    df = df.filter(
        (col("time") >= lit(start_date)) & (col("time") <= lit(end_date))
    )
    print(f"Données filtrées entre {start_date} et {end_date}")

    # Dupliquer les points en fonction de leurs poids (num_bikes)
    df = df.withColumn("weight", col("num_bikes"))
    df = df.withColumn("weight", expr("int(weight)"))  # Convertir les poids en entiers
    df = df.withColumn("weight", expr("IF(weight > 0, weight, 1)"))  # Assurer que le poids est au moins 1

    # Exploser les lignes en fonction du poids
    df = df.withColumn(
        "dummy", expr("explode(array_repeat(struct(lat, lon), weight))")
    )
    df = df.select(
        "provider", "id", "dummy.lat", "dummy.lon",
        "time", "num_bikes", "num_docks"
    )

    # Sélectionner les colonnes pertinentes pour le clustering
    feature_columns = ["lat", "lon"]

    # Assembler les colonnes en un vecteur de caractéristiques
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df = assembler.transform(df)
    print("Caractéristiques assemblées")

    # Appliquer l'algorithme K-means
    kmeans = KMeans().setK(12).setSeed(1)
    model = kmeans.fit(df)

    # Ajouter les résultats du clustering au DataFrame
    df = model.transform(df)
    print("Clustering effectué")

    # Ajouter une colonne pour l'ID concaténé
    df = df.withColumn(
        "id_concat", concat(col("id"), lit("_"), col("time").cast("string"))
    )
    df = df.withColumn("location", array(col("lon"), col("lat")))
    df = df.drop("lat", "lon")
    # Sélectionner uniquement les colonnes nécessaires pour l'indexation
    df_to_index = df.select(
        "provider", "id_concat", "location", "time",
        "num_bikes", "num_docks", "prediction"
    )

    # Écrire les résultats dans un fichier Parquet sur S3
    output_path = f"s3a://{S3_BUCKET}/usage/kmeans_results/"
    df_to_index.write.mode("overwrite").parquet(output_path)
    print(f"Données écrites dans {output_path}")

    # Indexer les résultats dans Elasticsearch
    df_to_index.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", elastic_host) \
        .option("es.port", elastic_port) \
        .option("es.net.http.auth.user", elastic_user) \
        .option("es.net.http.auth.pass", elastic_password) \
        .option("es.nodes.wan.only", "true") \
        .option("es.resource", "kmeans_results") \
        .option("es.mapping.id", "id_concat") \
        .option("es.index.auto.create", "false") \
        .mode("overwrite") \
        .save()
    print("Données indexées dans Elasticsearch")

    spark.stop()

if __name__ == "__main__":
    run_kmeans('elastic_kibana', 'aws_s3',)
