import sys
from pyspark.sql import SparkSession
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from pyspark.sql.functions import from_unixtime, col, explode, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, TimestampType
)


# Identifiants AWS
S3_BUCKET = Variable.get("S3_BUCKET", default_var="datalake-bgd705")
# Dossier cible pour les fichiers transformés
FORMATTED_VELIB_SI = Variable.get("FORMATTED_VELIB_SI", default_var="formatted/velib/station_information/")


def spark_json_to_parquet(aws_conn_id='aws_s3', **kwargs):
    """
    Formats the Velib JSON data to Parquet format using Spark.
    :param aws_conn_id: Airflow connection ID for AWS
    :param kwargs: Additional arguments
    """

    if len(sys.argv) < 2:
        raise ValueError("Aucun chemin d'entrée fourni.")

    s3_input_path = sys.argv[1]  # Chemin S3 passé par Airflow

    # Déterminer le dossier de sortie basé sur la date du fichier
    date_folder = s3_input_path.split('/')[-3]  # Extrait "20250210"
    time_folder = s3_input_path.split('/')[-2]  # Extrait "180813"
    s3_output_path = f"s3://{S3_BUCKET}/{FORMATTED_VELIB_SI}{date_folder}/{time_folder}/"
    print(f"Chemin de sortie: {s3_output_path}")

    # Récupérer les identifiants AWS de la connexion Airflow
    conn = BaseHook.get_connection(aws_conn_id)
    aws_access_key_id = conn.login
    aws_secret_access_key = conn.password
    print(f"Connexion à AWS S3")

    spark = SparkSession.builder \
        .appName("VelibJSONToParquet") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    print("Spark session démarrée")

    s3_input_path = f"s3://{S3_BUCKET}/{s3_input_path}"
    
    df = spark.read.json(s3_input_path)
    print("Données JSON lues avec succès")

    # Flatten the "data.stations" array
    df_flattened = df.select(
        from_unixtime(col("lastUpdatedOther")).alias("lastUpdatedOther_timestamp"),  # Convert root-level timestamp
        explode(col("data.stations")).alias("station")  # Explode nested array
    )
    print("Données aplaties")

    # Définition du schéma pour garantir les types
    schema = StructType([
        StructField("lastUpdatedOther_timestamp", TimestampType(), True),
        StructField("station_id", StringType(), True),
        StructField("stationCode", StringType(), True),
        StructField("name", StringType(), True),
        StructField("lat", FloatType(), True),
        StructField("lon", FloatType(), True),
        StructField("capacity", IntegerType(), True),
        StructField("rental_methods", StringType(), True),
    ])


    # Extract fields from nested structure
    df_transformed = df_flattened.select(
        col("lastUpdatedOther_timestamp"),
        col("station.station_id"),
        col("station.stationCode"),
        col("station.name"),
        col("station.lat"),
        col("station.lon"),
        col("station.capacity"),
        col("station.rental_methods")
    )

    df_transformed = df_transformed.withColumn("lastUpdatedOther_timestamp", to_timestamp(col("lastUpdatedOther_timestamp")))
    print("Données transformées")

    df_transformed = spark.createDataFrame(df_transformed.rdd, schema)  # Appliquer le schéma

    # Write to Parquet format
    df_transformed.write.mode("overwrite").parquet(s3_output_path)

    print(f"Données transformées en Parquet et stockées dans {s3_output_path}")

    spark.stop()

if __name__ == "__main__":
    spark_json_to_parquet()