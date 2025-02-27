import sys
from pyspark.sql import SparkSession
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from pyspark.sql.functions import (
    from_unixtime, col, explode, when, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType,
    TimestampType, BooleanType
)



# Identifiants AWS
S3_BUCKET = Variable.get("S3_BUCKET", default_var="datalake-bgd705")
# Dossier cible pour les fichiers transformés
FORMATTED_VELIB_SS = Variable.get(
    "FORMATTED_VELIB_SS", default_var="formatted/velib/stations_status/"
)


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
    date_folder = s3_input_path.split('/')[-3]
    time_folder = s3_input_path.split('/')[-2]
    s3_output_path = f"s3://{S3_BUCKET}/{FORMATTED_VELIB_SS}{date_folder}/{time_folder}/"
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
        from_unixtime(col("lastUpdatedOther")).alias("lastUpdatedOther_timestamp"),
        explode(col("data.stations")).alias("station")  # Explode nested array
    )
    print("Données aplaties")


    # Définition du schéma pour garantir les types
    schema = StructType([
        StructField("lastUpdatedOther_timestamp", TimestampType(), True),
        StructField("station_id", StringType(), True),
        StructField("stationCode", StringType(), True),
        StructField("num_bikes_available", IntegerType(), True),
        StructField("num_docks_available", IntegerType(), True),
        StructField("is_installed", BooleanType(), True),
        StructField("is_returning", BooleanType(), True),
        StructField("is_renting", BooleanType(), True),
        StructField("last_reported_timestamp", TimestampType(), True)
    ])


    # Extract fields from nested structure
    df_transformed = df_flattened.select(
        col("lastUpdatedOther_timestamp"),
        col("station.station_id"),
        col("station.stationCode"),
        col("station.num_bikes_available"),
        col("station.num_docks_available"),
        col("station.is_installed"),
        col("station.is_returning"),
        col("station.is_renting"),
        from_unixtime(col("station.last_reported")).alias("last_reported_timestamp")
    )

    # Convertir les valeurs 1/0 en booléens et les timestamps Unix en timestamps Spark
    df_transformed = df_transformed.withColumn("is_installed", when(col("is_installed") == 1, True).otherwise(False))
    df_transformed = df_transformed.withColumn("is_returning", when(col("is_returning") == 1, True).otherwise(False))
    df_transformed = df_transformed.withColumn("is_renting", when(col("is_renting") == 1, True).otherwise(False))
    df_transformed = df_transformed.withColumn("lastUpdatedOther_timestamp", to_timestamp(col("lastUpdatedOther_timestamp")))
    df_transformed = df_transformed.withColumn("last_reported_timestamp", to_timestamp(col("last_reported_timestamp")))
    print("Données transformées")
    
    df_transformed = spark.createDataFrame(df_transformed.rdd, schema)  # Appliquer le schéma

    # Write to Parquet format
    df_transformed.write.mode("overwrite").parquet(s3_output_path)

    print(f"Données transformées en Parquet et stockées dans {s3_output_path}")

    spark.stop()

if __name__ == "__main__":
    spark_json_to_parquet()