import sys
from pyspark.sql import SparkSession
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from pyspark.sql.functions import (
    from_unixtime, col, explode, when, to_timestamp
)
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, FloatType, TimestampType,
                               BooleanType
)


# Identifiants AWS
S3_BUCKET = Variable.get("S3_BUCKET", default_var="datalake-bgd705")
# Dossier cible pour les fichiers transformés
FORMATTED_LIME_BS = Variable.get(
    "FORMATTED_LIME_BS", default_var="formatted/lime/free_bike_status/"
)


def spark_json_to_parquet(aws_conn_id='aws_s3', **kwargs):
    """
    Formats the Lime JSON data to Parquet format using Spark.
    :param aws_conn_id: Airflow connection ID for AWS
    :param kwargs: Additional arguments
    """

    if len(sys.argv) < 2:
        raise ValueError("Aucun chemin d'entrée fourni.")

    s3_input_path = sys.argv[1]  # Chemin S3 passé par Airflow

    # Déterminer le dossier de sortie basé sur la date du fichier
    date_folder = s3_input_path.split('/')[-3]
    time_folder = s3_input_path.split('/')[-2]
    s3_output_path = f"s3://{S3_BUCKET}/{FORMATTED_LIME_BS}{date_folder}/{time_folder}/"
    print(f"Chemin de sortie: {s3_output_path}")

    # Récupérer les identifiants AWS de la connexion Airflow
    conn = BaseHook.get_connection(aws_conn_id)
    aws_access_key_id = conn.login
    aws_secret_access_key = conn.password
    print(f"Connexion à AWS S3")

    spark = SparkSession.builder \
        .appName("LimeJSONToParquet") \
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
        from_unixtime(col("last_updated")).alias("last_updated_timestamp"),  # Convert root-level timestamp
        explode(col("data.bikes")).alias("bike")  # Explode nested array
    )
    print("Données aplaties")


    # Définition du schéma pour garantir les types
    schema = StructType([
        StructField("last_updated_timestamp", TimestampType(), True),
        StructField("bike_id", StringType(), True),
        StructField("lat", FloatType(), True),
        StructField("lon", FloatType(), True),
        StructField("is_reserved", BooleanType(), True),
        StructField("is_disabled", BooleanType(), True),
        StructField("current_range_meters", IntegerType(), True),
        StructField("vehicle_type_id", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("last_reported_timestamp", TimestampType(), True)
    ])

    # Extract fields from nested structure
    df_transformed = df_flattened.select(
        col("last_updated_timestamp"),
        col("bike.bike_id"),
        col("bike.lat"),
        col("bike.lon"),
        col("bike.is_reserved"),
        col("bike.is_disabled"),
        col("bike.current_range_meters"),
        col("bike.vehicle_type_id"),
        col("bike.vehicle_type"),
        from_unixtime(col("bike.last_reported")).alias("last_reported_timestamp")  # Convert timestamp
    )
    
    # Convertir les valeurs string "true"/"false" en booléens et les 
    # timestamps Unix en timestamps Spark
    df_transformed = df_transformed.withColumn("is_reserved", when(col("is_reserved") == "true", True).otherwise(False))
    df_transformed = df_transformed.withColumn("is_disabled", when(col("is_disabled") == "true", True).otherwise(False))
    df_transformed = df_transformed.withColumn("last_updated_timestamp", to_timestamp(col("last_updated_timestamp")))
    df_transformed = df_transformed.withColumn("last_reported_timestamp", to_timestamp(col("last_reported_timestamp")))
    print("Données transformées")

    # Appliquer le schéma
    df_transformed = spark.createDataFrame(df_transformed.rdd, schema)  

    # Write to Parquet format
    df_transformed.write.mode("overwrite").parquet(s3_output_path)

    print(f"Données transformées en Parquet et stockées dans {s3_output_path}")

    spark.stop()

if __name__ == "__main__":
    spark_json_to_parquet()