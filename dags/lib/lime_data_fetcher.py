from datetime import datetime
import requests
import boto3
from airflow.hooks.base import BaseHook
from airflow.models import Variable

# Identifiants AWS
S3_BUCKET = Variable.get("S3_BUCKET", default_var="datalake-bgd705")
# Dossier cible dans le Data Lake S3
RAW_LIME_BS = Variable.get(
    "RAW_LIME_BS", default_var="raw/lime/free_bike_status/"
)

def fetch_store_lime_s3(aws_conn_id='aws_s3', **kwargs):
    """
    Get the Lime API data and store it in S3

    :param aws_conn_id: AWS connection ID
    :type aws_conn_id: str

    :return: None
    """
    current_day = datetime.now().strftime("%Y%m%d")
    current_time = datetime.now().strftime("%H%M%S")
    s3_key = f"{RAW_LIME_BS}{current_day}/{current_time}/station_status.json"

    # URL de l'API Vélib'
    url = 'https://data.lime.bike/api/partners/v2/gbfs/paris/free_bike_status'

    
    try:
        print("Démarrage de la récupération des données Lime")
        # Requête HTTP avec délai d'attente
        r = requests.get(url, timeout=10)
        r.raise_for_status()  # Vérifie si la requête a réussi (code 200)

        print("Données récupérées avec succès")
        # Récupérer les identifiants AWS de la connexion Airflow
        conn = BaseHook.get_connection(aws_conn_id)
        aws_access_key_id = conn.login
        aws_secret_access_key = conn.password

        print("Connexion à AWS S3")
        # Initialisation du client S3
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=conn.extra_dejson.get('region_name', 'eu-north-1')
        )

        print(f"Envoi du fichier free_bike_status.json vers s3://{S3_BUCKET}/{s3_key}")
        # Envoi du fichier directement dans S3 
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=r.content,
            ContentType="application/json"
        )

        print(f"Fichier free_bike_status.json stocké dans S3: s3://{S3_BUCKET}/{s3_key}")

        return s3_key  # Stockage du chemin du fichier dans XCom

    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la récupération des données: {e}")
    except Exception as e:
        print(f"Erreur S3: {e}")
