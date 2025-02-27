from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook

from lib.velib_data_fetcher import fetch_store_velib_ss, fetch_store_velib_si
from lib.lime_data_fetcher import fetch_store_lime_s3
from airflow.models import Variable


conn_aws = BaseHook.get_connection("aws_s3")
aws_access_key_id = conn_aws.login
aws_secret_access_key = conn_aws.password

conn_elastic = BaseHook.get_connection("elastic_kibana")
elastic_host = conn_elastic.host
elastic_user = conn_elastic.login
elastic_password = conn_elastic.password

DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")

with DAG(
    'bike_data_pipeline',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Bike data pipeline DAG',
    schedule_interval='0 */3 * * *',  # Toutes les 3 heures
    start_date=datetime(2025, 2, 19),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
    This is a bike data pipeline DAG.
    """

    start_task = EmptyOperator(task_id='start_task')

    fetch_ss_velib_data = PythonOperator(
        task_id='fetch_ss_data_from_velib',
        python_callable=fetch_store_velib_ss,
        provide_context=True,
        do_xcom_push=True, # stockage de la valeur de retour dans XCom
    )

    fetch_si_velib_data = PythonOperator(
        task_id='fetch_si_data_from_velib',
        python_callable=fetch_store_velib_si,
        provide_context=True,
        do_xcom_push=True, # stockage de la valeur de retour dans XCom
    )

    fetch_lime_data = PythonOperator(
        task_id='fetch_data_from_lime',
        python_callable=fetch_store_lime_s3,
        provide_context=True,
        do_xcom_push=True, # stockage de la valeur de retour dans XCom
    )

    # Tâche SparkSubmitOperator pour transformer les données JSON en Parquet
    transform_ss_velib_to_parquet = SparkSubmitOperator(
        task_id='transform_ss_velib_json_to_parquet',
        application='dags/lib/transform_ss_velib_with_spark.py',
        conn_id='spark_default',  # Connexion Spark configurée dans Airflow
        application_args=["{{ ti.xcom_pull(task_ids='fetch_ss_data_from_velib') }}"],
        conf={
            'spark.hadoop.fs.s3a.access.key': aws_access_key_id,
            'spark.hadoop.fs.s3a.secret.key': aws_secret_access_key,
            'spark.hadoop.fs.s3a.endpoint': "s3.eu-north-1.amazonaws.com",
            'spark.jars.packages': "org.apache.hadoop:hadoop-aws:3.3.1",
            'spark.hadoop.fs.s3.impl': "org.apache.hadoop.fs.s3a.S3AFileSystem"
        },  
        dag=dag,
    )

    transform_si_velib_to_parquet = SparkSubmitOperator(
        task_id='transform_si_velib_json_to_parquet',
        application='dags/lib/transform_si_velib_with_spark.py',
        conn_id='spark_default',
        application_args=["{{ ti.xcom_pull(task_ids='fetch_si_data_from_velib') }}"],
        conf={
            'spark.hadoop.fs.s3a.access.key': aws_access_key_id,
            'spark.hadoop.fs.s3a.secret.key': aws_secret_access_key,
            'spark.hadoop.fs.s3a.endpoint': "s3.eu-north-1.amazonaws.com",
            'spark.jars.packages': "org.apache.hadoop:hadoop-aws:3.3.1",
            'spark.hadoop.fs.s3.impl': "org.apache.hadoop.fs.s3a.S3AFileSystem"
        },  
        dag=dag,
    )

    transform_lime_to_parquet = SparkSubmitOperator(
        task_id='transform_lime_json_to_parquet',
        application='dags/lib/transform_lime_with_spark.py',
        conn_id='spark_default',
        application_args=["{{ ti.xcom_pull(task_ids='fetch_data_from_lime') }}"],
        conf={
            'spark.hadoop.fs.s3a.access.key': aws_access_key_id,
            'spark.hadoop.fs.s3a.secret.key': aws_secret_access_key,
            'spark.hadoop.fs.s3a.endpoint': "s3.eu-north-1.amazonaws.com",
            'spark.jars.packages': "org.apache.hadoop:hadoop-aws:3.3.1",
            'spark.hadoop.fs.s3.impl': "org.apache.hadoop.fs.s3a.S3AFileSystem"
        },  
        dag=dag,
    )

    # dbt_run = BashOperator(
    #     task_id='dbt_run',
    #     bash_command=f"""
    #     dbt debug --project-dir /Users/fabreindira/dbt_datalake/datalake_project
    #     """,
    #     dag=dag,
    # )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"""
        dbt run --models enriched_lime enriched_join_velib enriched_join_velib_lime --project-dir {DBT_PROJECT_DIR}
        """,
        dag=dag,
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"""dbt test --models enriched_join_velib enriched_lime enriched_join_velib_lime --project-dir {DBT_PROJECT_DIR}
        """,
        dag=dag,
    )

    index_to_elastic = SparkSubmitOperator(
        task_id='index_to_elastic_with_spark',
        application='dags/lib/index_to_elastic_with_spark.py',
        conn_id='spark_default',
        application_args=[elastic_host, elastic_user, elastic_password],
        conf={
            'spark.jars.packages': "org.elasticsearch:elasticsearch-spark-30_2.12:8.10.2,org.apache.hadoop:hadoop-aws:3.3.1",
            'spark.hadoop.fs.s3a.access.key': aws_access_key_id,
            'spark.hadoop.fs.s3a.secret.key': aws_secret_access_key,
            'spark.hadoop.fs.s3a.endpoint': "s3.eu-north-1.amazonaws.com",
            'spark.hadoop.fs.s3.impl': "org.apache.hadoop.fs.s3a.S3AFileSystem"
        },
        dag=dag,
    )

    k_means = SparkSubmitOperator(
        task_id='run_kmeans',
        application='dags/lib/k_means_with_spark.py',
        conn_id='spark_default',
        conf={
            'spark.jars.packages': "org.elasticsearch:elasticsearch-spark-30_2.12:8.10.2,org.apache.hadoop:hadoop-aws:3.3.1",
            'spark.hadoop.fs.s3a.access.key': aws_access_key_id,
            'spark.hadoop.fs.s3a.secret.key': aws_secret_access_key,
            'spark.hadoop.fs.s3a.endpoint': "s3.eu-north-1.amazonaws.com",
            'spark.hadoop.fs.s3.impl': "org.apache.hadoop.fs.s3a.S3AFileSystem"
        },
        dag=dag,
    )

    start_task >> [fetch_ss_velib_data, fetch_si_velib_data, fetch_lime_data]
    fetch_ss_velib_data >> transform_ss_velib_to_parquet
    fetch_si_velib_data >> transform_si_velib_to_parquet
    fetch_lime_data >> transform_lime_to_parquet

    [
        transform_ss_velib_to_parquet,
        transform_si_velib_to_parquet,
        transform_lime_to_parquet
    ] >> dbt_run >> dbt_test >> index_to_elastic >> k_means

