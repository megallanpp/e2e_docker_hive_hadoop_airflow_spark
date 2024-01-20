from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 


default_args = {
    'owner': 'allan',
    'start_date': datetime(2024, 1, 1, 10, 00)
}


with DAG('extracao_HDFS_Hadoop',
         default_args=default_args,
         schedule_interval='@monthly',
         catchup=False) as dag:

    # Adicione a tarefa SparkSubmitOperator ao DAG
    spark_task = SparkSubmitOperator(
        task_id='extracao_dados_HDFS_Hadoop',
        conn_id='spark',  # ajuste conforme sua conexão ao cluster Spark
        application='./spark/_1.2_bronze_copy_csv_to_hadoop.py',  # caminho para o script Spark
        dag=dag,
    )    
