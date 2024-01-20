# airflow_dados/tasks/3_silver_trusted_transformacao_task.py
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow import DAG
from datetime import datetime
import logging 

def silver_trusted_transformacao():
    # Lógica de transformação para a camada silver
    logging.info("Executing silver_trusted_transformacao")

def create_silver_trusted_task(parent_dag):

    silver_trusted_task = PythonOperator(
        task_id='silver_trusted_transformacao',
        python_callable=silver_trusted_transformacao,
        dag=parent_dag,
    )

    return silver_trusted_task


def create_silver_trusted_spark_task(parent_dag):

    # Task para submeter o job Spark usando o SparkSubmitOperator
    silver_spark_task = SparkSubmitOperator(
        task_id='silver_trusted_transformacao',
        conn_id='spark',  # ajuste conforme sua conexão ao cluster Spark
        application='./spark/silver_spark.py',  # caminho para o script Spark
        dag=parent_dag,
    )

    return silver_spark_task

def create_silver_trusted_hive_task(parent_dag):

    # Task para criar tabela no Hive (usando o HiveOperator, ajuste conforme necessário)
    silver_hive_task = HiveOperator(
        task_id='create_hive_table',
        hql='path/to/your/create_table_script.hql',  # script Hive para criar tabela
        hive_cli_conn_id='hive_default',  # ajuste conforme sua conexão ao Hive
        depends_on_past=True,
        dag=parent_dag,
    )    

    return silver_hive_task
