# airflow_dados/tasks/2_bronze_raw_extracao_task.py
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import logging 

def bronze_raw_extracao():
    # Lógica de extração para a camada bronze
    logging.info("Executing bronze_raw_extracao")

def create_bronze_raw_task(parent_dag):
    
    bronze_raw_task = PythonOperator(
        task_id='bronze_raw_extracao',
        python_callable=bronze_raw_extracao,
        dag=parent_dag,
    )

    return bronze_raw_task
