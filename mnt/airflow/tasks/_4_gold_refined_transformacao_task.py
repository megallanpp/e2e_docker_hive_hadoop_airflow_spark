# airflow_dados/tasks/4_gold_refined_transformacao_task.py
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import logging 

def gold_refined_transformacao():
    # Lógica de transformação para a camada gold
    logging.info("Executing gold_refined_transformacao")

def create_gold_refined_task(parent_dag):

    gold_refined_task = PythonOperator(
        task_id='gold_refined_transformacao',
        python_callable=gold_refined_transformacao,
        dag=parent_dag,
    )

    return gold_refined_task
