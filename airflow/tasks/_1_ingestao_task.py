# airflow_dados/tasks/1_ingestao_task.py
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import logging 


import os
import sys

# Adiciona o diretório do DAG ao sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from modulo.brasil_io_descompactar import ingestao_dados_API


def ingestao_dados():
    # Lógica de ingestão de dados
    ingestao_dados_API()

    logging.info("Executing ingestao_dados")

def create_ingestao_task(parent_dag):

    ingestao_task = PythonOperator(
        task_id='ingestao_dados',
        python_callable=ingestao_dados,
        dag=parent_dag,
    )

    return ingestao_task
