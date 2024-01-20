# airflow_dados/dags/main_dag.py
from airflow import DAG
from datetime import datetime

import os
import sys

# Adiciona o diretório do DAG ao sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from tasks._1_ingestao_task import create_ingestao_task
from tasks._2_bronze_raw_extracao_task import create_bronze_raw_task
from tasks._3_silver_trusted_transformacao_task import create_silver_trusted_spark_task
from tasks._4_gold_refined_transformacao_task import create_gold_refined_task

default_args = {
    'owner': 'allan',
    'start_date': datetime(2024, 1, 1, 10, 00),
}

dag = DAG('main_dag',
         default_args=default_args,
         schedule_interval='@monthly',
         catchup=False)

# Crie as tarefas para cada DAG
ingestao_dag = create_ingestao_task(dag)
bronze_raw_dag = create_bronze_raw_task(dag)
silver_trusted_dag = create_silver_trusted_spark_task(dag)
gold_refined_dag = create_gold_refined_task(dag)

# Defina as dependências entre as DAGs
ingestao_dag >> bronze_raw_dag >> silver_trusted_dag >> gold_refined_dag
