
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType


def transformacao_dados():

    # Creates a session on a local master
    spark = SparkSession.builder.appName("Array to Dataframe") \
        .master("spark://6a21116c0886:7077").getOrCreate()

    try:
        data = [['Jean'], ['Liz'], ['Pierre'], ['Lauric']]
        schema = StructType([StructField('name', StringType(), True)])

        df = spark.createDataFrame(data, schema)
        df.show()
        df.printSchema()

    finally:
        # Certificar-se de encerrar o SparkContext ao final
        if spark._sc:
            spark._sc.stop()

    

default_args = {
    'owner': 'allan',
    'start_date': datetime(2024, 1, 1, 10, 00)
}


with DAG('transformacao_SparkMaster',
         default_args=default_args,
         schedule_interval='@monthly',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='transformacao_dados_SparkMaster',
        python_callable=transformacao_dados
    )
