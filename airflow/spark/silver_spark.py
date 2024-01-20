import time

from pyspark.sql import SparkSession
import logging

def main():
    # Configuração do logger
    logger = logging.getLogger(__name__)

    # Inicialize a sessão Spark
    spark = SparkSession.builder.appName("airflow_dags_spark_silver_spark.py").getOrCreate()

    logger.info(f"Spark iniciado.")

    time.sleep(60) 

    logger.info(f"Tempo 60s concluido.")

    # Encerre a sessão Spark
    spark.stop()

if __name__ == "__main__":
    main()
