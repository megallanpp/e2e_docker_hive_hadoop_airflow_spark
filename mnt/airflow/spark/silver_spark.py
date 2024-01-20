import time

from pyspark.sql import SparkSession
import logging

def main():
    # Configuração do logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Inicialize a sessão Spark
        spark = SparkSession.builder.appName("airflow_dags_spark_silver_spark.py").getOrCreate()

        logger.info(f"Spark iniciado.")

        time.sleep(60)

        logger.info(f"Tempo 60s concluído.")

        # Encerre a sessão Spark
        spark.stop()

    except Exception as e:
        logger.error(f"Erro durante a execução do Spark: {str(e)}")
        raise

if __name__ == "__main__":
    main()
