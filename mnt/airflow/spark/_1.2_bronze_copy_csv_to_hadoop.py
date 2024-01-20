
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, current_date

def main():
    # Inicialize a sessão Spark
    spark = SparkSession.builder.appName("Hava_Labs_ETL").getOrCreate()

    # Crie uma instância da classe FileSystem do Hadoop
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hdfs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    # Caminho do diretório no HDFS
    hdfs_directory = "hdfs://namenode:9000/data/havanlabs/bronze/"

    # Crie o diretório no HDFS se não existir
    if not hdfs.exists(spark._jvm.org.apache.hadoop.fs.Path(hdfs_directory)):
        hdfs.mkdirs(spark._jvm.org.apache.hadoop.fs.Path(hdfs_directory))

    # Caminho completo do arquivo CSV no HDFS
    hdfs_file_path = hdfs_directory + "cota-parlamentar.csv"

    # Copie o arquivo para o HDFS se não existir
    if not hdfs.exists(spark._jvm.org.apache.hadoop.fs.Path(hdfs_file_path)):
        hdfs.copyFromLocalFile(spark._jvm.org.apache.hadoop.fs.Path("/opt/airflow/cota-parlamentar.csv"),
                               spark._jvm.org.apache.hadoop.fs.Path(hdfs_file_path))

    # Carregue o arquivo CSV
    df = spark.read.csv(hdfs_file_path, header=True, inferSchema=True)

    # Adicione colunas fictícias para ano e mês
    df_with_year_month = df.withColumn("year", year(current_date())) \
                          .withColumn("month", month(current_date()))

    # Salve o DataFrame particionado por ano e mês no formato Parquet
    df_with_year_month.write.mode("overwrite") \
      .partitionBy("year", "month") \
      .parquet("hdfs://namenode:9000/data/havanlabs/bronze/cota-parlamentar_partitioned.parquet")

    # Encerre a sessão Spark
    spark.stop()

if __name__ == "__main__":
    main()
