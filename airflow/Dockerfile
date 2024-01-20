# Use a imagem oficial do Apache Airflow como base
FROM apache/airflow:latest

# Mude para o usuário root temporariamente para instalar pacotes
USER root

# Instale as dependências adicionais
RUN sudo apt-get update

RUN apt-get install -y openjdk-11-jre


# Mude de volta para o usuário padrão do Airflow
USER airflow

RUN pip install pyspark


RUN pip install apache-airflow-providers-apache-spark

RUN pip install --upgrade pip

RUN pip3 install apache-airflow-providers-apache-hive

RUN pip install wget

# Airflow
ARG SPARK_VERSION=3.0.0
ARG HADOOP_VERSION=3.2


# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

RUN export JAVA_HOME
## Finished JAVA installation

## SPARK files and variables

ENV SPARK_HOME /usr/local/spark

# Mude para o usuário root temporariamente para instalar pacotes
USER root

RUN mkdir -p "${SPARK_HOME}"

# Mude de volta para o usuário padrão do Airflow
USER airflow

# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
RUN cd "/tmp" 
RUN curl -L -o spark-3.0.0-bin-hadoop3.2.tgz https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz && \
    echo "Downloading Spark..."
RUN tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    echo "Extracting Spark..." 
    
# Mude para o usuário root temporariamente para instalar pacotes
USER root

RUN mkdir -p "${SPARK_HOME}/bin" 
RUN echo "Criar pasta Spark..." 
RUN mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" 
RUN echo "Criar pasta jars..." 

RUN cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" 
RUN echo "Copiando bin..." 
RUN cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" 
RUN echo "Copiando jars..." 


# Crie a pasta python e copie o conteúdo
RUN mkdir -p "${SPARK_HOME}/python" 
RUN cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/python/." "${SPARK_HOME}/python/" 

#copiando as permissões da pasta principal do Spark para a pasta python
RUN chown -R --reference="${SPARK_HOME}" "${SPARK_HOME}/python/"


ENV PYTHONPATH="${SPARK_HOME}/python:$PYTHONPATH"
ENV PATH="${SPARK_HOME}/python:$PATH"

ENV PYSPARK_PYTHON="/usr/local/bin/python"  
ENV PYSPARK_DRIVER_PYTHON="/usr/local/bin/python"

RUN rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" 
RUN echo "Removendo arquivo tgz baixado..."

# Mude de volta para o usuário padrão do Airflow
USER airflow

# Create SPARK_HOME env var
RUN export SPARK_HOME
ENV PATH $PATH:/usr/local/spark/bin

## Finished SPARK files and variables


