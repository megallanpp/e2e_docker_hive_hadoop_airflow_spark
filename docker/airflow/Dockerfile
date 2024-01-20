FROM python:3.10-slim-buster

# FROM python:3.8

ARG AIRFLOW_VERSION=2.4.3
ARG AIRFLOW_HOME=/opt/airflow


ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Install dependencies and tools

RUN apt-get update -y && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \
    python3-dev \
    wget \
    libczmq-dev \
    unixodbc-dev \
    curl \
    libssl-dev \
    git \
    inetutils-telnet \
    bind9utils freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    libffi-dev libpq-dev \
    freetds-bin build-essential \
    default-libmysqlclient-dev \
    apt-utils \
    rsync \
    zip \
    unzip \
    gcc \
    gnupg \
    gnupg2 \
    vim \
    netcat \
    && apt-get autoremove -yqq --purge && apt-get clean

COPY ./constraints-3.10.txt /constraints-3.10.txt
COPY ./requirements.txt /requirements.txt

ENV DEBIAN_FRONTEND noninteractive

# Copy the entrypoint.sh from host to container (at path AIRFLOW_HOME)
# COPY ./install-odbc-mssql.sh ./install-odbc-mssql.sh

# # Set the entrypoint.sh file to be executable
# RUN chmod +x ./install-odbc-mssql.sh

# RUN apt-get update \
#     && apt-get install -y curl apt-transport-https \
#     && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
#     && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
#     && apt-get update \
#     && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev
    
###############################
## Begin JAVA installation
###############################
# Java is required in order to spark-submit work
# Install OpenJDK-8
# RUN apt-get update && \
#     apt-get install -y software-properties-common && \
#     apt-get install -y gnupg2 && \
#     apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
#     # add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \ 
#     echo "deb http://deb.debian.org/debian stretch-backports main" > /etc/apt/sources.list.d/stretch-backports.list && \
#     apt-get update && \
#     # apt-get install -y openjdk-8-jdk && \
#     apt-get install -y -t stretch-backports openjdk-8-jdk && \
#     pip freeze && \
#     java -version $$ \
#     javac -version

# RUN echo "deb http://deb.debian.org/debian stretch-backports main" > /etc/apt/sources.list.d/stretch-backports.list && \
#     apt-get update && \
#     apt-get install -y -t stretch-backports openjdk-8-jdk && \
#     apt-get clean    

# RUN echo "deb http://deb.debian.org/debian buster-backports main" > /etc/apt/sources.list.d/buster-backports.list && \
#     apt-get update && \
#     apt-get install -y -t buster-backports openjdk-8-jdk && \
#     apt-get clean

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 3B4FE6ACC0B21F32 && \
    add-apt-repository "deb http://archive.ubuntu.com/ubuntu bionic main universe" && \
    apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get clean


# Upgrade pip
# Create airflow user 
# Install apache airflow with subpackages
RUN pip install --upgrade "pip==23.0.1"

RUN pip install psycopg2 && \
    pip install pyodbc && \
    useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && \
    pip install apache-airflow[postgres,mssql]==${AIRFLOW_VERSION} --constraint /constraints-3.10.txt && \
    pip install -r /requirements.txt && \
    pip install apache-airflow-providers-microsoft-mssql==3.3.2 && \
    pip install apache-airflow-providers-common-sql==1.4.0 && \
    pip install apache-airflow-providers-microsoft-azure==5.3.0 && \
    pip install apache-airflow-providers-amazon==7.4.1 && \
    pip install apache-airflow-providers-apache-spark==4.0.1 

RUN pip install apache-airflow-providers-apache-hive




#Atualizar as versões do OpenSSL para TLSv1
RUN chmod +rwx /etc/ssl/openssl.cnf
RUN sed -i 's/TLSv1.2/TLSv1/g' /etc/ssl/openssl.cnf
RUN sed -i 's/SECLEVEL=2/SECLEVEL=1/g' /etc/ssl/openssl.cnf

# Copy the airflow.cfg file (config)
COPY ./airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# Set the owner of the files in AIRFLOW_HOME to the user airflow
RUN chown -R airflow: ${AIRFLOW_HOME}

# Copy the entrypoint.sh from host to container (at path AIRFLOW_HOME)
COPY ./startAirflow.sh ./startAirflow.sh

# Set the entrypoint.sh file to be executable
RUN chmod +x ./startAirflow.sh

# Set the username to use
USER airflow

# Create the folder dags inside $AIRFLOW_HOME
RUN mkdir -p ${AIRFLOW_HOME}/dags
RUN mkdir -p ${AIRFLOW_HOME}/sql
# Expose ports (just to indicate that this container needs to map port)
EXPOSE 8080


# Execute startAirflow.sh
CMD [ "./startAirflow.sh" ]