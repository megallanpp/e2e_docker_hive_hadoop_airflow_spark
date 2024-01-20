#!/usr/bin/env bash

# Move to the AIRFLOW HOME directory
cd $AIRFLOW_HOME

# Export environement variables
export AIRFLOW__CORE__EXECUTOR: LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
export AIRFLOW__API__AUTH_BACKENDS='airflow.api.auth.backend.basic_auth'

#por causa do hive precisou disso
airflow db reset -y

# Initiliase the metadatabase
airflow db init

#por causa do hive precisou disso
airflow db upgrade

# Create User
airflow users create -e "admin@airflow.com" -f "airflow" -l "airflow" -p "airflow" -r "Admin" -u "airflow"

# Run the scheduler in background
airflow scheduler &> /dev/null &

# Run the web sever in foreground (for docker logs)
exec airflow webserver