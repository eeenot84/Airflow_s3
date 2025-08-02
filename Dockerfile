FROM apache/airflow:2.9.1

USER root

COPY ./airflow_project /opt/airflow/dags

USER airflow