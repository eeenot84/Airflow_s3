FROM apache/airflow:2.9.1

USER root

COPY ./dags/ /opt/airflow/dags/

USER airflow