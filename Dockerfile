FROM apache/airflow:2.9.1

USER root

COPY ./airflow_project/dags/ /opt/airflow/dags/

USER airflow