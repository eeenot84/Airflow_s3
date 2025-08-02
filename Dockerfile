FROM apache/airflow:2.9.1

USER root

COPY ./dags/simple_dag.py /opt/airflow/dags/simple_dag.py

USER airflow