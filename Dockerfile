FROM quay.io/astronomer/astro-runtime:9.1.0

COPY requirements.txt /opt/airflow/requirements.txt

COPY dags/data /opt/airflow/data

COPY dags/config /opt/airflow/config

COPY dags/jobs /opt/airflow/jobs

USER root

RUN pip install -r /opt/airflow/requirements.txt

RUN pip install /opt/airflow/jobs/predict/src
