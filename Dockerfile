FROM quay.io/astronomer/astro-runtime:9.1.0

COPY requirements.txt /opt/airflow/requirements.txt

COPY dags/data /opt/airflow/data

COPY dags/jobs /opt/airflow/jobs

COPY src/__init__.py /opt/airflow/src/__init__.py

COPY src/pyproject.toml /opt/airflow/src/pyproject.toml

USER root

RUN pip install -r /opt/airflow/requirements.txt

RUN pip install /opt/airflow/jobs/predict/src
