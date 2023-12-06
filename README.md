Overview
========

This project contains code to for various topics around Airflow, AzureML, MLOPS and Data Quality. Checkout my articles on Medium for more information on these topics https://medium.com/@datafairy.

Project Contents
================

This project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs.
- dags/data: This folder contains the data files for the DAGs.
- dags/jobs: This folder contains the Python code for the AzureML jobs.
- dags/jobs/inference/src: This folder contains the source code used in the AzureML inference job and Airflow DAGs.

Files needed for Airflow and Pydantic
=====================================

To get Pydantic running you need to create a .env file in the root of the project with the following content:

```AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES=['include.request_model.Request','include.request_model.InputData']```

Extras
======

## Getting started locally:

- Create a virtual environment: python -m venv .venv
- Activate the venv: source .venv/bin/activate or .venv/scripts/activate
- pip install --upgrade setuptools
- pip install --upgrade pip
- pip install -r requirements.txt
- pip install .dags/jobs/inference/src to install the inference package
- setup pre-comit hooks: pre-commit install

## Astronomer CLI Information

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

## Project Contents

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs.
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

## Deploy Your Project Locally

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://docs.astronomer.io/astro/test-and-troubleshoot-locally#ports-are-not-available).

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.
