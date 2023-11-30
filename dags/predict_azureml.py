import os

import pendulum
from typing import List
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from azure.ai.ml import Input, command
from airflow.operators.python import PythonOperator
from azure.ai.ml.entities import ComputeInstance
from azure.ai.ml.constants import AssetTypes, InputOutputModes
from airflow_provider_azure.azure_service_bus.operators.service_bus import AzureServiceBusReceiveMessageOperator

from airflow_provider_azure_machinelearning.operators.machine_learning.job import (
    AzureMachineLearningCreateJobOperator,
)
from airflow_provider_azure_machinelearning.operators.machine_learning.compute import (
    AzureMachineLearningCreateComputeResourceOperator,
    AzureMachineLearningDeleteComputeResourceOperator,
)

with DAG(
    dag_id="AML_predict_job",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval="0 17 * * *",
    tags=["AML predict"],
) as dag:
    # set the connection to the Azure ML workspace and provide the model information
    connection_id = "azure-ml-ws-conn"
    model_info = "credit-default-model:1"

    compute1 = ComputeInstance(
        name="af-test-instance",
        size="Standard_D2s_v3",  # spec for a compute instance
    )

    amlc_create_1 = AzureMachineLearningCreateComputeResourceOperator(
        task_id="create_compute_instance",
        conn_id=connection_id,
        compute=compute1,
        waiting=True,
    )

    amlc_delete_1 = AzureMachineLearningDeleteComputeResourceOperator(
        task_id="delete_computer_instance",
        conn_id=connection_id,
        compute_name=compute1.name,
    )

    curr_dir = os.path.dirname(os.path.abspath(__file__))
    code_file_path = os.path.join(curr_dir, "/opt/airflow/jobs/predict/src")

    predict_command_job = command(
        code=code_file_path,
        command="python main.py --endpoint ${{inputs.endpoint}} --input_config_yaml ${{inputs.input_config_yaml}}",
        environment=model_info,
        inputs={
            "input_config_yaml": Input(
                mode=InputOutputModes.RO_MOUNT,
                type=AssetTypes.URI_FILE,
                path="azureml://datastores/xxx/paths/credit_defaults_model/config/config.yml",
            ),
            "input_data_folder": Input(
                mode=InputOutputModes.RO_MOUNT,
                type=AssetTypes.URI_Folder,
                path="azureml://datastores/xxx/paths/credit_defaults_model/data",
            ),
        },
        compute=compute1.name,
        display_name="predict_from_airflow",
        experiment_name="testing-airflow",
        description="predict command job",
    )
    predict_task = AzureMachineLearningCreateJobOperator(
        task_id="predict",
        job=predict_command_job,
        waiting=True,
        conn_id=connection_id,
    )

    receive_message_service_bus_queue = AzureServiceBusReceiveMessageOperator(
    task_id="receive_message_service_bus_queue",
    queue_name="QUEUE_NAME",
    max_message_count=20,
    max_wait_time=5,
)

    def download_results(messages: List[str]):
        pass
        

    download_results = PythonOperator(
        task_id="download_results",
        python_callable=download_results,
        provide_context=True,
    )

    start_task = EmptyOperator(task_id="start")
    success_task = EmptyOperator(task_id="success")

    start_task >> amlc_create_1 >> predict_task >> [amlc_delete_1, download_results] >> success_task
