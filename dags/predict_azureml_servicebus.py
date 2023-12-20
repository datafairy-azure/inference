import os
import pendulum
from typing import List
import json
from inference.etl import load_data, prepare_requests, clean_request
from inference.request import Request

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from azure.ai.ml import Input, command
from azure.ai.ml.entities import ComputeInstance
from azure.ai.ml.constants import AssetTypes, InputOutputModes
from airflow.models.baseoperator import chain

from airflow.decorators import task
from airflow.operators.python import PythonOperator

from airflow.providers.microsoft.azure.operators.asb import (
    AzureServiceBusReceiveMessageOperator,
)

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
     default_args={
        "azure_service_bus_conn_id": "azure-sb-conn",
    },
) as dag:
    # set the connection to the Azure ML workspace and provide the model information
    connection_id = "azure-ml-ws-conn"
    model_info = "credit-default-model:1"
    queue_name = "test_queue"

    def get_list_of_raw_data() -> List[str]:
        """
        #### Dummy list of raw data. 
        """
        return [
            '{"input_data": {"columns": [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22],"index": [0, 1],"data": [[20000,2,2,1,24,2,2,-1,-1,-2,-2,3913,3102,689,0,0,0,0,689,0,0,0,0],[10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 10, 9, 8]]}}',
            '{"input_data": {"columns": [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22],"index": [0, 1],"data": [[3913,3102,689,0,0,0,0,689,0,0,0,0,20000,2,2,1,24,2,2,-1,-1,-2,-2],[10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 10, 9, 8]]}}',
            '{"input_data": {"columns": [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22],"index": [0, 1],"data": [[3913,3102,-689,0,0,0,0,-689,0,0,0,0,20000,2,2,1,-24,2,-2,1,1,2,2],[10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 10, 9, 8]]}}',
        ]
    
    def load_data_task() -> List[Request]:
        """
        #### Extract task
        """
        ordered_data = load_data(get_list_of_raw_data())

        requests: List[Request] = [Request(**item) for item in ordered_data]

        return requests

    def clean_data_task(task_instance, **kwargs) -> List[str]:
        """
        #### Clean data task
        """
        cleaned_requests = []

        requests = task_instance.xcom_pull(task_ids="load_data_task")

        for request in requests:
            cleaned_requests.append(json.dumps(clean_request(request)))

        return cleaned_requests

    def prepare_requests_task(task_instance, **kwargs):
        """
        #### Load task: load cleaned data into database
        """
        cleaned_requests = task_instance.xcom_pull(task_ids="clean_data_task")

        request_locations = prepare_requests(cleaned_requests)

        return request_locations
    
    load_data_task = PythonOperator(task_id="load_data_task", python_callable=load_data_task)
    clean_data_task = PythonOperator(task_id="clean_data_task", python_callable=clean_data_task)
    prepare_requests_task = PythonOperator(task_id="prepare_requests_task", python_callable=prepare_requests_task)

    compute1 = ComputeInstance(
        name="compute-321-inference",
        size="Standard_D2s_v3",  # spec for a compute instance
    )

    amlc_create_1 = AzureMachineLearningCreateComputeResourceOperator(
        task_id="create_compute_instance",
        conn_id=connection_id,
        compute=compute1,
        waiting=True,
    )

    amlc_delete_1 = AzureMachineLearningDeleteComputeResourceOperator(
        task_id="delete_compute_instance",
        conn_id=connection_id,
        compute_name=compute1.name,
    )
    # Service principal will need Azure Service Bus Data Owner role
    env_servicebus = {
        "AZURE_CLIENT_SECRET": "",
        "AZURE_TENANT_ID": "",
        "AZURE_CLIENT_ID": "",
    }

    curr_dir = os.path.dirname(os.path.abspath(__file__))
    code_file_path = os.path.join(curr_dir, "/opt/airflow/jobs/predict/src")

    predict_command_job = command(
        code=code_file_path,
        command="python main.py --input_data_folder ${{inputs.input_data_folder}} --input_config_yaml ${{inputs.input_config_yaml}}",
        environment="credit-default-env:4",
        inputs={
            "input_config_yaml": Input(
                mode=InputOutputModes.RO_MOUNT,
                type=AssetTypes.URI_FILE,
                path="azureml://datastores/xxx/paths/credit_defaults_model/config/config.yml",
            ),
            "input_data_folder": Input(
                mode=InputOutputModes.RO_MOUNT,
                type=AssetTypes.URI_FOLDER,
                path="azureml://datastores/xxx/paths/inference_input",
            ),
        },
        environment_variables=env_servicebus,
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

    check_job_status =  AzureServiceBusReceiveMessageOperator(
    task_id="receive_message_service_bus_queue",
    queue_name=queue_name,
    max_message_count=20,
    max_wait_time=5,
    )

    start_task = EmptyOperator(task_id="start")
    success_task = EmptyOperator(task_id="success")

    start_task >>  amlc_create_1 >> predict_task >> [amlc_delete_1, check_job_status] >> success_task
    start_task >> load_data_task >> clean_data_task >> prepare_requests_task >> predict_task
