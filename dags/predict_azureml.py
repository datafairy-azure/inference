import os
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from azure.ai.ml import Input, command
from azure.ai.ml.entities import ComputeInstance
from azure.ai.ml.constants import AssetTypes, InputOutputModes
from airflow_provider_azure_machinelearning.operators.machine_learning.job import (
    AzureMachineLearningCreateJobOperator,
)
from airflow_provider_azure_machinelearning.operators.machine_learning.compute import (
    AzureMachineLearningCreateComputeResourceOperator,
    AzureMachineLearningDeleteComputeResourceOperator,
)

with DAG(
    dag_id="AML_predict_job_default",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval="0 17 * * *",
    tags=["AML predict"]
) as dag:
    # set the connection to the Azure ML workspace and provide the model information
    connection_id = "azure-ml-ws-conn"
    model_info = "credit-default-model:1"

    compute1 = ComputeInstance(
        name="compute-123-inference",
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
        "AZURE_CLIENT_SECRET": "xxx",
        "AZURE_TENANT_ID": "xxx",
        "AZURE_CLIENT_ID": "xxx",
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
                path="azureml://datastores/xxx/paths/credit_defaults_model/data",
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

    start_task = EmptyOperator(task_id="start")
    success_task = EmptyOperator(task_id="success")

    start_task >> amlc_create_1 >> predict_task >> amlc_delete_1 >> success_task
