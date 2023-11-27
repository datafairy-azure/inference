# Connection details:
# Create a connection to Azure Blob Storage and Azure Data Lake Storage Gen2
# Blob storage:
# connection_id: unique string to identify the connection
# conn_type: Azure Blob Storage
# SAS Token: SAS URL to the storage account not the container
# Data Lake Storage Gen2:
# connection_id: unique string to identify the connection
# conn_type: Azure Data Lake Storage Gen2
# ADLS Gen2 Connection String: Connection string from SAS

import pendulum

from airflow.decorators import dag, task
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def tutorial_azure_storage():
    """Template on how to connect to Azure Blob Storage and Azure DAta Lake Storage Gen2."""

    @task()
    def extract_using_wasb():
        blob_connection = WasbHook(wasb_conn_id="connection_id_blob")
        data = len(blob_connection.get_blobs_list("blob_container"))
        return data

    order_data = extract_using_wasb()
    print(order_data)

    @task()
    def extract_using_adls():
        blob_connection = AzureDataLakeStorageV2Hook(adls_conn_id="connection_id_adls")
        data = blob_connection.list_file_system()
        return data

    order_data = extract_using_adls()
    print(order_data)

    extract_using_adls()


tutorial_azure_storage()
