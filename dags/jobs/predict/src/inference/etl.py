import json
from typing import List
import pydantic
import pendulum as pm

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


class InputData(pydantic.BaseModel):
    columns: List[int]
    index: List[int]
    data: List[List[int]]

    @pydantic.validator("columns")
    @classmethod
    def columns_valid(cls, value) -> None:
        """Validator to check whether columns are valid"""
        if len(value) != 23:
            raise ValueError("Columns should be of length 23")

        for x in value:
            if x not in range(0, 23):
                raise ValueError("Columns should be in range 0-22")

        return value


class Request(pydantic.BaseModel):
    input_data: InputData


def load_data(json_list: List[str]) -> List[dict]:
    """
    The function "load_data" loads data and returns a list.
    """

    ordered_data = []

    for item in json_list:
        ordered_dict = json.loads(item)
        ordered_data.append(ordered_dict)

    return ordered_data


def clean_data(request_data: List[Request]) -> List[str]:
    """The function "clean_data" takes a list of Request objects as input and returns a list of strings.

    Parameters
    ----------
    order_data : List[Request]
        The parameter `order_data` is a list of `Request` objects.

    """
    cleaned_requests = []

    for request in request_data:
        cleaned_dict = clean_request(request)
        cleaned_requests.append(json.dumps(cleaned_dict))


def prepare_requests(cleaned_requests: List[str]):
    """
    #### Load task: load cleaned data into database
    """
    request_locations = []

    blob_connection = WasbHook(wasb_conn_id="connection_id_blob")
    for item in cleaned_requests:
        blob_connection.load_string(
            item,
            "azureml",
            f"inference_input/request_sample_{pm.now().timestamp()}.json",
        )
        request_locations.append(
            f"inference_input/request_sample_{pm.now().timestamp()}.json"
        )

    return request_locations


def clean_request(request: Request) -> dict:
    """
    #### Clean data task: no negative values
    """
    data_items =  []
    cleaned_dict = {"input_data": {}}
    cleaned_dict["input_data"]["columns"] = request.input_data.columns
    cleaned_dict["input_data"]["index"] = request.input_data.index
    # converts negative values to 0
    for data_item in range(len(request.input_data.data)):
        x = [
            max(item, 0) for item in request.input_data.data[data_item]
        ]
        data_items.append(x)
    cleaned_dict["input_data"]["data"] = data_items
    return cleaned_dict