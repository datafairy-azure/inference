from airflow.decorators import dag, task
from typing import List
import pendulum as pm
import json

from inference.utils import define_headers_with_sp, call_endpoint_with_requests


@dag(
    schedule=None,
    start_date=pm.datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["predict"],
)
def predict_with_airflow():
    """
    ### Predict pipeline
    """

    @task()
    def trigger_inference_job() -> List[str]:
        """
        #### Calls an endpoint with the request data given
        """

        paths = [
            "/opt/airflow/data/sample_request_1.json",
            "/opt/airflow/data/sample_request_2.json",
        ]

        cfg = {
            "deployments": {"endpoint_name": "datalab-ml-ws-ifrmw"},
            "connections": {"location": "westeurope"},
        }

        headers = define_headers_with_sp(
            config_path="/opt/airflow/config/config.json",
            endpoint=cfg["deployments"]["endpoint_name"],
        )

        request_items = []

        for path in paths:
            with open(path, "r") as file:
                data = json.load(file)
                request_items.append(json.dumps(data))

        print("Running inference job...using " + "httpx")

        response = call_endpoint_with_requests(request_items, headers, cfg, "httpx")

        return response

    print(trigger_inference_job())


predict_with_airflow()
