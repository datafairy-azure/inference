from azure.ai.ml import MLClient
from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
from azureml.core.authentication import InteractiveLoginAuthentication
import yaml
import json
from typing import List
import requests
import logging
from http import client as http_client


def call_endpoint_with_requests(
    request_items: List[str], headers: dict, cfg: dict, request_type: str = "requests"
) -> List[str]:
    """Calls the endpoint with the given request data."""
    response = []

    print("Running logging for " + request_type)
    define_logging_for_request_type(request_type)

    for item in request_items:
        print(item)
        res = requests.post(
            url="https://"
            + cfg["deployments"]["endpoint_name"]
            + "."
            + cfg["connections"]["location"]
            + ".inference.ml.azure.com/score",
            data=item,
            headers=headers,
        )
        print(res.text)
        response.append({"status": str(res.status_code), "prediction": res.text})

    return response


def define_logging_for_request_type(request_type: str):
    http_client.HTTPConnection.debuglevel = 1
    if request_type == "httpx":
        logging.basicConfig(
            format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            level=logging.DEBUG,
        )
    elif request_type == "requests":
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True


def parse_requests(paths: List[str]) -> List[str]:
    """
    Returns a list of request data items from a list for files paths.
    """
    request_items = []
    for path in paths:
        with open(path, "r") as file:
            data = json.load(file)
            request_items.append(json.dumps(data))
    return request_items


def define_headers(cfg: dict) -> dict:
    """Function to define headers for the request to AzureML."""
    credential = DefaultAzureCredential()

    ml_client = MLClient(
        credential=credential,
        subscription_id=cfg["connections"]["subscription_id"],
        resource_group_name=cfg["connections"]["resource_group"],
        workspace_name=cfg["connections"]["workspace"],
    )

    endpoint_cred = ml_client.online_endpoints.get_keys(
        name=cfg["deployments"]["endpoint_name"]
    ).access_token

    headers = {
        "Authorization": f"Bearer {endpoint_cred}",
        "Content-type": "application/json",
    }

    return headers


def define_headers_with_sp(endpoint: str, config_path="") -> dict:
    ml_client = ml_connect_with_sp(config_path)

    endpoint_cred = ml_client.online_endpoints.get_keys(name=endpoint).access_token

    headers = {
        "Authorization": f"Bearer {endpoint_cred}",
        "Content-type": "application/json",
    }

    return headers


def load_config(config_path: str) -> dict:
    """Function to load config file."""
    cfg = []
    with open(config_path, "r") as ymlfile:
        cfg = yaml.safe_load(ymlfile)
    return cfg


def ml_connect_with_sp(config_path: str) -> MLClient:
    credential = DefaultAzureCredential()
    credential.get_token("https://management.azure.com/.default")

    # try:
    #     ml_client = MLClient.from_config(credential=credential)
    # except Exception:
    ml_client = MLClient.from_config(credential=credential, path=config_path)

    return ml_client


def ml_connect(credential_type: str, cfg: dict) -> MLClient:
    """Function to connect to ML workspace.

    Args:
        cfg: Dict with config values.
        credential_type: Type of credential.

    Returns:
        MLClient.
    """
    credential = get_credential(credential_type)

    try:
        credential.get_token("https://management.azure.com/.default")
    except Exception:
        credential = InteractiveLoginAuthentication(
            tenant_id=cfg["connections"]["tenant_id"]
        )

    return MLClient(
        credential,
        cfg["connections"]["subscription_id"],
        cfg["connections"]["resource_group"],
        cfg["connections"]["workspace"],
    )


def get_credential(credential_type: str) -> DefaultAzureCredential:
    """Function to get credential.

    Args:
        credential_type: Type of credential.

    Returns:
        Credential.
    """
    if credential_type == "default":
        return DefaultAzureCredential()
    elif credential_type == "interactive":
        return InteractiveBrowserCredential()
    else:
        raise ValueError("Invalid credential type.")

    # https://learn.microsoft.com/en-us/azure/machine-learning/how-to-setup-authentication?view=azureml-api-2&tabs=sdk
