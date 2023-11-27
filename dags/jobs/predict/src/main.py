import argparse
import sys, os

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from inference.utils import (
    define_headers,
    load_config,
    call_endpoint_with_requests,
    parse_requests,
)


def parse_args():
    """Parse input arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_data_folder", type=str, help="location of the data")
    parser.add_argument(
        "--input_config_yaml", type=str, help="config location on azure"
    )
    return parser.parse_args()


def main(args):
    cfg = load_config(args.input_config_yaml)
    headers = define_headers(cfg)
    paths = [
        args.input_data_folder + "sample_request_1.json",
        args.input_data_folder + "sample_request_2.json",
    ]

    request_items = parse_requests(paths)

    return call_endpoint_with_requests(request_items, headers, cfg, "httpx")


if __name__ == "__main__":
    args = parse_args()

    response = main(args)
    print(response)
