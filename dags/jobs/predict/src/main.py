import argparse
import sys, os
import glob

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

    paths = glob.glob(args.input_data_folder + "/*.json")

    print(paths)

    request_items = parse_requests(paths)

    print(request_items)

    return call_endpoint_with_requests(request_items, headers, cfg, "requests")


if __name__ == "__main__":
    args = parse_args()

    response = main(args)
    print(response)
