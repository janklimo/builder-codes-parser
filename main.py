import json_stream
from pathlib import Path
from typing import Any
import time


def parse_json_file(file_path: str):
    """
    Parse a JSON file using json-stream.

    Args:
        file_path (str): Path to the JSON file
    """
    try:
        with open(file_path, "r") as f:
            data: Any = json_stream.load(f)

            # Access the fees_tracker data
            exchange = data["exchange"]
            snapshot_time = exchange["context"]["time"]
            print(f"\nSnapshot Time: {snapshot_time}")

            fee_tracker = exchange["fee_tracker"]
            code_to_referrer = fee_tracker["code_to_referrer"]

            for entry in code_to_referrer:
                code = entry[0]
                referrer = entry[1]
                print(f"  Code: {code} -> Referrer: {referrer}")

            builder_fees = fee_tracker["collected_builder_fees"]

            print("\nBuilder to Total Fees Collected:")
            for builder_entry in builder_fees:
                # Every item is an array with two elements:
                # ["0x0cbf655b0d22ae71fba3a674b0e1c0c7e7f975af", [[0, 707674919034866]]]
                builder_address = builder_entry[0]
                fees = builder_entry[1][0][1]
                print(f"  {builder_address}: {fees}")

    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except Exception as e:
        print(f"Error parsing JSON: {str(e)}")


if __name__ == "__main__":
    start_time = time.time()

    parse_json_file("data.json")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nScript execution time: {execution_time:.2f} seconds")
