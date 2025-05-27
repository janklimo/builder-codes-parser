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
            fee_tracker = exchange["fee_tracker"]
            builder_fees = fee_tracker["builder_to_total_fees_collected"]

            print("\nBuilder to Total Fees Collected:")
            for builder_entry in builder_fees:
                builder_address = builder_entry[0]
                fees = builder_entry[1]
                print(f"  {builder_address}: {fees}")

    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except Exception as e:
        print(f"Error parsing JSON: {str(e)}")


if __name__ == "__main__":
    start_time = time.time()

    # Get the path to data.json relative to the script
    json_file = Path(__file__).parent / "data.json"
    parse_json_file(str(json_file))

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nScript execution time: {execution_time:.2f} seconds")
