import json_stream
from typing import Any, Dict, List, Tuple
import time
import os
import requests
from dotenv import load_dotenv

load_dotenv()

ADDRESS_MAPPINGS = {
    "0x49ae63056b3a0be0b166813ee687309ab653c07c": "NFTINITCOM",
    "0xc74812f67eddaf2f3aed6e061eaa9168b36d7ea1": "FASTER100X",
    "0x507c7777837b85ede1e67f5a4554ddd7e58b1f87": "RAGE_TRADE",
    "0xf7aae720d79b062ec268ac8e4389be68baf8f9d3": "HYPER_WALLET",
    "0xc1a2f762f67af72fd05e79afa23f8358a4d7dbaf": "TRUSTMEBROS",
    "0x05984fd37db96dc2a11a09519a8def556e80590b": "OKTO",
}


def format_address(address: str, codes_map: Dict[str, str]) -> str:
    if address in codes_map:
        return codes_map[address]
    if address in ADDRESS_MAPPINGS:
        return ADDRESS_MAPPINGS[address]

    return address


def parse_json_file() -> Tuple[Dict[str, float], str]:
    try:
        with open("data.json", "r") as f:
            data: Any = json_stream.load(f)

            # Access the fees_tracker data
            exchange = data["exchange"]
            snapshot_time = exchange["context"]["time"]
            print(f"\nSnapshot Time: {snapshot_time}")

            fee_tracker = exchange["fee_tracker"]
            code_to_referrer = fee_tracker["code_to_referrer"]

            # Create address to code mapping
            codes_map = {referrer: code for code, referrer in code_to_referrer}

            builder_fees = fee_tracker["collected_builder_fees"]

            fee_entries = {}
            for builder_entry in builder_fees:
                builder_address = builder_entry[0]
                fees = builder_entry[1][0][1]
                # Convert amount to USD (assuming the amount is in wei, divide by 10^6)
                actual_amount = fees / (10**8)
                formatted_address = format_address(builder_address, codes_map)
                fee_entries[formatted_address] = actual_amount

            # Sort entries by amount in descending order for display
            sorted_entries = sorted(
                fee_entries.items(), key=lambda x: x[1], reverse=True
            )

            print("\nBuilder to Total Fees Collected (Sorted by Amount):")
            for formatted_address, actual_amount in sorted_entries:
                print(f"{formatted_address}: ${actual_amount:,.0f}")

            return fee_entries, snapshot_time

    except FileNotFoundError:
        print(f"Error: File not found.")
        raise
    except Exception as e:
        print(f"Error parsing JSON: {str(e)}")
        raise


def send_to_api(fee_entries: Dict[str, float], snapshot_time: str) -> bool:
    token = os.getenv("BUILDER_CODES_TOKEN")
    host = os.getenv("BUILDER_CODES_HOST")

    api_url = f"{host}/api/v1/builder_codes_snapshots"

    if not token:
        print("Error: BUILDER_CODES_TOKEN environment variable not set")
        return False

    payload = {
        "builder_codes_snapshot": {
            "data": fee_entries,
            "taken_at": snapshot_time,
        }
    }

    headers = {"X-Token": token, "Content-Type": "application/json"}

    try:
        response = requests.post(api_url, json=payload, headers=headers)

        if response.status_code == 201:
            print("\nSuccessfully sent data to API")
            return True
        else:
            print(f"\nError sending data to API: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"Error sending data to API: {str(e)}")
        return False


if __name__ == "__main__":
    start_time = time.time()
    try:
        fee_entries, snapshot_time = parse_json_file()
        send_to_api(fee_entries, snapshot_time)
    except Exception as e:
        print(f"Error: {str(e)}")
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nScript execution time: {execution_time:.2f} seconds")
