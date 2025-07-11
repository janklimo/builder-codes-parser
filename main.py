import json_stream
from typing import Any, Dict, Tuple
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
    "0x2868fc0d9786a740b491577a43502259efa78a39": "INSILICO",
    "0xb84168cf3be63c6b8dad05ff5d755e97432ff80b": "PHANTOM",
    "0xa47d4d99191db54a4829cdf3de2417e527c3b042": "PEAR",
}


def format_address(address: str, codes_map: Dict[str, str]) -> str:
    if address in ADDRESS_MAPPINGS:
        return ADDRESS_MAPPINGS[address]
    if address in codes_map:
        return codes_map[address]

    return address


def parse_json_file() -> Tuple[Dict[str, float], str, float]:
    try:
        with open("data.json", "r") as f:
            data: Any = json_stream.load(f)

            # Access the fees_tracker data
            exchange = data["exchange"]
            snapshot_time = exchange["context"]["time"]
            print(f"\nSnapshot Time: {snapshot_time}")

            fee_tracker = exchange["fee_tracker"]

            # Parse user_states for referral rewards
            user_states = fee_tracker["user_states"]
            referral_fees = {}

            for user_entry in user_states:
                user_data = user_entry[1]

                referrer_address = user_data.get("r")

                if referrer_address:
                    t_array = user_data.get("T", [])

                    # Initialize referrer in dictionary if not present
                    if referrer_address not in referral_fees:
                        referral_fees[referrer_address] = 0

                    # Parse T array for referral rewards
                    for t_entry in t_array:
                        reward = t_entry[1].get("r", 0)
                        # Convert to actual amount (assuming same format as fees)
                        actual_reward = reward / (10**8)
                        referral_fees[referrer_address] += actual_reward

            # Calculate and print total referrer rewards paid out
            total_referral_fees = sum(referral_fees.values())
            print(f"\nTotal Referrer Rewards Paid Out: ${total_referral_fees:,.0f}")

            # Sort referrer rewards by amount in descending order
            sorted_referral_fees = sorted(
                referral_fees.items(), key=lambda x: x[1], reverse=True
            )

            print("\nReferrer to Total Rewards (Sorted by Amount):")
            for referrer_address, total_reward in sorted_referral_fees:
                print(f"{referrer_address}: ${total_reward:,.0f}")

            # Get code mapping first
            code_to_referrer = fee_tracker["code_to_referrer"]
            codes_map = {referrer: code for code, referrer in code_to_referrer}

            # Parse builder fees
            builder_fees = fee_tracker["collected_builder_fees"]

            fee_entries = {}
            for builder_entry in builder_fees:
                builder_address = builder_entry[0]
                fees = builder_entry[1][0][1]
                # Convert amount to USD
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

            return fee_entries, snapshot_time, total_referral_fees

    except FileNotFoundError:
        print(f"Error: File not found.")
        raise
    except Exception as e:
        print(f"Error parsing JSON: {str(e)}")
        raise


def send_to_api(
    fee_entries: Dict[str, float], snapshot_time: str, total_referral_fees: float
) -> bool:
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
            "total_referral_fees": total_referral_fees,
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
        fee_entries, snapshot_time, total_referral_fees = parse_json_file()
        send_to_api(fee_entries, snapshot_time, total_referral_fees)
    except Exception as e:
        print(f"Error: {str(e)}")
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nScript execution time: {execution_time:.2f} seconds")
