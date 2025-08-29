import json_stream
from typing import Any, Dict, List, Tuple
import time
from collections import defaultdict
import csv
import json
import os
import requests
from dotenv import load_dotenv

load_dotenv()


def parse_delegations() -> tuple:
    """
    Parse delegations data from data.json file.
    Returns a tuple of (validator_delegations, snapshot_time) where:
    - validator_delegations: list of (validator_address, delegations_list) tuples, sorted by total stake (sum of wei) descending
    - snapshot_time: the timestamp from the data
    Each delegations_list is a list of (user_address, token_amount).
    """
    try:
        with open("data.json", "r") as f:
            data: Any = json_stream.load(f)

            # Navigate to the delegation data
            exchange = data["exchange"]
            snapshot_time = exchange["context"]["time"]
            print(f"\nSnapshot Time: {snapshot_time}")

            c_staking = exchange["c_staking"]
            delegations = c_staking["delegations"]
            user_to_delegations = delegations["user_to_delegations"]

            # Dictionary to store validator -> list of delegations
            validator_delegations = defaultdict(list)

            # Parse the user_to_delegations structure
            for user_entry in user_to_delegations:
                user_address = user_entry[0]
                delegations_list = user_entry[1]

                # Process each delegation for this user
                for delegation in delegations_list:
                    validator_address = delegation[0]
                    delegation_info = delegation[1]

                    wei_amount = delegation_info["wei"]

                    # Add to validator's delegation list
                    validator_delegations[validator_address].append(
                        (user_address, wei_amount)
                    )

            # Sort delegations within each validator by wei amount (descending)
            for validator_address in validator_delegations:
                validator_delegations[validator_address].sort(
                    key=lambda delegation: delegation[1],  # Sort by wei amount
                    reverse=True,
                )

            # Sort validators by total stake (descending)
            sorted_validators = sorted(
                validator_delegations.items(),
                key=lambda item: sum(wei for _, wei in item[1]),
                reverse=True,
            )
            return sorted_validators, snapshot_time

    except FileNotFoundError:
        print(f"Error: data.json file not found.")
        raise
    except Exception as e:
        print(f"Error parsing JSON: {str(e)}")
        raise


def delegations_to_nivo_json(validator_delegations):
    """
    Group delegations as follows:
    - > 10,000 tokens: individual entries
    - < 1,000 tokens: grouped as '🦐'
    - >= 1,000 but < 10,000 tokens: grouped as '🐬'
    Output amounts in tokens (float, rounded to 2 decimals).
    """
    result = {"address": "Total Staked", "children": []}
    for validator, delegations in validator_delegations:
        children = []
        shrimp_sum = 0  # < 1000
        dolphin_sum = 0  # 1000 <= x < 10,000
        for user, wei in delegations:
            tokens = wei / (10**8)
            if tokens > 10000:
                children.append({"address": user, "amount": round(tokens, 2)})
            elif tokens < 1000:
                shrimp_sum += tokens
            else:  # 1000 <= tokens <= 10,000
                dolphin_sum += tokens
        if shrimp_sum > 0:
            children.append({"address": "\U0001F990", "amount": round(shrimp_sum, 2)})
        if dolphin_sum > 0:
            children.append({"address": "\U0001F42C", "amount": round(dolphin_sum, 2)})

        # Sort all children by amount (descending) to maintain proper order
        children.sort(key=lambda x: x["amount"], reverse=True)

        if children:
            result["children"].append({"address": validator, "children": children})
    return result


def send_validators_to_api(nivo_data, snapshot_time):
    """
    Send nivo_data to the /validators endpoint as { "staking_snapshot": { "data": data, "taken_at": snapshot_time } }
    """
    token = os.getenv("BUILDER_CODES_TOKEN")
    host = os.getenv("BUILDER_CODES_HOST")
    api_url = f"{host}/api/v1/staking_snapshots"
    if not token:
        print("Error: BUILDER_CODES_TOKEN environment variable not set")
        return False
    payload = {"staking_snapshot": {"data": nivo_data, "taken_at": snapshot_time}}
    print("Sending the following payload to API:", payload)
    headers = {"X-Token": token, "Content-Type": "application/json"}
    try:
        response = requests.post(api_url, json=payload, headers=headers)
        if response.status_code == 201:
            print("\nSuccessfully sent validators data to API")
            return True
        else:
            print(f"\nError sending validators data to API: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"Error sending validators data to API: {str(e)}")
        return False


if __name__ == "__main__":
    start_time = time.time()
    try:
        validator_delegations, snapshot_time = parse_delegations()
        nivo_data = delegations_to_nivo_json(validator_delegations)

        print(json.dumps(nivo_data, indent=2))

        send_validators_to_api(nivo_data, snapshot_time)

    except Exception as e:
        print(f"Error: {str(e)}")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nScript execution time: {execution_time:.2f} seconds")
