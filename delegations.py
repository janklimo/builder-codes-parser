import json_stream
from typing import Any
import time
from collections import defaultdict
import csv
import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timezone
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

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
            children.append({"address": "\U0001f990", "amount": round(shrimp_sum, 2)})
        if dolphin_sum > 0:
            children.append({"address": "\U0001f42c", "amount": round(dolphin_sum, 2)})

        # Sort all children by amount (descending) to maintain proper order
        children.sort(key=lambda x: x["amount"], reverse=True)

        if children:
            result["children"].append({"address": validator, "children": children})
    return result


def calculate_validator_stats(validator_delegations):
    """
    Calculate statistics for validators:
    - Number of delegations per validator
    - Total delegated stake per validator (in tokens)

    Returns a tuple of (delegations_count_dict, total_stake_dict)
    """
    delegations_count = {}
    total_stake = {}

    for validator_address, delegations in validator_delegations:
        # Count number of delegations
        delegations_count[validator_address] = len(delegations)

        # Calculate total stake in tokens (wei / 10^8)
        total_wei = sum(wei for _, wei in delegations)
        total_tokens = total_wei / (10**8)
        total_stake[validator_address] = round(total_tokens, 2)

    return delegations_count, total_stake


def generate_timestamped_filename():
    """
    Generate a timestamped filename in UTC format.
    Returns: delegations-YYYY-MM-DDTHH-MM-SSZ.csv
    """
    utc_now = datetime.now(timezone.utc)
    timestamp = utc_now.strftime("%Y-%m-%dT%H-%M-%SZ")
    return f"delegations-{timestamp}.csv"


def save_delegations_to_csv(validator_delegations, filename="delegations.csv"):
    """
    Save all delegations to a CSV file in the format:
    Validator address, Staker address, Amount

    Args:
        validator_delegations: List of (validator_address, delegations_list) tuples
        filename: Name of the CSV file to create
    """
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        # Write header
        writer.writerow(["Validator address", "Staker address", "Amount"])

        # Write data
        for validator_address, delegations in validator_delegations:
            for staker_address, wei_amount in delegations:
                # Convert wei to tokens (divide by 10^8)
                token_amount = wei_amount / (10**8)
                writer.writerow(
                    [validator_address, staker_address, f"{token_amount:.8f}"]
                )

    print(f"Delegations saved to {filename}")


def upload_to_r2(local_filename, r2_filename):
    """
    Upload a file to Cloudflare R2 bucket.

    Args:
        local_filename: Path to the local file to upload
        r2_filename: Name to use for the file in R2

    Returns:
        bool: True if upload successful, False otherwise
    """
    try:
        # Get R2 credentials from environment variables
        account_id = os.getenv("CLOUDFLARE_ACCOUNT_ID")
        access_key = os.getenv("R2_ACCESS_KEY_ID")
        secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
        bucket_name = "hypeburn"

        if not all([account_id, access_key, secret_key]):
            print(
                "Error: Missing R2 environment variables (CLOUDFLARE_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY)"
            )
            return False

        # Create S3 client for R2
        s3 = boto3.client(
            service_name="s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )

        # Upload file with proper content type
        s3.upload_file(
            local_filename,
            bucket_name,
            r2_filename,
            ExtraArgs={"ContentType": "text/csv"},
        )
        print(f"Successfully uploaded {local_filename} to R2 as {r2_filename}")
        return True

    except NoCredentialsError:
        print("Error: R2 credentials not found")
        return False
    except ClientError as e:
        print(f"Error uploading to R2: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error uploading to R2: {e}")
        return False


def send_validators_to_api(
    nivo_data, snapshot_time, delegations_count=None, total_stake=None, filename=None
):
    """
    Send nivo_data to the /validators endpoint as { "staking_snapshot": { "data": data, "taken_at": snapshot_time } }
    Also send delegations_count, total_stake, and filename if provided.
    """
    token = os.getenv("BUILDER_CODES_TOKEN")
    host = os.getenv("BUILDER_CODES_HOST")
    api_url = f"{host}/api/v1/staking_snapshots"
    if not token:
        print("Error: BUILDER_CODES_TOKEN environment variable not set")
        return False

    payload = {"staking_snapshot": {"data": nivo_data, "taken_at": snapshot_time}}

    # Add validator stats if provided
    if delegations_count is not None:
        payload["staking_snapshot"]["validators_delegations_count"] = delegations_count
    if total_stake is not None:
        payload["staking_snapshot"]["validators_total_stake"] = total_stake
    if filename is not None:
        payload["staking_snapshot"]["filename"] = filename

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

        # Calculate and print validator statistics
        delegations_count, total_stake = calculate_validator_stats(
            validator_delegations
        )

        # Sort dictionaries for consistent ordering
        sorted_delegations_count = dict(
            sorted(delegations_count.items(), key=lambda x: x[1], reverse=True)
        )
        sorted_total_stake = dict(
            sorted(total_stake.items(), key=lambda x: x[1], reverse=True)
        )

        print("\n" + "=" * 50)
        print("NUMBER OF DELEGATIONS PER VALIDATOR:")
        print("=" * 50)
        for validator, count in sorted_delegations_count.items():
            print(f"{validator}: {count} delegations")

        print("\n" + "=" * 50)
        print("TOTAL DELEGATED STAKE PER VALIDATOR (tokens):")
        print("=" * 50)
        for validator, stake in sorted_total_stake.items():
            print(f"{validator}: {stake:,.2f} tokens")

        nivo_data = delegations_to_nivo_json(validator_delegations)

        # Save delegations to local CSV file (always overwrite delegations.csv)
        local_filename = "delegations.csv"
        save_delegations_to_csv(validator_delegations, local_filename)

        # Generate timestamped filename for R2 upload
        timestamped_filename = generate_timestamped_filename()

        # Upload CSV to R2 bucket with timestamped filename
        filename_for_api = None
        if upload_to_r2(local_filename, timestamped_filename):
            filename_for_api = timestamped_filename
            print(f"Will include filename {timestamped_filename} in API payload")
        else:
            print("R2 upload failed - filename will not be included in API payload")

        send_validators_to_api(
            nivo_data,
            snapshot_time,
            sorted_delegations_count,
            sorted_total_stake,
            filename_for_api,
        )

    except Exception as e:
        print(f"Error: {str(e)}")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nScript execution time: {execution_time:.2f} seconds")
