import json_stream
from typing import Any, Dict, List, Tuple
import time
from collections import defaultdict
import csv


def parse_delegations() -> list:
    """
    Parse delegations data from data.json file.
    Returns a list of (validator_address, delegations_list) tuples, sorted by total stake (sum of wei) descending.
    Each delegations_list is a list of (user_address, token_amount).
    """
    try:
        with open("data.json", "r") as f:
            data: Any = json_stream.load(f)

            # Navigate to the delegation data
            exchange = data["exchange"]
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

            # Sort validators by total stake (descending)
            sorted_validators = sorted(
                validator_delegations.items(),
                key=lambda item: sum(wei for _, wei in item[1]),
                reverse=True,
            )
            return sorted_validators

    except FileNotFoundError:
        print(f"Error: data.json file not found.")
        raise
    except Exception as e:
        print(f"Error parsing JSON: {str(e)}")
        raise


def display_delegations(validator_delegations):
    """Display delegation information in a readable format"""

    # Count total delegations before filtering
    total_delegations = sum(
        len(delegations) for _, delegations in validator_delegations
    )
    print(f"Total delegations: {total_delegations}")

    # Filter out delegations under 10 tokens (wei < 10^9)
    filtered_validator_delegations = []
    for validator, delegations in validator_delegations:
        filtered_delegations = [
            (user, wei) for user, wei in delegations if wei >= 10**9
        ]
        if (
            filtered_delegations
        ):  # Only include validators that still have delegations after filtering
            filtered_validator_delegations.append((validator, filtered_delegations))

    # Count delegations after filtering
    filtered_delegations_count = sum(
        len(delegations) for _, delegations in filtered_validator_delegations
    )
    print(f"Filtered delegations (>= 10 tokens): {filtered_delegations_count}")
    print(
        f"Removed {total_delegations - filtered_delegations_count} delegations under 10 tokens\n"
    )

    # Use filtered data for the rest of the display
    validator_delegations = filtered_validator_delegations

    print(
        f"Found {len(validator_delegations)} validators with delegations >= 10 tokens\n"
    )

    # Sort validators by total staked amount (descending)
    validator_totals = []
    for validator, delegations in validator_delegations:
        total_wei = sum(delegation[1] for delegation in delegations)
        validator_totals.append((validator, total_wei, len(delegations)))

    validator_totals.sort(key=lambda x: x[1], reverse=True)

    print("=== VALIDATOR SUMMARY (sorted by total staked) ===")
    for validator, total_wei, delegation_count in validator_totals:
        total_tokens = total_wei / (10**8)  # Convert wei to tokens
        print(
            f"{validator}: {total_tokens:.2f} tokens from {delegation_count} delegations"
        )

    print("\n=== DETAILED DELEGATIONS ===")
    for validator, total_wei, delegation_count in validator_totals:
        delegations = dict(validator_delegations)[validator]
        total_tokens = total_wei / (10**8)

        print(f"\nValidator: {validator}")
        print(f"Total Staked: {total_tokens:.2f} tokens ({total_wei} wei)")
        print(f"Number of Delegations: {delegation_count}")
        print("Delegations:")

        # Sort delegations by amount (descending)
        sorted_delegations = sorted(delegations, key=lambda x: x[1], reverse=True)

        for user_address, wei_amount in sorted_delegations:
            token_amount = wei_amount / (10**8)
            print(f"  {user_address}: {token_amount:.2f} tokens")


def delegations_to_nivo_json(validator_delegations):
    """
    Group delegations as follows:
    - > 10,000 tokens: individual entries
    - < 100 tokens: grouped as '🦐'
    - 100 <= tokens < 10,000: grouped as '🐬'
    Output amounts in tokens (float, rounded to 2 decimals).
    """
    result = {"address": "delegations", "children": []}
    for validator, delegations in validator_delegations:
        children = []
        shrimp_sum = 0  # < 100
        dolphin_sum = 0  # 100 <= x < 10,000
        for user, wei in delegations:
            tokens = wei / (10**8)
            if tokens > 10000:
                children.append({"address": user, "amount": round(tokens, 2)})
            elif tokens < 100:
                shrimp_sum += tokens
            else:  # 100 <= tokens <= 10,000
                dolphin_sum += tokens
        if shrimp_sum > 0:
            children.append({"address": "\U0001F990", "amount": round(shrimp_sum, 2)})
        if dolphin_sum > 0:
            children.append({"address": "\U0001F42C", "amount": round(dolphin_sum, 2)})
        if children:
            result["children"].append({"address": validator, "children": children})
    return result


def write_delegations_csv(
    validator_delegations,
    filename: str = "delegations.csv",
):
    """
    Write every user delegation into a CSV file with columns: Validator address | User address | Amount (tokens)
    """
    import csv

    with open(filename, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Validator address", "User address", "Amount (tokens)"])
        for validator, delegations in validator_delegations:
            for user, wei in delegations:
                tokens = wei / (10**8)
                writer.writerow([validator, user, f"{tokens:.2f}"])


def analyze_user_token_ranges(csv_filename="delegations.csv"):
    """
    Analyze delegations.csv and print counts of users:
    - < 100 tokens
    - > 100 but < 10000
    - > 10000
    """
    import csv
    from collections import defaultdict

    user_totals = defaultdict(float)

    with open(csv_filename, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            user = row["User address"]
            try:
                tokens = float(row["Amount (tokens)"])
            except Exception:
                continue
            user_totals[user] += tokens

    count_lt_100 = 0
    count_100_10000 = 0
    count_gt_10000 = 0

    for total in user_totals.values():
        if total < 100:
            count_lt_100 += 1
        elif total < 10000:
            count_100_10000 += 1
        else:
            count_gt_10000 += 1

    print("User token distribution:")
    print(f"< 100 tokens: {count_lt_100}")
    print(f"> 100 but < 10000 tokens: {count_100_10000}")
    print(f"> 10000 tokens: {count_gt_10000}")


if __name__ == "__main__":
    start_time = time.time()
    try:
        validator_delegations = parse_delegations()
        # Print Nivo-compatible JSON structure
        nivo_data = delegations_to_nivo_json(validator_delegations)
        import json

        print(json.dumps(nivo_data, indent=2))
        # Optionally, also display delegations as before
        # display_delegations(validator_delegations)
        # Write delegations to CSV
        write_delegations_csv(validator_delegations)

    except Exception as e:
        print(f"Error: {str(e)}")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nScript execution time: {execution_time:.2f} seconds")

    print("\n--- User Token Range Analysis ---")
    analyze_user_token_ranges("delegations.csv")
