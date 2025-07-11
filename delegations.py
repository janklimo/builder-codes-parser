import json_stream
from typing import Any, Dict, List, Tuple
import time
from collections import defaultdict


def parse_delegations() -> Dict[str, List[Tuple[str, int]]]:
    """
    Parse delegations data from data.json file.
    Returns a dictionary where:
    - Key: validator address
    - Value: List of tuples (user_address, token_amount)
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

            return dict(validator_delegations)

    except FileNotFoundError:
        print(f"Error: data.json file not found.")
        raise
    except Exception as e:
        print(f"Error parsing JSON: {str(e)}")
        raise


def display_delegations(validator_delegations: Dict[str, List[Tuple[str, int]]]):
    """Display delegation information in a readable format"""

    # Count total delegations before filtering
    total_delegations = sum(
        len(delegations) for delegations in validator_delegations.values()
    )
    print(f"Total delegations: {total_delegations}")

    # Filter out delegations under 10 tokens (wei < 10^9)
    filtered_validator_delegations = {}
    for validator, delegations in validator_delegations.items():
        filtered_delegations = [
            (user, wei) for user, wei in delegations if wei >= 10**9
        ]
        if (
            filtered_delegations
        ):  # Only include validators that still have delegations after filtering
            filtered_validator_delegations[validator] = filtered_delegations

    # Count delegations after filtering
    filtered_delegations_count = sum(
        len(delegations) for delegations in filtered_validator_delegations.values()
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
    for validator, delegations in validator_delegations.items():
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
        delegations = validator_delegations[validator]
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


if __name__ == "__main__":
    start_time = time.time()
    try:
        validator_delegations = parse_delegations()
        display_delegations(validator_delegations)

    except Exception as e:
        print(f"Error: {str(e)}")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nScript execution time: {execution_time:.2f} seconds")
