from bluesky_parser import PartialBlueskyUser
from client import get_follows, get_profile
from utils import load_bluesky_users
import json
import time
from tqdm import tqdm
from pathlib import Path
import multiprocessing
from collections import Counter
import statistics
import os
from concurrent.futures import ProcessPoolExecutor, as_completed


def save_batch(accounts, output_file, mode="w"):
    """
    Save a batch of accounts to the JSON file

    Args:
        accounts: Set of PartialBlueskyUser objects to save
        output_file: Path to the output file
        mode: Write mode ('w' for write, 'a' for append)
    """
    # Convert set to list for JSON serialization
    accounts_list = [account.to_dict() for account in accounts]

    if mode == "a" and Path(output_file).exists():
        # Read existing data
        with open(output_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
        accounts_list = existing_data + accounts_list

    # Write to file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(accounts_list, f, indent=2)


def fetch_follows_of_seed_accounts(
    max_seeds=500, output_file="globally_significant_accounts.json", batch_size=50
):
    """
    Fetch follows of seed accounts from Bluesky, saving in batches

    Args:
        max_seeds: Maximum number of seed accounts to process
        output_file: File to save the results to
        batch_size: Number of accounts to process before saving

    Returns:
        Set of globally significant accounts
    """
    # Load seed accounts (limit to max_seeds to avoid rate limiting)
    seed_accounts = load_bluesky_users(limit=max_seeds)
    print(f"Loaded {len(seed_accounts)} seed accounts")

    # Set to store globally significant accounts
    globally_significant_handles = set()
    current_batch = set()

    # Process each seed account
    for i, account in enumerate(tqdm(seed_accounts, desc="Processing seed accounts")):
        handle = account["handle"].lstrip("@")
        try:
            # Get follows of this account
            follows = get_follows(handle)

            # Add each follow to the sets
            for follow in follows:
                follow_obj = PartialBlueskyUser(
                    name=follow["display_name"],
                    handle=follow["handle"].lstrip("@"),
                    followers=None,  # We don't have this info yet
                    following=None,  # We don't have this info yet
                )
                globally_significant_handles.add(follow_obj)
                current_batch.add(follow_obj)

            print(f"Added {len(follows)} follows from @{handle}")

            # Save batch if we've reached batch_size
            if len(current_batch) >= batch_size:
                print(f"\nSaving batch of {len(current_batch)} accounts...")
                save_batch(current_batch, output_file, "a" if i > batch_size else "w")
                current_batch = set()

            # Sleep to avoid rate limiting
            time.sleep(0.2)

        except Exception as e:
            print(f"Error processing account @{handle}: {e}")

    # Save any remaining accounts in the final batch
    if current_batch:
        print(f"\nSaving final batch of {len(current_batch)} accounts...")
        save_batch(current_batch, output_file, "a")

    print(f"Found {len(globally_significant_handles)} globally significant accounts")
    print(f"Saved to {output_file}")

    return globally_significant_handles


def process_seed_account(seed_account):
    """
    Process a single seed account to get its follows
    Used for parallel processing in calculate_sfc

    Args:
        seed_account: Dictionary containing seed account info

    Returns:
        List of handles that this seed account follows
    """
    handle = seed_account["handle"].lstrip("@")
    try:
        follows = get_follows(handle)
        return [follow["handle"].lstrip("@") for follow in follows]
    except Exception as e:
        print(f"Error processing account @{handle}: {e}")
        return []


def calculate_sfc(
    seed_accounts_file="bluesky_top_users.json",
    gsa_file="globally_significant_accounts.json",
    output_file="accounts_with_sfc.json",
    max_workers=None,
):
    """
    Calculate Significant Followers Count (SFC) for each account

    Args:
        seed_accounts_file: File containing the seed accounts
        gsa_file: File containing globally significant accounts
        output_file: File to save the results to
        max_workers: Maximum number of worker processes (None = use CPU count)

    Returns:
        Dictionary mapping handles to their SFC
    """
    # Load seed accounts
    seed_accounts = load_bluesky_users(filepath=seed_accounts_file)
    print(f"Loaded {len(seed_accounts)} seed accounts")

    # Load globally significant accounts
    with open(gsa_file, "r", encoding="utf-8") as f:
        gsa_accounts = json.load(f)
    print(f"Loaded {len(gsa_accounts)} globally significant accounts")

    # Extract all handles from globally significant accounts
    gsa_handles = set(account["handle"].lstrip("@") for account in gsa_accounts)

    # Initialize counter for SFC
    sfc_counter = Counter()

    # Use multiprocessing to process seed accounts in parallel
    max_workers = max_workers or multiprocessing.cpu_count()
    print(f"Using {max_workers} worker processes")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks
        future_to_account = {
            executor.submit(process_seed_account, account): account
            for account in seed_accounts
        }

        # Process results as they complete
        for future in tqdm(
            as_completed(future_to_account),
            total=len(future_to_account),
            desc="Processing seed accounts",
        ):
            account = future_to_account[future]
            try:
                follows = future.result()
                # Increment SFC for each followed account that's in our GSA set
                for handle in follows:
                    handle = handle.lstrip("@")
                    if handle in gsa_handles:
                        sfc_counter[handle] += 1
            except Exception as e:
                print(f"Error processing account {account['handle']}: {e}")

    # Create a list of accounts with their SFC
    accounts_with_sfc = []
    for account in gsa_accounts:
        handle = account["handle"].lstrip("@")
        sfc = sfc_counter.get(handle, 0)
        account_with_sfc = account.copy()
        account_with_sfc["sfc"] = sfc
        accounts_with_sfc.append(account_with_sfc)

    # Save to file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(accounts_with_sfc, f, indent=2)

    print(f"Calculated SFC for {len(accounts_with_sfc)} accounts")
    print(f"Saved to {output_file}")

    return sfc_counter


def analyze_sfc_stats(
    sfc_file="accounts_with_sfc.json",
    output_file="sfc_stats.json",
    cutoff_percentile=90,
):
    """
    Analyze SFC statistics and apply a cutoff to select the best accounts

    Args:
        sfc_file: File containing accounts with SFC
        output_file: File to save the selected accounts to
        cutoff_percentile: Percentile to use as cutoff (e.g., 90 means top 10%)

    Returns:
        List of selected accounts
    """
    # Load accounts with SFC
    with open(sfc_file, "r", encoding="utf-8") as f:
        accounts = json.load(f)

    # Extract SFC values
    sfc_values = [account["sfc"] for account in accounts]

    # Calculate statistics
    stats = {
        "count": len(sfc_values),
        "min": min(sfc_values),
        "max": max(sfc_values),
        "mean": statistics.mean(sfc_values),
        "median": statistics.median(sfc_values),
        "stdev": statistics.stdev(sfc_values) if len(sfc_values) > 1 else 0,
    }

    # Calculate percentiles
    percentiles = {}
    for p in [10, 25, 50, 75, 90, 95, 99]:
        percentiles[f"p{p}"] = (
            statistics.quantiles(sfc_values, n=100)[p - 1]
            if len(sfc_values) >= 100
            else None
        )

    # Combine stats and percentiles
    stats["percentiles"] = percentiles

    # Calculate cutoff value based on percentile
    if cutoff_percentile < 100:
        cutoff = (
            statistics.quantiles(sfc_values, n=100)[cutoff_percentile - 1]
            if len(sfc_values) >= 100
            else 0
        )
    else:
        cutoff = 0  # No cutoff

    # Select accounts above cutoff
    selected_accounts = [account for account in accounts if account["sfc"] >= cutoff]

    # Sort by SFC (descending)
    selected_accounts.sort(key=lambda x: x["sfc"], reverse=True)

    # Add stats to the output
    output = {
        "stats": stats,
        "cutoff": {
            "percentile": cutoff_percentile,
            "value": cutoff,
            "selected_count": len(selected_accounts),
        },
        "selected_accounts": selected_accounts,
    }

    # Save to file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"SFC Statistics:")
    print(f"  Count: {stats['count']}")
    print(f"  Min: {stats['min']}")
    print(f"  Max: {stats['max']}")
    print(f"  Mean: {stats['mean']:.2f}")
    print(f"  Median: {stats['median']}")
    print(f"  Cutoff ({cutoff_percentile}th percentile): {cutoff}")
    print(f"  Selected {len(selected_accounts)} accounts")
    print(f"Saved to {output_file}")

    return selected_accounts


def download_user_profiles(
    accounts_file="sfc_stats.json",
    output_file="user_profiles.json",
    batch_size=50,
    delay=0.5,
):
    """
    Download full user profiles for selected accounts

    Args:
        accounts_file: File containing selected accounts
        output_file: File to save the user profiles to
        batch_size: Number of profiles to save in each batch
        delay: Time to wait between API requests

    Returns:
        List of user profiles
    """
    # Load selected accounts
    with open(accounts_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    if "selected_accounts" in data:
        accounts = data["selected_accounts"]
    else:
        accounts = data  # Assume the file contains a list of accounts

    print(f"Downloading profiles for {len(accounts)} accounts")

    # List to store profiles
    profiles = []
    current_batch = []

    # Process each account
    for i, account in enumerate(tqdm(accounts, desc="Downloading profiles")):
        handle = account["handle"].lstrip("@")
        try:
            # Get the profile
            profile_data = get_profile(handle)

            # Add SFC if available
            if "sfc" in account:
                profile_data["sfc"] = account["sfc"]

            # Add to lists
            profiles.append(profile_data)
            current_batch.append(profile_data)

            # Save batch if we've reached batch_size
            if len(current_batch) >= batch_size:
                print(f"\nSaving batch of {len(current_batch)} profiles...")

                # Save to file (append mode if not the first batch)
                mode = "w" if i < batch_size else "a"
                save_profiles_batch(current_batch, output_file, mode)

                # Clear the batch
                current_batch = []

            # Sleep to avoid rate limiting
            time.sleep(delay)

        except Exception as e:
            print(f"Error downloading profile for @{handle}: {e}")

    # Save any remaining profiles in the final batch
    if current_batch:
        print(f"\nSaving final batch of {len(current_batch)} profiles...")
        save_profiles_batch(
            current_batch, output_file, "a" if len(profiles) > batch_size else "w"
        )

    print(f"Downloaded {len(profiles)} profiles")
    print(f"Saved to {output_file}")

    return profiles


def save_profiles_batch(profiles, output_file, mode="w"):
    """
    Save a batch of profiles to the JSON file

    Args:
        profiles: List of profile objects to save
        output_file: Path to the output file
        mode: Write mode ('w' for write, 'a' for append)
    """
    if mode == "a" and os.path.exists(output_file):
        # Read existing data
        with open(output_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
        profiles = existing_data + profiles

    # Write to file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(profiles, f, indent=2)


def run_full_pipeline(max_seeds=500, cutoff_percentile=90, batch_size=50):
    """
    Run the full pipeline: fetch follows, calculate SFC, analyze stats, download profiles

    Args:
        max_seeds: Maximum number of seed accounts to process
        cutoff_percentile: Percentile to use as cutoff
        batch_size: Batch size for saving data
    """
    print("=== Step 1: Fetching follows of seed accounts ===")
    fetch_follows_of_seed_accounts(max_seeds=max_seeds, batch_size=batch_size)

    print("\n=== Step 2: Calculating SFC ===")
    calculate_sfc()

    print("\n=== Step 3: Analyzing SFC statistics ===")
    analyze_sfc_stats(cutoff_percentile=cutoff_percentile)

    print("\n=== Step 4: Downloading user profiles ===")
    download_user_profiles(batch_size=batch_size)

    print("\n=== Pipeline complete! ===")


if __name__ == "__main__":
    # Uncomment the function you want to run
    # fetch_follows_of_seed_accounts()
    # calculate_sfc()
    # analyze_sfc_stats(cutoff_percentile=90)
    # download_user_profiles()

    # Or run the full pipeline
    # run_full_pipeline(max_seeds=500, cutoff_percentile=90, batch_size=50)

    # Default: just fetch follows
    fetch_follows_of_seed_accounts()
