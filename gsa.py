from bluesky_parser import PartialBlueskyUser
from client import get_follows, get_posts_public_api, get_profile_public_api
from utils import load_bluesky_users
import json
import time
from tqdm import tqdm
from pathlib import Path
from collections import Counter
import statistics
import os
from multiprocessing import Pool
import anthropic
from concurrent.futures import ThreadPoolExecutor


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
        f.write("[\n")
        for i, account in enumerate(accounts_list):
            json_line = json.dumps(account)
            f.write(f'  {json_line}{"," if i < len(accounts_list)-1 else ""}\n')
        f.write("]\n")


def fetch_follows_of_seed_accounts(
    max_seeds=500, output_file="globally_significant_accounts.json"
):
    """
    Fetch follows of seed accounts from Bluesky, calculating SFC as we go

    Args:
        max_seeds: Maximum number of seed accounts to process
        output_file: File to save the results to

    Returns:
        Dictionary mapping handles to PartialBlueskyUser objects with SFC counts
    """
    # Load seed accounts (limit to max_seeds to avoid rate limiting)
    seed_accounts = load_bluesky_users(limit=max_seeds)
    print(f"Loaded {len(seed_accounts)} seed accounts")

    # Dict to store globally significant accounts, keyed by handle
    # Value is a dict of form: {"user": PartialBlueskyUser, "sfc": int}
    globally_significant_accounts = {}

    # Process each seed account
    for account in tqdm(seed_accounts, desc="Processing seed accounts"):
        handle = account["handle"].lstrip("@")

        # Skip accounts with a low follower:following ratio
        if account["following"] > 10000 or (
            account["following"] > 1000
            and account["followers"] / account["following"] < 1.5
        ):
            print(f"Skipping {handle} due to follow ratio")
            continue

        try:
            # Get follows of this account
            follows = get_follows(handle)
            print(f"Found {len(follows)} follows for @{handle}")

            # Process each followed account
            for profile in follows:
                follow_handle = profile["handle"].lstrip("@")

                if follow_handle not in globally_significant_accounts:
                    # First time seeing this account
                    follow_obj = PartialBlueskyUser(
                        name=profile["display_name"],
                        handle=follow_handle,
                        followers=None,
                        following=None,
                    )
                    globally_significant_accounts[follow_handle] = {
                        "user": follow_obj,
                        "sfc": 1,
                    }
                else:
                    # Increment SFC for this account
                    globally_significant_accounts[follow_handle]["sfc"] += 1

            # Sleep to avoid rate limiting
            time.sleep(0.1)

        except Exception as e:
            print(f"Error processing account @{handle}: {e}")

    # Convert to list format for JSON output
    output_data = []
    for handle, data in globally_significant_accounts.items():
        account_data = data["user"].to_dict()
        account_data["sfc"] = data["sfc"]
        output_data.append(account_data)

    # Sort by SFC (descending)
    output_data.sort(key=lambda x: x["sfc"], reverse=True)

    # Save to file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)

    print(f"\nStats:")
    print(f"Found {len(globally_significant_accounts)} globally significant accounts")
    print(f"Top 10 accounts by SFC:")
    for account in output_data[:10]:
        print(f"  @{account['handle']}: {account['sfc']} significant followers")
    print(f"\nSaved to {output_file}")

    return globally_significant_accounts


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


def analyze_sfc_stats(
    sfc_file="globally_significant_accounts.json",
    output_file="sfc_stats.json",
    min_sfc=5,
):
    """
    Analyze SFC statistics and apply a minimum SFC cutoff to select accounts

    Args:
        sfc_file: File containing accounts with SFC
        output_file: File to save the selected accounts to
        min_sfc: Minimum number of significant followers to be included

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

    # Select accounts above minimum SFC
    selected_accounts = [account for account in accounts if account["sfc"] >= min_sfc]

    # Sort by SFC (descending)
    selected_accounts.sort(key=lambda x: x["sfc"], reverse=True)

    # Add stats to the output
    output = {
        "stats": stats,
        "cutoff": {
            "min_sfc": min_sfc,
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
    print(f"  Minimum SFC: {min_sfc}")
    print(f"  Selected {len(selected_accounts)} accounts")
    print(f"Saved to {output_file}")

    return selected_accounts


def process_single_profile(account):
    """
    Process a single account to download its profile
    Used for parallel processing in download_user_profiles

    Args:
        account: Dictionary containing account info

    Returns:
        Dictionary containing profile data and SFC (if available)
    """
    handle = account["handle"].lstrip("@")
    try:
        # Get the profile
        profile_data = get_profile_public_api(handle)

        # Add SFC if available
        if "sfc" in account:
            profile_data["sfc"] = account["sfc"]

        return profile_data
    except Exception as e:
        print(f"Error downloading profile for @{handle}: {e}")
        return None


def download_user_profiles(
    accounts_file="sfc_stats.json",
    output_file="user_profiles.json",
    batch_size=50,
    num_workers=20,
):
    """
    Download full user profiles for selected accounts using parallel processing

    Args:
        accounts_file: File containing selected accounts
        output_file: File to save the user profiles to
        batch_size: Number of profiles to save in each batch
        num_workers: Number of parallel workers to use

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

    print(
        f"Downloading profiles for {len(accounts)} accounts using {num_workers} workers"
    )

    # Process accounts in parallel
    with Pool(processes=num_workers) as pool:
        profiles = []
        current_batch = []

        # Use imap_unordered for better performance with error handling
        for i, profile_data in enumerate(
            tqdm(
                pool.imap_unordered(process_single_profile, accounts),
                total=len(accounts),
                desc="Downloading profiles",
            )
        ):
            if profile_data is not None:
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


def gather_posts(
    profiles_file="user_profiles.json",
    output_file="user_profiles_with_posts.json",
    num_workers=10,
):
    """
    Gather posts from user profiles and add to existing profile data using parallel processing

    Args:
        profiles_file: Input JSON file with user profiles
        output_file: Output JSON file with profiles and their recent posts
        num_workers: Number of parallel workers
    """
    # Load existing profiles
    with open(profiles_file, "r", encoding="utf-8") as f:
        profiles = json.load(f)

    def process_profile(profile):
        try:
            # Fetch posts for the user
            posts = get_posts_public_api(profile["handle"])
            # Add posts to the profile object
            profile["recent_posts"] = posts
            return profile
        except Exception as e:
            print(f"Error fetching posts for {profile['handle']}: {e}")
            # Still return the profile even if post fetching fails
            return profile

    # Process profiles in parallel
    print(f"Processing {len(profiles)} profiles with {num_workers} workers...")
    with Pool(processes=num_workers) as pool:
        updated_profiles = list(
            tqdm(
                pool.imap(process_profile, profiles),
                total=len(profiles),
                desc="Fetching user posts",
            )
        )

    # Save updated profiles to a new JSON file, with each object on a single line
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i, profile in enumerate(updated_profiles):
            json_line = json.dumps(profile)
            f.write(f'  {json_line}{"," if i < len(updated_profiles)-1 else ""}\n')
        f.write("]\n")

    print(f"Saved profiles with posts to {output_file}")
    print(f"Total profiles processed: {len(updated_profiles)}")


def gather_unstructured_data(
    profiles_file="user_profiles_with_posts.json",
    output_file="user_profiles_with_unstructured_data.json",
    num_workers=10,
    batch_size=50,
):
    """
    Gather unstructured metadata using Claude 3.5 Haiku for each profile

    Args:
        profiles_file: Input JSON file with user profiles and posts
        output_file: Output JSON file with profiles and metadata
        num_workers: Number of parallel workers
        batch_size: Size of batches for API calls
    """
    # Load existing profiles
    with open(profiles_file, "r", encoding="utf-8") as f:
        profiles = json.load(f)

    # Initialize Anthropic client
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    # Claude prompt for metadata extraction
    CLAUDE_PROMPT = """
    You are a precise metadata extraction assistant for social media profiles. Your task is to parse the given account information and generate an array of structured JSON metadata profiles.

    Input will be a batch of social media account objects, each with a display name and description. Output a list of JSON objects with the following schema:

    {
      "identity": {
        "display_name": "string",
        "handle": "string",
        "pronouns": "optional_string",
        "self_description": "string"
      },
      "professional_tags": [
        "writer", "journalist", "comedian", "streamer", "podcaster", 
        "filmmaker", "content_creator"
      ],
      "interests": [
        "sports", "books", "gaming", "music", "technology", "politics", 
        "film", "comedy", "history", "transgender_rights", 
        "media_criticism", "pop_culture"
      ],
      "social_links": {
        "primary_platforms": ["twitch", "youtube", "patreon", "substack", "onlyfans"],
        "other_links": ["array_of_urls"]
      },
      "content_characteristics": {
        "nsfw": "boolean",
        "primary_themes": ["array_of_thematic_tags"]
      },
      "identity_markers": {
        "gender_identity": ["trans", "non_binary", "cis"],
        "location": "optional_string",
        "cultural_background": "optional_string"
      }
    }

    Guidelines:
    - Be comprehensive but concise
    - Use the predefined tags where possible
    - If no clear match exists, use the most appropriate general category
    - Infer context from writing style and self-description
    - Only include links that are explicitly mentioned in the profile

    Respond ONLY with the array of JSON objects. Do not include any additional text or explanation.
    """

    def process_batch(batch):
        results = []
        for profile in batch:
            try:
                # Extract display name and description
                display_name = profile.get("displayName", "")
                description = profile.get("description", "")
                handle = profile.get("handle", "")

                # Skip if no meaningful data
                if not display_name and not description:
                    profile["metadata"] = None
                    results.append(profile)
                    continue

                # Prepare input for Claude
                input_text = f"Display Name: {display_name}; Description: {description}; Handle: {handle}"

                # Call Claude API
                message = client.messages.create(
                    model="claude-3-haiku-20240307",
                    max_tokens=1000,
                    temperature=0,
                    system=CLAUDE_PROMPT,
                    messages=[{"role": "user", "content": input_text}],
                )

                # Parse the response
                try:
                    metadata = json.loads(message.content[0].text)
                    profile["metadata"] = metadata
                except json.JSONDecodeError:
                    print(f"Error parsing JSON for {handle}")
                    profile["metadata"] = None

                results.append(profile)

                # Rate limiting - sleep a small amount between requests
                time.sleep(0.05)

            except Exception as e:
                print(f"Error processing {profile.get('handle', 'unknown')}: {e}")
                profile["metadata"] = None
                results.append(profile)

        return results

    # Split profiles into batches
    profile_batches = [
        profiles[i : i + batch_size] for i in range(0, len(profiles), batch_size)
    ]

    # Process batches in parallel
    updated_profiles = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_batch, batch) for batch in profile_batches]

        # Collect results as they complete
        for future in tqdm(futures, desc="Processing batches"):
            batch_results = future.result()
            updated_profiles.extend(batch_results)

    # Save updated profiles to a new JSON file, with each object on a single line
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i, profile in enumerate(updated_profiles):
            json_line = json.dumps(profile)
            f.write(f'  {json_line}{"," if i < len(updated_profiles)-1 else ""}\n')
        f.write("]\n")

    print(f"Saved profiles with metadata to {output_file}")
    print(f"Total profiles processed: {len(updated_profiles)}")


def run_full_pipeline(max_seeds=500, min_sfc=5, batch_size=50):
    """
    Run the full pipeline: fetch follows, calculate SFC, analyze stats, download profiles

    Args:
        max_seeds: Maximum number of seed accounts to process
        min_sfc: Minimum number of significant followers to include
        batch_size: Batch size for saving data
    """
    print("=== Step 1: Fetching follows of seed accounts ===")
    fetch_follows_of_seed_accounts(max_seeds=max_seeds)

    print("\n=== Step 3: Analyzing SFC statistics ===")
    analyze_sfc_stats(min_sfc=min_sfc)

    print("\n=== Step 4: Downloading user profiles ===")
    download_user_profiles(batch_size=batch_size)

    print("\n=== Step 5: Gathering posts ===")
    gather_posts()

    print("\n=== Step 6: Gathering unstructured data ===")
    gather_unstructured_data()

    print("\n=== Pipeline complete! ===")


if __name__ == "__main__":
    gather_posts()
    gather_unstructured_data()
