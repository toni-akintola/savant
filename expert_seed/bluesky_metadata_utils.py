import json
import os
from typing import Dict, List, Optional

from models import PartialBlueskyUser


def extract_descriptions_from_profiles(profiles_json_path: str) -> Dict[str, str]:
    """
    Extract descriptions from Bluesky user profiles.

    Args:
        profiles_json_path: Path to JSON file containing Bluesky user profiles

    Returns:
        Dictionary mapping user handles to their descriptions
    """
    if not os.path.exists(profiles_json_path):
        print(f"File not found: {profiles_json_path}")
        return {}

    try:
        with open(profiles_json_path, "r") as f:
            profiles_data = json.load(f)
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {profiles_json_path}")
        return {}

    descriptions = {}

    # Process the profiles data based on its structure
    # This implementation assumes a specific structure - adjust as needed
    for profile in profiles_data:
        if isinstance(profile, dict):
            # Try to extract handle and description
            handle = None
            description = None

            # Look for handle - could be under different keys
            if "handle" in profile:
                handle = profile["handle"]
            elif "did" in profile:
                handle = profile["did"]

            # Look for description - could be under different keys
            if "description" in profile:
                description = profile["description"]
            elif "bio" in profile:
                description = profile["bio"]

            # If we found both, add to our dictionary
            if handle and description:
                descriptions[handle] = description

    print(f"Extracted descriptions for {len(descriptions)} users")
    return descriptions


def load_users_from_json(users_json_path: str) -> List[PartialBlueskyUser]:
    """
    Load Bluesky users from a JSON file.

    Args:
        users_json_path: Path to JSON file containing user data

    Returns:
        List of PartialBlueskyUser objects
    """
    if not os.path.exists(users_json_path):
        print(f"File not found: {users_json_path}")
        return []

    try:
        with open(users_json_path, "r") as f:
            users_data = json.load(f)
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {users_json_path}")
        return []

    users = []

    # Process the users data based on its structure
    for user_data in users_data:
        if isinstance(user_data, dict):
            # Extract required fields
            name = user_data.get("name")
            handle = user_data.get("handle")

            # Skip if required fields are missing
            if not name or not handle:
                continue

            # Create a new PartialBlueskyUser
            user = PartialBlueskyUser(
                name=name,
                handle=handle,
                followers=user_data.get("followers"),
                following=user_data.get("following"),
                rank=user_data.get("rank"),
            )

            users.append(user)

    print(f"Loaded {len(users)} Bluesky users")
    return users


def save_metadata_results(metadata: List[Dict], output_file: str) -> None:
    """
    Save metadata results to a JSON file.

    Args:
        metadata: List of metadata objects
        output_file: Path to output JSON file
    """
    with open(output_file, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Saved metadata to {output_file}")
