import json
from typing import List, Dict, Any, Optional, Union
import os
import wikipediaapi


def load_bluesky_users(
    filepath: str = "bluesky_top_users.json", limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Load Bluesky users from a JSON file.

    Args:
        filepath: Path to the JSON file containing user data
        limit: Maximum number of users to return (None for all)

    Returns:
        List of user dictionaries with keys: rank, name, handle, followers, following

    Raises:
        FileNotFoundError: If the specified file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"User data file not found: {filepath}")

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            users = json.load(f)

        # Sort by rank to ensure proper ordering
        users = sorted(users, key=lambda x: x.get("rank", float("inf")))

        # Apply limit if specified
        if limit is not None:
            users = users[:limit]

        return users

    except json.JSONDecodeError:
        raise json.JSONDecodeError(f"Invalid JSON in file: {filepath}", "", 0)


def write_json_lines(filepath: str, data: Union[dict, List[dict]]):
    """
    Writes a single JSON object or a list of JSON objects to a file, ensuring each object is on a single line.

    Args:
        filepath (str): The path to the output file.
        data (Union[dict, List[dict]]): A JSON object or a list of JSON objects to write.
    """
    try:
        with open(filepath, "w") as f:
            if isinstance(data, list):
                f.write("[\n")
                for i, item in enumerate(data):
                    f.write(
                        "  "
                        + json.dumps(item)
                        + ("," if i < len(data) - 1 else "")
                        + "\n"
                    )
                f.write("]\n")
            else:
                f.write(json.dumps(data) + "\n")
        print(f"Successfully wrote JSON data to {filepath}")
    except Exception as e:
        print(f"Error writing to {filepath}: {str(e)}")


def get_top_users(count: int = 500) -> List[Dict[str, Any]]:
    """
    Get the top N Bluesky users by follower count.

    Args:
        count: Number of top users to return

    Returns:
        List of the top N users sorted by follower count
    """
    users = load_bluesky_users()

    # Sort by followers (descending)
    users = sorted(users, key=lambda x: x.get("followers", 0), reverse=True)

    return users[:count]


def get_user_by_handle(handle: str) -> Optional[Dict[str, Any]]:
    """
    Find a user by their handle.

    Args:
        handle: The user's handle (without the @ symbol)

    Returns:
        User dictionary if found, None otherwise
    """
    users = load_bluesky_users()

    # Normalize handle by removing @ if present
    handle = handle.lstrip("@")

    for user in users:
        if user.get("handle", "").lstrip("@") == handle:
            return user

    return None


def get_user_stats() -> Dict[str, Any]:
    """
    Get statistics about the loaded users.

    Returns:
        Dictionary with statistics like total users, average followers, etc.
    """
    users = load_bluesky_users()

    if not users:
        return {
            "total_users": 0,
            "avg_followers": 0,
            "avg_following": 0,
            "max_followers": 0,
            "min_followers": 0,
        }

    followers = [user.get("followers", 0) for user in users]
    following = [user.get("following", 0) for user in users]

    return {
        "total_users": len(users),
        "avg_followers": sum(followers) / len(followers),
        "avg_following": sum(following) / len(following),
        "max_followers": max(followers),
        "min_followers": min(followers),
        "median_followers": sorted(followers)[len(followers) // 2],
    }


def get_wikipedia_summary(name: str) -> str:
    """
    Get a summary of a Wikipedia page for a given name.

    Args:
        name: The name of the person to search for on Wikipedia

    Returns:
        A summary of the Wikipedia page for the given name
    """
    wiki_wiki = wikipediaapi.Wikipedia(user_agent="filter-bot", language="en")
    page = wiki_wiki.page(name)
    if page.exists():
        return page.summary
    else:
        return f"No Wikipedia page found for {name}"
