import json
from typing import List, Dict, Any, Optional, Union
import os
import requests
import wikipedia
from models import WikipediaPage
from bs4 import BeautifulSoup
import re
from tqdm.contrib.concurrent import process_map


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


def get_wikipedia_summary(title: str) -> Dict[str, Any]:
    """
    Get a Wikipedia summary for a given title by parsing the HTML content.

    Args:
        title: The Wikipedia page title or the last part of the URL

    Returns:
        A structured summary of the Wikipedia page
    """
    # Format the title for URL (replace spaces with underscores)
    formatted_title = title.replace(" ", "_")
    url = f"https://en.wikipedia.org/wiki/{formatted_title}"

    try:
        # Get the page content
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        # Parse the HTML
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract the page title
        page_title = soup.find(id="firstHeading").text.strip()

        # Extract the summary (first paragraph of the content)
        content_div = soup.find(id="mw-content-text")

        # Find the first paragraph that has substantial content
        paragraphs = content_div.find_all("p")
        summary_text = ""
        for p in paragraphs:
            # Skip empty paragraphs or those with just whitespace/newlines
            if p.text.strip() and len(p.text.strip()) > 50:
                summary_text = p.text.strip()
                break

        # Clean up the summary text (remove citation brackets like [1], [2], etc.)
        summary_text = re.sub(r"\[\d+\]", "", summary_text)

        # Extract infobox data if available
        infobox = soup.find("table", class_="infobox")
        infobox_data = {}

        if infobox:
            rows = infobox.find_all("tr")
            for row in rows:
                header = row.find("th")
                data = row.find("td")
                if header and data:
                    key = header.text.strip()
                    value = data.text.strip()
                    # Clean up the value (remove citation brackets)
                    value = re.sub(r"\[\d+\]", "", value)
                    infobox_data[key] = value

        # Extract categories
        categories = []
        category_links = soup.select("#mw-normal-catlinks ul li a")
        for link in category_links:
            categories.append(link.text.strip())

        # Construct a structured summary
        result = {
            "title": page_title,
            "url": url,
            "summary": summary_text,
            "infobox": infobox_data,
            "categories": categories,
        }

        return result

    except Exception as e:
        return {
            "title": title,
            "url": url,
            "error": str(e),
            "summary": f"Failed to extract summary: {str(e)}",
            "infobox": {},
            "categories": [],
        }


def get_wikipedia_search_results(query: str, limit: int = 3) -> List[str]:
    """
    Get a Wikipedia search for a given query.
    """
    return wikipedia.search(query, results=limit)


def get_wikipedia_search_results_api(
    query: str, language: str = "en", limit: int = 3
) -> List[WikipediaPage]:
    """
    Get a Wikipedia search for a given query.
    """
    try:
        formatted_query = query.replace(" ", "_")
        headers = {
            "Authorization": f"Bearer {os.environ.get('WIKIMEDIA_API_KEY')}",
            "User-Agent": "filter [2.0]",
        }
        response = requests.get(
            f"https://api.wikimedia.org/core/v1/wikipedia/{language}/search/page?q={formatted_query}&limit={limit}"
        )
        response.raise_for_status()
        return [WikipediaPage.from_dict(page) for page in response.json()["pages"]]
    except Exception as e:
        print(f"Error getting Wikipedia search results: {e}")
        return []
