import requests
import json
import os
from dotenv import load_dotenv
import time

load_dotenv()


def search(query, max_retries=3, retry_delay=2, count=10, extra_snippets=True):
    """
    Perform a search using the Brave Search API.

    Args:
        query: The search query string
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
        count: Number of results to return
        extra_snippets: Whether to include extra snippets

    Returns:
        Tuple of (JSON response from the Brave Search API, rate limit information)
    """
    url = "https://api.search.brave.com/res/v1/web/search"
    headers = {
        "X-Subscription-Token": f"{os.getenv('BRAVE_SEARCH_API_KEY')}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                headers=headers,
                params={"q": query, "count": count, "extra_snippets": extra_snippets},
            )
            response.raise_for_status()  # Raise exception for 4XX/5XX responses

            # Extract rate limit information from headers
            rate_limit_info = {
                "limit": response.headers.get("X-RateLimit-Limit"),
                "remaining": response.headers.get("X-RateLimit-Remaining"),
                "reset": response.headers.get("X-RateLimit-Reset"),
            }

            # Parse and return the JSON response along with rate limit info
            return response.json(), rate_limit_info
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request failed: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"Request failed after {max_retries} attempts: {str(e)}")
                # Return empty dict to avoid breaking the calling code
                return {}, {}
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON response: {str(e)}")
            print(f"Response content: {response.text[:500]}...")
            return {}, {}
