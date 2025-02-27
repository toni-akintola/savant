from atproto import Client, client_utils
from dotenv import load_dotenv
import os
import time

load_dotenv()

BLUESKY_HANDLE = os.getenv("BLUESKY_HANDLE")
BLUESKY_PASSWORD = os.getenv("BLUESKY_PASSWORD")
print(BLUESKY_HANDLE, BLUESKY_PASSWORD)


def get_client() -> Client:
    """
    Get a Bluesky client
    """
    client = Client()
    client.login(BLUESKY_HANDLE, BLUESKY_PASSWORD)

    return client


def get_follows(handle: str, delay: float = 0.1) -> list[dict]:
    """
    Get all follows of an account, handling pagination

    Args:
        handle: The handle of the account to get follows for
        delay: Time to wait between pagination requests (default 0.1s = 10 requests/sec)

    Returns:
        List of follow objects containing display_name and handle
    """
    client = get_client()
    all_follows = []
    cursor = None

    while True:
        try:
            # Get the next page of follows
            response = client.get_follows(handle, cursor=cursor)

            # Add the follows from this page to our list
            all_follows.extend(response.follows)

            # Update the cursor
            cursor = response.cursor

            # If there's no cursor, we've reached the end
            if not cursor:
                break

            # Sleep between requests to stay under rate limit
            time.sleep(delay)  # 10 requests/second = 600 requests/minute

        except Exception as e:
            print(f"Error fetching follows for @{handle} (cursor: {cursor}): {e}")
            break

    return all_follows


def get_profile(handle: str) -> dict:
    """
    Get profile of an account
    """
    client = get_client()
    response = client.get_profile(handle)
    return response
