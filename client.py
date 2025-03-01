from atproto import Client, client_utils
from dotenv import load_dotenv
import os
import time
import requests
from enum import Enum

load_dotenv()

BLUESKY_HANDLE = os.getenv("BLUESKY_HANDLE")
BLUESKY_PASSWORD = os.getenv("BLUESKY_PASSWORD")
PUBLIC_API_URL = "https://public.api.bsky.app/"


class FeedFilter(Enum):
    posts_with_replies = "posts_with_replies"
    posts_no_replies = "posts_no_replies"
    posts_with_media = "posts_with_media"
    posts_and_author_threads = "posts_and_author_threads"


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


def get_profile_public_api(handle: str) -> dict:
    """
    Get profile of an account using the public Bluesky API via requests

    Args:
        handle: The handle of the account to retrieve profile for

    Returns:
        A dictionary containing the profile information
    """
    url = f"{PUBLIC_API_URL}/xrpc/app.bsky.actor.getProfile?actor={handle}"
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad responses
    return response.json()


def get_profile_authenticated(handle: str) -> dict:
    """
    Get profile of an account using an authenticated Bluesky client

    Args:
        handle: The handle of the account to retrieve profile for

    Returns:
        A dictionary containing the profile information
    """
    client = get_client()
    return client.get_profile(handle)


def get_posts_public_api(
    handle: str, limit: int = 10, filter: FeedFilter = FeedFilter.posts_no_replies
) -> list[dict]:
    """
    Get all posts of an account using the public Bluesky API via requests
    """
    url = f"{PUBLIC_API_URL}/xrpc/app.bsky.feed.getAuthorFeed?actor={handle}&limit={limit}&filter={filter.value}"
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad responses
    return response.json()["feed"]
