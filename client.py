from atproto import Client, client_utils
from dotenv import load_dotenv
import os

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


def get_friends_of_account(account: str) -> list[str]:
    """
    Get friends of an account
    """
    client = get_client()
    friends = client.get_profile(account).data.followers
    return friends


if __name__ == "__main__":
    pass
