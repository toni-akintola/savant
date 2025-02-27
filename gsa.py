from utils import load_bluesky_users


def fetch_friends_of_seed_accounts():
    """
    Fetch friends of seed accounts from Bluesky
    """
    seed_accounts = load_bluesky_users()
    globally_significant_accounts = set()
    for account in seed_accounts:
        friends = get_friends_of_account(account)
        globally_significant_accounts.add(friends)
    return globally_significant_accounts


if __name__ == "__main__":
    fetch_friends_of_seed_accounts()
