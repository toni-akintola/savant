from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import csv
import json


class BlueskyUser:
    def __init__(self, rank, name, handle, followers, following):
        self.rank = rank
        self.name = name
        self.handle = handle
        self.followers = followers
        self.following = following

    def __repr__(self):
        return f"#{self.rank}: {self.name} (@{self.handle}) - {self.followers} followers, {self.following} following"

    def to_dict(self):
        return {
            "rank": self.rank,
            "name": self.name,
            "handle": self.handle,
            "followers": self.followers,
            "following": self.following,
        }


def parse_bluesky_users(text):
    """
    Parse the text content from vqv.app to extract user information
    """
    users = []

    # Split the text into lines
    lines = text.strip().split("\n")

    i = 0
    while i < len(lines):
        # Look for a line that starts with a username (no # symbol)
        if i + 3 < len(lines) and not lines[i].startswith("#"):
            try:
                # The pattern appears to be:
                # Line 1: Username
                # Line 2: Handle
                # Line 3: Followers count
                # Line 4: Following count
                # Line 5: Rank (starts with #)

                name = lines[i].strip()
                handle = lines[i + 1].strip()
                followers = int(lines[i + 2].replace(",", ""))
                following = int(lines[i + 3].replace(",", ""))

                # Get the rank from the next line that starts with #
                rank_index = i + 4
                while rank_index < len(lines) and not lines[rank_index].startswith("#"):
                    rank_index += 1

                if rank_index < len(lines):
                    rank = int(lines[rank_index].replace("#", ""))

                    # Create a BlueskyUser object
                    user = BlueskyUser(rank, name, handle, followers, following)
                    users.append(user)

                    # Move to the next user (after the rank)
                    i = rank_index + 1
                    continue
            except (ValueError, IndexError):
                # If parsing fails, move to the next line
                pass

        i += 1

    return users


def scrape_bluesky_users():
    """
    Scrape the vqv.app website and parse the user data
    """
    url = "https://vqv.app/"

    # Configure Chrome options for headless operation
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    # Set up the Chrome driver
    try:
        # For Docker environment
        driver = webdriver.Chrome(options=chrome_options)
    except:
        # For local development with webdriver_manager
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        # Navigate to the URL
        driver.get(url)

        # Wait for the page to load
        time.sleep(5)

        # Get all text from the page
        page_text = driver.find_element("tag name", "body").text

        # Parse the user data
        users = parse_bluesky_users(page_text)

        print(f"Found {len(users)} users")

        # Print the first 10 users
        for user in users[:10]:
            print(user)

        # Save the data to CSV
        save_to_csv(users, "bluesky_top_users.csv")

        # Save the data to JSON
        save_to_json(users, "bluesky_top_users.json")

        return users

    finally:
        # Close the browser
        driver.quit()


def save_to_csv(users, filename):
    """Save the users data to a CSV file"""
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["rank", "name", "handle", "followers", "following"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for user in users:
            writer.writerow(user.to_dict())

    print(f"Saved data to {filename}")


def save_to_json(users, filename):
    """Save the users data to a JSON file"""
    with open(filename, "w", encoding="utf-8") as jsonfile:
        json.dump([user.to_dict() for user in users], jsonfile, indent=2)

    print(f"Saved data to {filename}")


if __name__ == "__main__":
    users = scrape_bluesky_users()
