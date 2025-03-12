import json
from utils import write_json_lines

if __name__ == "__main__":
    users = {}
    with open(
        "/home/ubuntu/data-science/data/expert-seed/user_profiles_with_posts.json", "r"
    ) as f:
        users_with_posts = json.load(f)
    for user in users_with_posts:
        users[user["handle"]] = user
    with open(
        "/home/ubuntu/data-science/data/expert-seed/user_profiles_with_metadata.json",
        "r",
    ) as f:
        users_with_metadata = json.load(f)

    wikipedia_matches = 0
    for user in users_with_metadata:
        if user["metadata"]:
            wikipedia_matches += 1
        users[user["handle"]]["metadata"] = user["metadata"]

    print(f"Wikipedia matches: {wikipedia_matches}")
    print(f"Total users: {len(users)}")
    write_json_lines(
        "/home/ubuntu/data-science/data/expert-seed/final_profiles.json", users
    )
