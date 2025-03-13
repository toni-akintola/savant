import json
from client import get_posts_public_api
from utils import write_json_lines
from tqdm.contrib.concurrent import thread_map


def get_posts_worker(user):
    user["posts"] = get_posts_public_api(user["handle"])
    return user


if __name__ == "__main__":
    with open(
        "/home/ubuntu/data-science/data/expert-seed/user_profiles.json", "r"
    ) as f:
        users = json.load(f)

    results = thread_map(
        get_posts_worker,
        users,
        max_workers=10,
    )

    print(results[0])
    write_json_lines(
        "/home/ubuntu/data-science/data/expert-seed/user_profiles_with_posts.json",
        results,
    )
