import json
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.database import Database
import ijson

load_dotenv()
MONGODB_URI = "mongodb+srv://takintola:ghyJc5M0aiAFXohV@cluster0.l7asyls.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"


def get_database() -> Database:
    client = MongoClient(MONGODB_URI, server_api=ServerApi("1"))
    return client["Filter"]


def read_profiles(path: str) -> list[dict]:
    profiles = []
    with open(path, "r") as f:
        objects = ijson.items(f, "item")
        print(len(objects))
        for object in objects:
            print(object["handle"])
    return profiles


if __name__ == "__main__":
    # Create a new client and connect to the server
    db = get_database()
    expert_seed_collection = db["expert_seed"]
    profiles = read_profiles(
        "/home/ubuntu/data-science/data/expert-seed/final_profiles.json"
    )
    # expert_seed_collection.insert_many(profiles)
