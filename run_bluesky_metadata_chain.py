#!/usr/bin/env python
import argparse
import os
from dotenv import load_dotenv

from bluesky_metadata_chain import BlueskyMetadataChain
from bluesky_metadata_utils import (
    extract_descriptions_from_profiles,
    load_users_from_json,
)

# Load environment variables
load_dotenv()


def main():
    """
    Main function to run the Bluesky metadata LLM chain.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run the Bluesky metadata LLM chain")
    parser.add_argument(
        "--users",
        type=str,
        required=True,
        help="Path to JSON file containing Bluesky users",
    )
    parser.add_argument(
        "--profiles",
        type=str,
        help="Path to JSON file containing Bluesky user profiles with descriptions",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="bluesky_metadata_results.json",
        help="Path to output JSON file",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of users to process in each batch",
    )
    parser.add_argument(
        "--limit", type=int, help="Limit the number of users to process"
    )

    args = parser.parse_args()

    # Check if ANTHROPIC_API_KEY is set
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("Error: ANTHROPIC_API_KEY environment variable is not set")
        print("Please set the ANTHROPIC_API_KEY in your .env file or environment")
        return

    # Check if BRAVE_SEARCH_API_KEY is set
    if not os.environ.get("BRAVE_SEARCH_API_KEY"):
        print("Error: BRAVE_SEARCH_API_KEY environment variable is not set")
        print("Please set the BRAVE_SEARCH_API_KEY in your .env file or environment")
        return

    # Load users
    users = load_users_from_json(args.users)
    if not users:
        print("No users to process. Exiting.")
        return

    # Apply limit if specified
    if args.limit and args.limit > 0:
        users = users[: args.limit]
        print(f"Limited to processing {len(users)} users")

    # Load descriptions if provided
    descriptions = {}
    if args.profiles:
        descriptions = extract_descriptions_from_profiles(args.profiles)

    # Initialize the chain
    chain = BlueskyMetadataChain(output_file=args.output)

    # Process users in batches
    total_users = len(users)
    batch_size = min(args.batch_size, total_users)

    for i in range(0, total_users, batch_size):
        batch_end = min(i + batch_size, total_users)
        current_batch = users[i:batch_end]

        print(
            f"Processing batch {i//batch_size + 1}: users {i+1} to {batch_end} of {total_users}"
        )

        # Process the current batch
        chain.process_users(current_batch, descriptions)

        # Save progress
        print(f"Batch complete. Progress: {batch_end}/{total_users} users processed.")

    print(f"All done! Processed {total_users} users. Results saved to {args.output}")


if __name__ == "__main__":
    main()
