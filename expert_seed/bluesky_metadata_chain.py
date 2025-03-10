import json
import os
import time
import logging
from typing import Dict, List, Any
import anthropic
from dotenv import load_dotenv
from models import PartialBlueskyUser, WikipediaPage
from utils import (
    get_wikipedia_search_results_api,
    get_wikipedia_summary,
    write_json_lines,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("bluesky_metadata.log"), logging.StreamHandler()],
)
logger = logging.getLogger("BlueskyMetadataChain")

# Load environment variables
load_dotenv()


# Initialize Anthropic client
anthropic_client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))


class BlueskyMetadataChain:
    def __init__(self, output_file: str = "bluesky_metadata_results.json"):
        """
        Initialize the Bluesky metadata chain.

        Args:
            output_file: Path to the output JSON file
        """
        self.output_file = output_file
        self.results = []
        self.wikipedia_matches = 0
        logger.info(f"Initialized BlueskyMetadataChain with output file: {output_file}")

    def verify_search_result(
        self, user: PartialBlueskyUser, page: WikipediaPage
    ) -> bool:
        """
        Use Claude to verify if a search result matches the Bluesky user with >95% confidence.

        Args:
            name: Display name of the user
            description: User's self-description on Bluesky
            result: A single search result from Brave

        Returns:
            Boolean indicating if the result matches the user with >95% confidence
        """
        # Extract relevant information from the search result

        logger.info(f"Verifying search result: {page.title}")

        prompt = f"""
        I need to verify if a search result is about the same person/entity as a Bluesky user profile.
        
        BLUESKY USER:
        Display name: {user.name}
        Handle: {user.handle}
        Self-description: {user.description}
        
        SEARCH RESULT TITLE: {page.title}
        SEARCH RESULT DESCRIPTION: {page.description}
        Based on this information, determine if we can be ABSOLUTELY CERTAIN that this search result refers to the same person as the Bluesky profile.
        THE NAMES OF THE PEOPLE MUST BE THE SAME.
       
        IF THE SEARCH RESULT IS NOT ABOUT A PERSON, respond with "NO".
        
        Respond with ONLY "YES" if you are ABSOLUTELY CERTAIN it's the same person, or "NO" if you are not that confident.
        """

        try:

            response = anthropic_client.messages.create(
                model="claude-3-7-sonnet-latest",
                max_tokens=1000,
                temperature=0,
                system="You are a verification system that determines if two sources of information refer to the same person. Respond with ONLY 'YES' if 100% confident of a match, or 'NO' otherwise.",
                messages=[{"role": "user", "content": prompt}],
            )

            # Check if response is affirmative
            result_matches = "YES" in response.content[0].text.strip().upper()
            logger.info(
                f"Verification result for {page.title}: {'MATCH' if result_matches else 'NO MATCH'}"
            )
            return result_matches
        except Exception as e:
            logger.error(f"Error verifying search result: {str(e)}")
            return False

    def extract_wikipedia_summary(self, title: str) -> Dict[str, Any]:
        """
        Extract and summarize content from a Wikipedia page, focusing on expertise and interests.

        Args:
            url: Wikipedia URL
            name: Name of the person

        Returns:
            Dictionary with summary and other extracted information as raw strings
        """
        return get_wikipedia_summary(title)

    def process_user(self, user: PartialBlueskyUser) -> Dict[str, Any]:
        """
        Process a single Bluesky user through the entire chain, focusing only on Wikipedia.

        Args:
            user: PartialBlueskyUser object

        Returns:
            Metadata object for the user
        """

        if not user.name or not user.description:
            logger.info(f"Skipping user: {user.handle} (no name)")
            return {user.handle: {"matched_results": []}}

        logger.info(f"Processing user: {user.name} (@{user.handle})")
        if user.description:
            logger.info(f"User description: {user.description}")

        logger.info("STEP 1: Creating Wikipedia-specific query")
        wikipedia_query = user.name
        logger.info(f"Wikipedia query: {wikipedia_query}")

        logger.info(f"STEP 2: Executing Wikipedia search with query: {wikipedia_query}")
        wikipedia_results: List[WikipediaPage] = get_wikipedia_search_results_api(
            query=wikipedia_query
        )

        logger.info("STEP 3: Verifying and processing Wikipedia results")
        matched_results = []

        for page in wikipedia_results:
            logger.info(f"Checking Wikipedia page: {page.title}")

            if self.verify_search_result(user, page):
                logger.info(f"Wikipedia page verified as a match: {page.title}")
                self.wikipedia_matches += 1
                # Extract and summarize Wikipedia content
                logger.info(f"Extracting Wikipedia content")
                wikipedia_data = self.extract_wikipedia_summary(page.title)

                matched_results.append(wikipedia_data)
                break
            else:
                logger.info(f"Wikipedia page not verified as a match: {page.title}")

        # Create the metadata object
        logger.info("STEP 5: Creating final metadata object")
        logger.info(f"Completed processing for user: {user.name} (@{user.handle})")
        result = user.to_dict()
        result["metadata"] = matched_results[0] if matched_results else {}

        return result

    def process_users(self, users: List[PartialBlueskyUser]) -> None:
        """
        Process multiple Bluesky users and save results to file.

        Args:
            users: List of PartialBlueskyUser objects
            descriptions: Dictionary mapping handles to descriptions
        """
        with open("final_profiles.jsonl", "a") as f:
            f.write("[\n")

        for user in users:
            logger.info(f"Processing {user.name} (@{user.handle})")

            start_time = time.time()
            self.process_user(user)
            end_time = time.time()
            logger.info(f"Time taken to process user: {end_time - start_time} seconds")

        with open("final_profiles.jsonl", "a") as f:
            f.write("]\n")

    def save_results(self) -> None:
        """Save the metadata results to a JSON file."""
        logger.info(
            f"Saving results for {len(self.results)} users to {self.output_file}"
        )
        try:
            write_json_lines(self.output_file, self.results)
            logger.info(f"Successfully saved metadata to {self.output_file}")
            logger.info(f"Wikipedia matches: {self.wikipedia_matches}")
        except Exception as e:
            logger.error(f"Error saving results to file: {str(e)}")


def main():
    with open("user_profiles.json", "r") as f:
        users = [
            PartialBlueskyUser(
                name=user.get("displayName"),
                handle=user.get("handle"),
                description=user.get("description"),
                followersCount=user.get("followersCount"),
                followsCount=user.get("followsCount"),
                postsCount=user.get("postsCount"),
                indexedAt=user.get("indexedAt"),
                createdAt=user.get("createdAt"),
                sfc=user.get("sfc"),
            )
            for user in json.load(f)
        ]
    logger.info("Starting BlueskyMetadataChain example")
    chain = BlueskyMetadataChain(output_file="final_profiles.jsonl")
    chain.process_users(users)


if __name__ == "__main__":
    main()
