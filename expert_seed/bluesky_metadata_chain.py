import json
import os
import random
import time
import logging
from typing import Dict, List, Any
import anthropic
from dotenv import load_dotenv

from tqdm.contrib.concurrent import thread_map
import threading
from datetime import datetime, timedelta

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


class TokenRateLimiter:
    """
    Manages token rate limiting for API calls to stay within usage limits.
    Implements a sliding window approach to track token usage over time.
    """

    def __init__(self, tokens_per_minute: int = 200000):
        """
        Initialize the token rate limiter.

        Args:
            tokens_per_minute: Maximum number of tokens allowed per minute
        """
        self.tokens_per_minute = tokens_per_minute
        self.usage_window = []  # List of (timestamp, token_count) tuples
        self.lock = threading.Lock()
        self.logger = logging.getLogger("TokenRateLimiter")
        self.logger.info(
            f"Initialized token rate limiter with {tokens_per_minute} tokens per minute limit"
        )

    def _clean_old_usage(self):
        """Remove usage data older than 1 minute from the current time."""
        now = datetime.now()
        one_minute_ago = now - timedelta(minutes=1)

        with self.lock:
            self.usage_window = [
                (ts, count) for ts, count in self.usage_window if ts > one_minute_ago
            ]

    def add_tokens(self, token_count: int):
        """
        Record token usage.

        Args:
            token_count: Number of tokens used
        """
        with self.lock:
            self.usage_window.append((datetime.now(), token_count))

    def get_current_usage(self) -> int:
        """
        Get the total token usage in the last minute.

        Returns:
            Total token count used in the last minute
        """
        self._clean_old_usage()

        with self.lock:
            return sum(count for _, count in self.usage_window)

    def wait_if_needed(self, planned_token_count: int) -> float:
        """
        Wait if adding the planned token count would exceed the rate limit.

        Args:
            planned_token_count: Number of tokens planned to be used

        Returns:
            Time waited in seconds
        """
        start_wait = time.time()
        wait_time = 0

        while True:
            current_usage = self.get_current_usage()
            remaining_tokens = self.tokens_per_minute - current_usage

            if planned_token_count <= remaining_tokens:
                if wait_time > 0:
                    self.logger.info(
                        f"Waited {wait_time:.2f}s for token rate limit. Current usage: {current_usage}/{self.tokens_per_minute}"
                    )
                return wait_time

            # Calculate how long to wait
            # Find the oldest usage entry that would free up enough tokens
            with self.lock:
                if not self.usage_window:
                    break

                tokens_to_free = planned_token_count - remaining_tokens
                cumulative_freed = 0
                wait_until = None

                for i, (ts, count) in enumerate(self.usage_window):
                    cumulative_freed += count
                    if cumulative_freed >= tokens_to_free:
                        # Wait until this entry expires (1 minute after its timestamp)
                        wait_until = ts + timedelta(minutes=1)
                        break

            if wait_until:
                now = datetime.now()
                if wait_until > now:
                    sleep_time = (wait_until - now).total_seconds()
                    self.logger.info(
                        f"Rate limit reached ({current_usage}/{self.tokens_per_minute} tokens used). Waiting {sleep_time:.2f}s"
                    )
                    time.sleep(
                        min(sleep_time, 5)
                    )  # Wait at most 5 seconds at a time to allow for rechecks
                    wait_time = time.time() - start_wait
                else:
                    # This should be freed already, but we'll recheck
                    self._clean_old_usage()
            else:
                # If we can't determine a specific wait time, wait a short time and recheck
                time.sleep(1)
                wait_time = time.time() - start_wait

        return wait_time

    def estimate_tokens(self, text: str) -> int:
        """
        Estimate the number of tokens in a text string.
        This is a simple approximation - Claude uses BPE tokenization which is more complex.

        Args:
            text: Text to estimate token count for

        Returns:
            Estimated token count
        """
        # Simple approximation: 1 token ≈ 4 characters for English text
        return len(text) // 4


class UserRateLimiter:
    """
    Manages user processing rate limiting to stay within hourly limits.
    Implements a sliding window approach to track user processing over time.
    """

    def __init__(self, users_per_hour: int = 5000):
        """
        Initialize the user rate limiter.

        Args:
            users_per_hour: Maximum number of users allowed to be processed per hour
        """
        self.users_per_hour = users_per_hour
        self.usage_window = []  # List of timestamps when users were processed
        self.lock = threading.Lock()
        self.logger = logging.getLogger("UserRateLimiter")
        self.logger.info(
            f"Initialized user rate limiter with {users_per_hour} users per hour limit"
        )

    def _clean_old_usage(self):
        """Remove usage data older than 1 hour from the current time."""
        now = datetime.now()
        one_hour_ago = now - timedelta(hours=1)

        with self.lock:
            self.usage_window = [ts for ts in self.usage_window if ts > one_hour_ago]

    def add_user(self):
        """Record a user being processed."""
        with self.lock:
            self.usage_window.append(datetime.now())

    def get_current_usage(self) -> int:
        """
        Get the total number of users processed in the last hour.

        Returns:
            Total users processed in the last hour
        """
        self._clean_old_usage()

        with self.lock:
            return len(self.usage_window)

    def wait_if_needed(self) -> float:
        """
        Wait if processing another user would exceed the rate limit.

        Returns:
            Time waited in seconds
        """
        start_wait = time.time()
        wait_time = 0

        while True:
            current_usage = self.get_current_usage()

            if current_usage < self.users_per_hour:
                if wait_time > 0:
                    self.logger.info(
                        f"Waited {wait_time:.2f}s for user rate limit. Current usage: {current_usage}/{self.users_per_hour}"
                    )
                return wait_time

            # If we're at the limit, we need to wait until the oldest entry expires
            with self.lock:
                if not self.usage_window:
                    break

                # Wait until the oldest entry expires (1 hour after its timestamp)
                wait_until = self.usage_window[0] + timedelta(hours=1)

            now = datetime.now()
            if wait_until > now:
                sleep_time = (wait_until - now).total_seconds()
                self.logger.info(
                    f"User rate limit reached ({current_usage}/{self.users_per_hour} users processed). Waiting {sleep_time:.2f}s"
                )
                time.sleep(
                    min(sleep_time, 30)
                )  # Wait at most 30 seconds at a time to allow for rechecks
                wait_time = time.time() - start_wait
            else:
                # This should be freed already, but we'll recheck
                self._clean_old_usage()

        return wait_time


class BlueskyMetadataChain:
    def __init__(self, output_file: str = "bluesky_metadata_results.json"):
        """
        Initialize the Bluesky metadata chain.

        Args:
            output_file: Path to the output JSON file
        """
        self.output_file = output_file
        self.results = []
        self.token_limiter = TokenRateLimiter(tokens_per_minute=200000)
        self.user_limiter = UserRateLimiter(users_per_hour=5000)
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

        # Estimate token count
        token_count = self.token_limiter.estimate_tokens(prompt)

        try:
            # Apply rate limiting
            logger.info(
                f"Verifying search result with Claude (est. {token_count} tokens)"
            )
            self.token_limiter.wait_if_needed(token_count)

            response = anthropic_client.messages.create(
                model="claude-3-7-sonnet-latest",
                max_tokens=1000,
                temperature=0,
                system="You are a verification system that determines if two sources of information refer to the same person. Respond with ONLY 'YES' if 100% confident of a match, or 'NO' otherwise.",
                messages=[{"role": "user", "content": prompt}],
            )

            # Record token usage
            self.token_limiter.add_tokens(token_count)

            # Check if response is affirmative
            print(response.content[0].text.strip().upper())
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
        # Apply user rate limiting
        self.user_limiter.wait_if_needed()
        self.user_limiter.add_user()

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
        metadata = matched_results[0] if matched_results else {}

        logger.info(f"Completed processing for user: {user.name} (@{user.handle})")
        result = user.to_dict()
        result["metadata"] = metadata
        self.results.append(result)
        return result

    def process_users(self, users: List[PartialBlueskyUser]) -> None:
        """
        Process multiple Bluesky users and save results to file.

        Args:
            users: List of PartialBlueskyUser objects
            descriptions: Dictionary mapping handles to descriptions
        """
        logger.info(
            f"Processing {len(users)} users with rate limit of {self.user_limiter.users_per_hour} users per hour"
        )

        for i, user in enumerate(users):
            logger.info(
                f"Processing user {i+1}/{len(users)}: {user.name} (@{user.handle})"
            )

            # Check user rate limit before processing
            current_usage = self.user_limiter.get_current_usage()
            logger.info(
                f"Current user processing rate: {current_usage}/{self.user_limiter.users_per_hour} users per hour"
            )

            user_metadata = self.process_user(user)

            # Add to results
            self.results.append(user_metadata)
            print(user_metadata)

            # Add delay to avoid rate limiting
            if i < len(users) - 1:  # Don't delay after the last user
                delay = 0.5
                logger.info(f"Waiting {delay} seconds before processing next user")
                time.sleep(delay)

        # Save results
        self.save_results()

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
    """Example usage of the BlueskyMetadataChain."""
    logger.info("Starting BlueskyMetadataChain example")
    with open("user_profiles.json", "r") as f:
        users = [
            PartialBlueskyUser(
                name=user.get("displayName"),
                handle=user["handle"],
                description=user["description"],
            )
            for user in json.load(f)
        ]

    chain = BlueskyMetadataChain(output_file="final_profiles.jsonl")

    # Use fewer workers to avoid overwhelming the rate limiter
    # Note: When using thread_map with multiple workers, each worker will independently
    # check the user rate limit, ensuring we don't exceed 5000 users per hour in total
    thread_map(chain.process_user, users[:1000], max_workers=10)

    chain.save_results()


if __name__ == "__main__":
    main()
