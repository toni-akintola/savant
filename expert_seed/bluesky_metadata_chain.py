import json
import os
import random
import time
import logging
from typing import Dict, List, Any
import anthropic
from dotenv import load_dotenv
import requests
from tqdm.contrib.concurrent import thread_map
import threading
from datetime import datetime, timedelta

from client import get_client
from models import PartialBlueskyUser
from brave_search import search
from utils import get_wikipedia_summary, write_json_lines

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
        logger.info(f"Initialized BlueskyMetadataChain with output file: {output_file}")

    def create_search_query(self, name: str, description: str = "") -> str:
        """
        Use Claude to create a specific search query for a Bluesky user.

        Args:
            name: Display name of the user
            description: User's self-description on Bluesky

        Returns:
            A specific search query optimized for Brave
        """
        logger.info(f"Creating search query for: {name}")
        if description:
            logger.info(f"Description: {description}")

        prompt = f"""
        I need to search for more information about a specific person on Bluesky.
        
        Display name: {name}
        Self-description: {description}
        
        Please create a very specific search query I can use on Brave Search to find information about this person.
        The query should be optimized to find:
        1. Their personal or professional websites
        2. Social media profiles
        3. Articles written by or about them
        4. Any notable achievements or affiliations
        
        Return ONLY the search query text, nothing else.
        """

        logger.debug(f"Sending prompt to Claude for search query generation")
        try:
            response = anthropic_client.messages.create(
                model="claude-3-7-sonnet-latest",
                max_tokens=150,
                temperature=0,
                system="You are an assistant that creates specific search queries to find information about people online. Return only the search query, no explanations.",
                messages=[{"role": "user", "content": prompt}],
            )

            # Extract just the query text
            query = response.content[0].text.strip()
            logger.info(f"Generated query for {name}: {query}")
            return query
        except Exception as e:
            logger.error(f"Error generating search query: {str(e)}")
            # Fallback to a simple query
            fallback_query = f"{name} {description}".strip()
            logger.info(f"Using fallback query: {fallback_query}")
            return fallback_query

    def verify_search_result(
        self, name: str, description: str, result: Dict[str, Any]
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
        title = result.get("title", "")
        result_description = result.get("description", "")
        url = result.get("url", "")

        logger.info(f"Verifying search result: {url}")
        logger.debug(f"Result title: {title}")
        logger.debug(f"Result description: {result_description[:100]}...")

        # For Wikipedia articles, perform a more rigorous name check
        if "wikipedia.org/wiki/" in url:
            # Extract the article title from the URL
            article_title = url.split("/wiki/")[-1].replace("_", " ")
            article_title = article_title.split("#")[0]  # Remove any section anchors

            # Decode URL encoding
            import urllib.parse

            article_title = urllib.parse.unquote(article_title)

            logger.info(f"Wikipedia article title: {article_title}")

            # Check if the name appears in the article title
            name_parts = name.lower().split()
            article_parts = article_title.lower().split()

            # Check if all parts of the name appear in the article title
            name_match = all(part in article_title.lower() for part in name_parts)

            if not name_match:
                logger.info(
                    f"Wikipedia article title does not match user name. Article: '{article_title}', Name: '{name}'"
                )
                return False

            logger.info(f"Wikipedia article title matches user name: {name_match}")

        prompt = f"""
        I need to verify if a search result is about the same person as a Bluesky user profile.
        
        BLUESKY USER:
        Display name: {name}
        Self-description: {description}
        
        SEARCH RESULT:
        Title: {title}
        Description: {result_description}
        URL: {url}
        
        Based on this information, determine if we can be MORE THAN 100% confident that this search result refers to the same person as the Bluesky profile.
        
        Consider name matches, profession/interests alignment, and any other identifying information.
        If the search result is a Wikipedia article, make sure the article is about the person, not just a generic article about the topic.
        If the article is about a topic and not the person, or if the Bluesky user's description is not robust enough to make a determination, respond with "NO".
        
        Respond with ONLY "YES" if you are 100% confident it's the same person, or "NO" if you are not that confident.
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
                max_tokens=5,
                temperature=0,
                system="You are a verification system that determines if two sources of information refer to the same person. Respond with ONLY 'YES' if 100% confident of a match, or 'NO' otherwise.",
                messages=[{"role": "user", "content": prompt}],
            )

            # Record token usage
            self.token_limiter.add_tokens(token_count)

            # Check if response is affirmative
            result_matches = response.content[0].text.strip().upper() == "YES"
            logger.info(
                f"Verification result for {url}: {'MATCH' if result_matches else 'NO MATCH'}"
            )
            return result_matches
        except Exception as e:
            logger.error(f"Error verifying search result: {str(e)}")
            return False

    def extract_wikipedia_summary(self, url: str, name: str) -> str:
        """
        Extract and summarize content from a Wikipedia page, focusing on expertise and interests.

        Args:
            url: Wikipedia URL
            name: Name of the person

        Returns:
            Dictionary with summary and other extracted information as raw strings
        """
        return get_wikipedia_summary(name)

    def process_user(self, user: PartialBlueskyUser) -> Dict[str, Any]:
        """
        Process a single Bluesky user through the entire chain, focusing only on Wikipedia.

        Args:
            user: PartialBlueskyUser object

        Returns:
            Metadata object for the user
        """
        logger.info(f"Processing user: {user.name} (@{user.handle})")
        if user.description:
            logger.info(f"User description: {user.description}")
        else:
            logger.info("No description available for this user")
            # Skip the search entirely when no description is provided
            logger.info("Skipping search process as no description is available")
            return {user.handle: {"matched_results": []}}

        logger.info("STEP 1: Creating Wikipedia-specific query")
        wikipedia_query = f"{user.name}+wikipedia"
        logger.info(f"Wikipedia query: {wikipedia_query}")

        logger.info(f"Executing Wikipedia search with query: {wikipedia_query}")
        wikipedia_results, _ = search(wikipedia_query, count=3)

        logger.info("STEP 2: Extracting and processing Wikipedia results")
        web_results = []

        # Process search results
        if "web" in wikipedia_results and "results" in wikipedia_results["web"]:
            web_count = len(wikipedia_results["web"]["results"])
            logger.info(f"Found {web_count} web results in Wikipedia search")
            web_results.extend(wikipedia_results["web"]["results"])

        # Mixed results
        if "mixed" in wikipedia_results:
            mixed_results = wikipedia_results.get("mixed", {}).get("results", [])
            mixed_count = len(mixed_results)
            logger.info(f"Found {mixed_count} mixed results in Wikipedia search")

            web_from_mixed = 0
            for item in mixed_results:
                if item.get("type") == "web" and "content" in item:
                    web_results.append(item["content"])
                    web_from_mixed += 1

            logger.info(f"Extracted {web_from_mixed} web results from mixed results")

        # Remove duplicate results based on URL
        logger.info("Removing duplicate results")
        unique_results = []
        seen_urls = set()
        for result in web_results:
            url = result.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_results.append(result)

        logger.info(
            f"Reduced {len(web_results)} results to {len(unique_results)} unique results"
        )
        web_results = unique_results

        if not web_results:
            logger.warning(f"No search results found for {user.handle}")
            return {user.handle: {"matched_results": []}}

        # Step 3: Filter for Wikipedia results only
        logger.info("STEP 3: Filtering for Wikipedia results only")
        wikipedia_results = [
            r for r in web_results if "wikipedia.org/wiki/" in r.get("url", "")
        ]
        logger.info(f"Found {len(wikipedia_results)} Wikipedia URLs to check")

        if not wikipedia_results:
            logger.info("No Wikipedia results found")
            return {user.handle: {"matched_results": []}}

        # Sort Wikipedia results to prioritize those that seem most relevant to the person
        wikipedia_results.sort(
            key=lambda r: sum(
                part.lower() in r.get("title", "").lower()
                for part in user.name.lower().split()
            ),
            reverse=True,
        )

        # Step 4: Verify and process Wikipedia results
        logger.info("STEP 4: Verifying and processing Wikipedia results")
        matched_results = []

        for result in wikipedia_results:
            url = result.get("url", "")
            logger.info(f"Checking Wikipedia URL: {url}")

            if self.verify_search_result(user.name, user.description, result):
                logger.info(f"Wikipedia page verified as a match: {url}")

                # Extract and summarize Wikipedia content
                logger.info(f"Extracting Wikipedia content")
                wikipedia_data = self.extract_wikipedia_summary(url, user.name)

                # Add to matched results with Wikipedia data directly embedded
                matched_results.append(
                    {
                        "title": result.get("title", ""),
                        "description": result.get("description", ""),
                        "url": url,
                        "source_type": "wikipedia",
                        "summary": wikipedia_data,
                    }
                )

                logger.info(
                    f"Found and processed one Wikipedia match. Skipping other Wikipedia results."
                )
                break
            else:
                logger.info(f"Wikipedia page not verified as a match: {url}")

        # Create the metadata object
        logger.info("STEP 5: Creating final metadata object")
        metadata = {"matched_results": matched_results}

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
        logger.info(f"Processing {len(users)} users")

        for i, user in enumerate(users):
            logger.info(
                f"Processing user {i+1}/{len(users)}: {user.name} (@{user.handle})"
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
            with open(self.output_file, "w+") as f:
                write_json_lines(self.output_file, self.results)

            logger.info(f"Successfully saved metadata to {self.output_file}")
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

    chain = BlueskyMetadataChain(output_file="final_profiles.json")

    # Use fewer workers to avoid overwhelming the rate limiter
    thread_map(chain.process_user, users, max_workers=10)

    chain.save_results()


if __name__ == "__main__":
    main()
