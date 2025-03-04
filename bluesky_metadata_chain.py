import json
import os
import random
import time
import re
import logging
from typing import Dict, List, Any
import anthropic
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

from models import PartialBlueskyUser
from brave_search import search

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
                model="claude-3-haiku-20240307",
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
        try:
            response = anthropic_client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=5,
                temperature=0,
                system="You are a verification system that determines if two sources of information refer to the same person. Respond with ONLY 'YES' if 100% confident of a match, or 'NO' otherwise.",
                messages=[{"role": "user", "content": prompt}],
            )

            # Check if response is affirmative
            result_matches = response.content[0].text.strip().upper() == "YES"
            logger.info(
                f"Verification result for {url}: {'MATCH' if result_matches else 'NO MATCH'}"
            )
            return result_matches
        except Exception as e:
            logger.error(f"Error verifying search result: {str(e)}")
            return False

    def extract_wikipedia_summary(self, url: str, name: str) -> Dict[str, str]:
        """
        Extract and summarize content from a Wikipedia page, focusing on expertise and interests.

        Args:
            url: Wikipedia URL
            name: Name of the person

        Returns:
            Dictionary with summary and other extracted information as raw strings
        """
        logger.info(f"Extracting Wikipedia summary from: {url}")
        try:
            # Fetch the Wikipedia page
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            logger.debug(f"Fetching Wikipedia page content")
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            # Get the HTML content
            html_content = response.text
            logger.debug(f"Retrieved Wikipedia page: {len(html_content)} characters")

            # Use Claude to extract and summarize the content with focus on expertise
            logger.info(f"Generating expertise summary using Claude")
            prompt = f"""
            I have the HTML content of a Wikipedia page about {name}. Please analyze this content and provide a rich, comprehensive summary focused on this person's expertise, interests, and professional domains.
            
            Focus specifically on:
            1. Areas of expertise and specialized knowledge
            2. Professional topics they regularly engage with
            3. Research interests or academic focus areas
            4. Methodologies, frameworks, or approaches they're known for
            5. Key ideas, theories, or concepts they've developed or champion
            6. Current projects or ongoing work
            7. Intellectual influences and how they've shaped their thinking
            
            Rather than just biographical details, I want to understand what makes this person an authority in their field and what specific topics they're deeply knowledgeable about.
            
            Provide a well-structured summary of 400-600 words that would help someone understand this person's intellectual and professional landscape.
            
            Here's the HTML content:
            {html_content[:50000]}  # Limit content to avoid token limits
            """

            response = anthropic_client.messages.create(
                model="claude-3-7-sonnet-latest",
                max_tokens=2000,
                temperature=0,
                system="You are a skilled researcher who analyzes and summarizes a person's expertise, intellectual contributions, and professional interests from their Wikipedia page. Focus on their domain knowledge rather than just biographical details.",
                messages=[{"role": "user", "content": prompt}],
            )

            # Extract the expertise summary as raw text
            expertise_summary = response.content[0].text.strip()
            logger.info(
                f"Generated expertise summary: {len(expertise_summary)} characters"
            )
            logger.debug(f"Summary excerpt: {expertise_summary[:100]}...")

            # Get additional structured information about their expertise
            logger.info(f"Extracting structured expertise data")
            prompt_structured = f"""
            Based on the Wikipedia page for {name}, please create a description of {name}, focusing on the following specific information in a structured format:
            
            1. Primary field(s) of expertise
            2. Key topics of interest
            3. Notable works or publications
            4. Professional affiliations
            5. Methodologies or approaches they're known for
            6. Audience or communities they engage with
            7. Tools, technologies, or platforms they're associated with
        
            
            Here's the HTML content:
            {html_content[:30000]}  # Limit content to avoid token limits
            """

            response_structured = anthropic_client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=2400,
                temperature=0,
                system="You are a data extraction specialist who extracts structured information about a person's expertise and professional interests from Wikipedia pages.",
                messages=[{"role": "user", "content": prompt_structured}],
            )

            # Store the structured data as raw text
            structured_text = response_structured.content[0].text.strip()
            logger.info(
                f"Generated structured expertise data: {len(structured_text)} characters"
            )
            logger.debug(f"Structured data excerpt: {structured_text[:100]}...")

            # Also get basic biographical info for context
            logger.info(f"Extracting basic biographical data")
            prompt_bio = f"""
            Based on the Wikipedia page for {name}, please extract only the following basic biographical information:
            
            1. Birth date (if available)
            2. Nationality/country
            3. Current position/role
            4. Education
            
            
            Here's the HTML content:
            {html_content[:20000]}  # Limit content to avoid token limits
            """

            response_bio = anthropic_client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=300,
                temperature=0,
                system="You are a data extraction specialist who extracts basic biographical information from Wikipedia pages.",
                messages=[{"role": "user", "content": prompt_bio}],
            )

            # Store the biographical data as raw text
            bio_text = response_bio.content[0].text.strip()
            logger.info(f"Generated biographical data: {len(bio_text)} characters")
            logger.debug(f"Biographical data excerpt: {bio_text[:100]}...")

            wikipedia_data = {
                "expertise_summary": expertise_summary,
                "expertise_data_raw": structured_text,
                "biographical_data_raw": bio_text,
                "source_url": url,
            }

            logger.info(f"Completed Wikipedia extraction for {name}")
            return wikipedia_data

        except Exception as e:
            logger.error(f"Error extracting Wikipedia summary: {str(e)}")
            return {
                "expertise_summary": f"Failed to extract summary: {str(e)}",
                "expertise_data_raw": "",
                "biographical_data_raw": "",
                "source_url": url,
            }

    def process_user(
        self, user: PartialBlueskyUser, description: str = ""
    ) -> Dict[str, Any]:
        """
        Process a single Bluesky user through the entire chain.

        Args:
            user: PartialBlueskyUser object
            description: User's self-description on Bluesky

        Returns:
            Metadata object for the user
        """
        logger.info(f"Processing user: {user.name} (@{user.handle})")
        if description:
            logger.info(f"User description: {description}")
        else:
            logger.info("No description available for this user")

        # Step 1: Create search query for general information
        logger.info("STEP 1: Creating general search query")
        query = self.create_search_query(user.name, description)

        # Step 2: Create a specific Wikipedia query (only if description is available)
        wikipedia_results = None
        if description:
            logger.info("STEP 2: Creating Wikipedia-specific query")
            wikipedia_query = f"{user.name}+wikipedia"
            logger.info(f"Wikipedia query: {wikipedia_query}")
        else:
            logger.info(
                "STEP 2: Skipping Wikipedia search as no description is available"
            )

        # Step 3: Perform Brave searches
        logger.info("STEP 3: Performing Brave searches")
        logger.info(f"Executing general search with query: {query}")
        search_results = search(query)

        if description:
            logger.info(f"Executing Wikipedia search with query: {wikipedia_query}")
            wikipedia_results = search(wikipedia_query)

        # Step 4: Extract web results based on the API response structure
        logger.info("STEP 4: Extracting and processing search results")
        web_results = []

        # Process search results
        results_to_process = [search_results]
        if description:
            results_to_process.append(wikipedia_results)

        for idx, results in enumerate(results_to_process):
            search_type = "General" if idx == 0 else "Wikipedia"
            logger.info(f"Processing {search_type} search results")

            try:
                # Direct web results
                if "web" in results and "results" in results["web"]:
                    web_count = len(results["web"]["results"])
                    logger.info(
                        f"Found {web_count} web results in {search_type} search"
                    )
                    web_results.extend(results["web"]["results"])

                # Mixed results
                if "mixed" in results:
                    mixed_results = results.get("mixed", {}).get("results", [])
                    mixed_count = len(mixed_results)
                    logger.info(
                        f"Found {mixed_count} mixed results in {search_type} search"
                    )

                    web_from_mixed = 0
                    for item in mixed_results:
                        if item.get("type") == "web" and "content" in item:
                            web_results.append(item["content"])
                            web_from_mixed += 1

                    logger.info(
                        f"Extracted {web_from_mixed} web results from mixed results"
                    )

                # News results
                if "news" in results and "results" in results["news"]:
                    news_results = results["news"]["results"]
                    news_count = len(news_results)
                    logger.info(
                        f"Found {news_count} news results in {search_type} search"
                    )

                    for news in news_results:
                        web_results.append(
                            {
                                "title": news.get("title", ""),
                                "description": news.get("description", ""),
                                "url": news.get("url", ""),
                            }
                        )

                # Discussions results
                if "discussions" in results and "results" in results["discussions"]:
                    discussion_results = results["discussions"]["results"]
                    discussion_count = len(discussion_results)
                    logger.info(
                        f"Found {discussion_count} discussion results in {search_type} search"
                    )

                    for discussion in discussion_results:
                        web_results.append(
                            {
                                "title": discussion.get("title", ""),
                                "description": discussion.get("description", ""),
                                "url": discussion.get("url", ""),
                            }
                        )
            except Exception as e:
                logger.error(f"Error processing {search_type} search results: {str(e)}")
                logger.debug(
                    f"Search results structure: {json.dumps(results, indent=2)[:500]}..."
                )

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

        # Step 5: Verify each result and prioritize Wikipedia (only if description is available)
        logger.info("STEP 5: Verifying search results and extracting Wikipedia data")
        matched_results = []
        wikipedia_data = None
        found_wikipedia_match = False

        # First, check specifically for Wikipedia results (only if description is available)
        if description:
            logger.info("Checking for Wikipedia results first")
            wikipedia_results = [
                r for r in web_results if "wikipedia.org/wiki/" in r.get("url", "")
            ]
            logger.info(f"Found {len(wikipedia_results)} Wikipedia URLs to check")

            # Sort Wikipedia results to prioritize those that seem most relevant to the person
            # This helps ensure we process the most likely match first
            wikipedia_results.sort(
                key=lambda r: sum(
                    part.lower() in r.get("title", "").lower()
                    for part in user.name.lower().split()
                ),
                reverse=True,
            )

            for result in wikipedia_results:
                url = result.get("url", "")
                logger.info(f"Checking Wikipedia URL: {url}")

                if self.verify_search_result(user.name, description, result):
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
                            "wikipedia_data": {
                                "expertise_summary": wikipedia_data[
                                    "expertise_summary"
                                ],
                                "expertise_data_raw": wikipedia_data[
                                    "expertise_data_raw"
                                ],
                                "biographical_data_raw": wikipedia_data[
                                    "biographical_data_raw"
                                ],
                            },
                        }
                    )

                    # Set flag to indicate we found a Wikipedia match
                    found_wikipedia_match = True
                    logger.info(
                        f"Found and processed one Wikipedia match. Skipping other Wikipedia results."
                    )
                    break
                else:
                    logger.info(f"Wikipedia page not verified as a match: {url}")

            if not found_wikipedia_match:
                logger.info("No matching Wikipedia results found")
        else:
            logger.info(
                "Skipping Wikipedia result processing as no description is available"
            )

        # Then process other results
        logger.info("Processing non-Wikipedia results")
        other_results_count = 0
        verified_count = 0

        for result in web_results:
            url = result.get("url", "")

            # Skip Wikipedia results as they've already been processed or we're skipping them
            if url and "wikipedia.org/wiki/" in url:
                continue

            other_results_count += 1
            if self.verify_search_result(user.name, description, result):
                verified_count += 1
                logger.info(f"Verified result: {url}")

                # Determine source type
                source_type = "other"
                if "linkedin.com" in url:
                    source_type = "linkedin"
                elif "twitter.com" in url or "x.com" in url:
                    source_type = "twitter"
                elif "github.com" in url:
                    source_type = "github"
                elif "medium.com" in url or "substack.com" in url or "blog" in url:
                    source_type = "blog"
                elif ".edu" in url:
                    source_type = "academic"
                elif (
                    "news" in url
                    or "nytimes.com" in url
                    or "washingtonpost.com" in url
                    or "cnn.com" in url
                ):
                    source_type = "news"

                logger.info(f"Categorized as: {source_type}")

                # Store the matched result
                matched_results.append(
                    {
                        "title": result.get("title", ""),
                        "description": result.get("description", ""),
                        "url": url,
                        "source_type": source_type,
                    }
                )

        logger.info(
            f"Processed {other_results_count} non-Wikipedia results, verified {verified_count}"
        )
        logger.info(f"Total matched results: {len(matched_results)}")

        # Create the metadata object
        logger.info("STEP 6: Creating final metadata object")
        metadata = {user.handle: {"matched_results": matched_results}}

        logger.info(f"Completed processing for user: {user.name} (@{user.handle})")
        return metadata

    def process_users(
        self, users: List[PartialBlueskyUser], descriptions: Dict[str, str] = None
    ) -> None:
        """
        Process multiple Bluesky users and save results to file.

        Args:
            users: List of PartialBlueskyUser objects
            descriptions: Dictionary mapping handles to descriptions
        """
        logger.info(f"Processing {len(users)} users")
        if descriptions is None:
            descriptions = {}

        for i, user in enumerate(users):
            logger.info(
                f"Processing user {i+1}/{len(users)}: {user.name} (@{user.handle})"
            )
            description = descriptions.get(user.handle, "")
            user_metadata = self.process_user(user, description)

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
            with open(self.output_file, "w") as f:
                json.dump(self.results, f, indent=2)

            logger.info(f"Successfully saved metadata to {self.output_file}")
        except Exception as e:
            logger.error(f"Error saving results to file: {str(e)}")


def main():
    """Example usage of the BlueskyMetadataChain."""
    logger.info("Starting BlueskyMetadataChain example")

    with open("user_profiles.json", "r") as f:
        users = [
            PartialBlueskyUser(
                name=user.get("displayName", ""),
                handle=user.get("handle", ""),
                description=user.get("description", ""),
            )
            for user in json.load(f)
        ]

    random_users = random.sample(users, 30)

    chain = BlueskyMetadataChain(output_file="metadata_results.json")
    chain.process_users(random_users)


if __name__ == "__main__":
    main()
