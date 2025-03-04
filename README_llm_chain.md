# Bluesky Metadata LLM Chain

This is an LLM chain that uses Anthropic's Claude models to enrich Bluesky user data with verified metadata from web searches.

## Overview

The chain works as follows:

1. **First LLM (Claude)**: Given a Bluesky user's display name and self-description, creates a specific search query optimized for Brave Search to find information about the user.

2. **Brave Search**: Performs the search using the generated query and returns results.

3. **Second LLM (Claude)**: Analyzes each Brave Search result to determine with >95% confidence whether it matches the original Bluesky user.

4. **Storage**: Creates an unstructured metadata object with each user's handle as the key and stores all verified results in a JSON file.

## Requirements

- Python 3.9+
- Anthropic API key
- Brave Search API key

## Installation

1. Install the required Python packages:

```bash
pip install -r requirements.txt
```

2. Create a `.env` file based on the `.env.example` template and add your API keys:

```bash
cp .env.example .env
# Then edit .env with your actual API keys
```

## Usage

You can run the chain from the command line:

```bash
python run_bluesky_metadata_chain.py --users users.json --profiles profiles.json --output results.json
```

### Command-line Arguments

- `--users`: Path to a JSON file containing Bluesky users (required)
- `--profiles`: Path to a JSON file containing Bluesky user profiles with descriptions (optional)
- `--output`: Path to output JSON file (default: bluesky_metadata_results.json)
- `--batch-size`: Number of users to process in each batch (default: 10)
- `--limit`: Limit the number of users to process (optional)

## Example

```bash
python run_bluesky_metadata_chain.py --users user_list.json --profiles user_profiles.json --limit 5
```

This will process the first 5 users from the user_list.json file, using descriptions from user_profiles.json if available.

## Output Format

The output is a JSON file containing an array of objects, each with a Bluesky handle as the key:

```json
[
  {
    "user.handle.bsky.social": {
      "matched_results": [
        {
          "title": "Result Title",
          "description": "Result description",
          "url": "https://example.com/result"
        },
        ...
      ]
    }
  },
  ...
]
```

## Files

- `bluesky_metadata_chain.py`: Main implementation of the LLM chain
- `bluesky_metadata_utils.py`: Utility functions for handling Bluesky data
- `run_bluesky_metadata_chain.py`: Command-line script to run the chain
- `brave_search.py`: Module for interfacing with the Brave Search API 