import json
import os
from gsa import gather_unstructured_data


def test_metadata_extraction():
    """
    Test the metadata extraction on a small sample of profiles
    """
    # Load the profiles with posts
    profiles_file = "user_profiles_with_posts.json"

    # Create a test output file
    test_output_file = "test_metadata_output.json"

    # Make sure the ANTHROPIC_API_KEY is set
    if "ANTHROPIC_API_KEY" not in os.environ:
        print("Please set the ANTHROPIC_API_KEY environment variable")
        return

    # Load the first 5 profiles for testing
    with open(profiles_file, "r", encoding="utf-8") as f:
        all_profiles = json.load(f)
        test_profiles = all_profiles[:5]

    # Create a temporary file with just 5 profiles
    temp_file = "temp_test_profiles.json"
    with open(temp_file, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i, profile in enumerate(test_profiles):
            json_line = json.dumps(profile)
            f.write(f'  {json_line}{"," if i < len(test_profiles)-1 else ""}\n')
        f.write("]\n")

    # Run the metadata extraction on the test profiles
    print(f"Testing metadata extraction on {len(test_profiles)} profiles...")
    gather_unstructured_data(
        profiles_file=temp_file,
        output_file=test_output_file,
        num_workers=1,  # Use just 1 worker for testing
        batch_size=5,  # Process all 5 profiles in one batch
    )

    # Load and display the results
    with open(test_output_file, "r", encoding="utf-8") as f:
        results = json.load(f)

    print("\n=== Metadata Extraction Results ===")
    for profile in results:
        print(
            f"\nProfile: {profile.get('displayName', 'Unknown')} (@{profile.get('handle', 'unknown')})"
        )
        if profile.get("metadata"):
            print("Metadata extracted successfully:")
            print(json.dumps(profile["metadata"]))  # Print on one line
        else:
            print("No metadata extracted")

    # Clean up temporary file
    os.remove(temp_file)

    print(f"\nTest complete. Full results saved to {test_output_file}")


if __name__ == "__main__":
    test_metadata_extraction()
