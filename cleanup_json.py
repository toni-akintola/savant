import json
import glob


def cleanup_json_file(filepath):
    try:
        with open(filepath, "r") as f:
            data = json.load(f)

        if not isinstance(data, list):
            print(f"Warning: {filepath} does not contain a JSON array")
            return

        # Write each object as a single line
        with open(filepath, "w") as f:
            f.write("[\n")
            for i, item in enumerate(data):
                json_line = json.dumps(item)
                f.write(f'  {json_line}{"," if i < len(data)-1 else ""}\n')
            f.write("]\n")

        print(f"Cleaned up {filepath}")

    except json.JSONDecodeError:
        print(f"Error: {filepath} contains invalid JSON")
    except Exception as e:
        print(f"Error processing {filepath}: {str(e)}")


def main():
    json_files = glob.glob("*.json")
    for json_file in json_files:
        cleanup_json_file(json_file)


if __name__ == "__main__":
    main()
