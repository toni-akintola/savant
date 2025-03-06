from bs4 import BeautifulSoup
import json
import requests


class Topic:
    def __init__(self, name, subtopics=None):
        self.name = name
        self.subtopics = subtopics if subtopics else []

    def __repr__(self):
        return f"Topic(name='{self.name}', subtopics={len(self.subtopics)} items)"

    def to_dict(self):
        return {
            "name": self.name,
            "subtopics": [subtopic.to_dict() for subtopic in self.subtopics],
        }


def extract_wikipedia_categories(html_content):
    """Extract categories from the provided HTML content"""
    soup = BeautifulSoup(html_content, "html.parser")

    # Find all main categories
    main_topics = []

    # Each main category is in a div with class contentsPage__heading
    headings = soup.select("div.contentsPage__heading")

    for heading in headings:
        # Extract the main category name from the h2 element
        h2 = heading.select_one("h2")
        if not h2:
            continue

        main_name = h2.text.strip()

        # Create a Topic object for this main category
        main_topic = Topic(main_name)

        # Find the section that contains the content for this category
        section = heading.find_next_sibling("div", class_="contentsPage__section")
        if not section:
            continue
        h_list = section.find("div", class_="hlist")
        li_list = h_list.find_all("li")
        current_topic = (
            Topic(li_list[0].text.strip(), []) if li_list[0].find("b") else main_topic
        )
        for li in li_list[1:]:
            current_topic.subtopics.append(Topic(li.text.strip(), []))
        if current_topic != main_topic:
            main_topic.subtopics.append(current_topic)
        main_topics.append(main_topic)

    return main_topics


def main():

    # For this example, I'll use the actual content
    # Replace the placeholder above with the full HTML content
    html_content = requests.get(
        "https://en.wikipedia.org/wiki/Wikipedia:Contents/Categories"
    ).text

    # Extract the Wikipedia categories
    topics = extract_wikipedia_categories(html_content)

    # Print the topics
    for topic in topics:
        print(topic)

    # Save to a JSON file
    with open("wikipedia_categories.json", "w") as f:
        json.dump([topic.to_dict() for topic in topics], f, indent=2)


if __name__ == "__main__":
    main()
