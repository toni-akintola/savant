import json
import networkx as nx
import matplotlib.pyplot as plt


def add_edges(graph, parent, children):
    for child in children:
        graph.add_edge(parent, child["name"])
        add_edges(graph, child["name"], child.get("subtopics", []))


def visualize_topic_tree(json_data):
    G = nx.DiGraph()

    for topic in json_data:
        G.add_node(topic["name"])
        add_edges(G, topic["name"], topic.get("subtopics", []))

    plt.figure(figsize=(12, 8))
    pos = nx.spring_layout(G, seed=42, k=0.5)
    nx.draw(
        G,
        pos,
        with_labels=True,
        node_size=800,
        node_color="lightblue",
        edge_color="gray",
        font_size=8,
        font_weight="bold",
    )


data = json.load(open("wikipedia_categories.json"))
visualize_topic_tree(data)
