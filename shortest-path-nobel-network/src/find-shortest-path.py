from neo4j import GraphDatabase
import networkx as nx
import matplotlib.pyplot as plt
import warnings
from dotenv import load_dotenv
import os

warnings.filterwarnings('ignore')
load_dotenv()

URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(
    URI, 
    auth=(USER, PASSWORD),
    notifications_min_severity="OFF"
)

def load_graph_from_neo4j(session):
    # Query với elementId() thay vì id() để tránh deprecated warning
    query = """
            MATCH (n)-[r]->(m)
            RETURN 
                elementId(n) AS source_id,
                labels(n)[0] AS source_label,
                coalesce(n.name, n.id, elementId(n)) AS source_name,
                type(r) AS rel_type,
                elementId(m) AS target_id,
                labels(m)[0] AS target_label,
                coalesce(m.name, m.id, elementId(m)) AS target_name
            """
    
    result = session.run(query)
    
    G = nx.Graph()  # Undirected graph
    
    for record in result:
        source = f"{record['source_label']}:{record['source_name']}"
        target = f"{record['target_label']}:{record['target_name']}"
        
        G.add_node(source, 
                   label=record['source_label'], 
                   name=record['source_name'],
                   neo4j_id=record['source_id'])
        G.add_node(target, 
                   label=record['target_label'], 
                   name=record['target_name'],
                   neo4j_id=record['target_id'])
        G.add_edge(source, target, relationship=record['rel_type'])
    return G

def find_person_node(G, person_name):
    for node, data in G.nodes(data=True):
        if data.get('label') == 'Person' and person_name.lower() in data.get('name', '').lower():
            return node
    return None

def shortest_path_analysis(G, person_a, person_b):
    node_a = find_person_node(G, person_a)
    node_b = find_person_node(G, person_b)
    
    if not node_a:
        print(f"Not found '{person_a}'")
        return
    if not node_b:
        print(f"Not found '{person_b}'")
        return

    try:
        path = nx.shortest_path(G, node_a, node_b)
        length = len(path) - 1
        
        print(f"Path length: {length} steps")
        
        for i, node in enumerate(path, 1):
            print(f"  {i}. {node}")
            
            # Print relationship between steps
            if i < len(path):
                edge_data = G.get_edge_data(node, path[i])
                print(f"     [{edge_data['relationship']}]")
        
        return path
        
    except nx.NetworkXNoPath:
        print(f"\nNo path exists between '{person_a}' and '{person_b}'")

if __name__ == "__main__":
    with driver.session() as session:
        # Load graph into NetworkX
        G = load_graph_from_neo4j(session)
        
        # Find shortest path
        shortest_path_analysis(G, "Rabindranath Tagore", "Marie Curie")

    driver.close()