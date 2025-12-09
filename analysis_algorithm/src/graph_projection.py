"""Build a projected Person-Person graph from Neo4j data."""
from neo4j_connector import get_driver, fetch_person_relations
import networkx as nx


def build_person_projection(driver=None):
    """Return a NetworkX Graph where nodes are Person ids and edges
    connect persons who share an organization, field, country, or
    were co-recipients of the same AwardStatement. Edge weight counts
    how many shared entities connect the pair.
    """
    drv = driver or get_driver()
    data = fetch_person_relations(drv)
    persons = data.get('persons', {})
    relations = data.get('relations', {})

    G = nx.Graph()
    for pid, name in persons.items():
        G.add_node(pid, name=name)

    for rel_name, ent_map in relations.items():
        for ent, members in ent_map.items():
            members = [m for m in members if m is not None]
            if len(members) < 2:
                continue
            for i in range(len(members)):
                for j in range(i + 1, len(members)):
                    u = members[i]
                    v = members[j]
                    if G.has_edge(u, v):
                        G[u][v]['weight'] += 1
                        G[u][v].setdefault('relations', set()).add(rel_name)
                    else:
                        G.add_edge(u, v, weight=1, relations={rel_name})

    return G


if __name__ == '__main__':
    # quick smoke test when run directly
    drv = get_driver()
    G = build_person_projection(drv)
    print(f"Built graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
