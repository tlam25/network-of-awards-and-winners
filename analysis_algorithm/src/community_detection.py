"""Community detection using the Louvain method (python-louvain).

Produces partition mapping and basic stats.
"""
import networkx as nx
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

try:
    import community as community_louvain
except Exception:
    community_louvain = None


def detect_communities(G, output_file='../output/community_detection_result.txt'):
    if community_louvain is None:
        raise RuntimeError("python-louvain (imported as 'community') is required. Install 'python-louvain'.")
    if G.number_of_nodes() == 0:
        print("Graph is empty")
        return

    print("Running Louvain community detection...")
    partition = community_louvain.best_partition(G, weight='weight')

    # compute sizes
    sizes = {}
    for node, com in partition.items():
        sizes.setdefault(com, 0)
        sizes[com] += 1

    modularity = community_louvain.modularity(partition, G, weight='weight')

    # Open output file
    with open(output_file, 'w', encoding='utf-8') as f:
        # Write header
        f.write("="*80 + "\n")
        f.write("LOUVAIN COMMUNITY DETECTION RESULTS\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges\n")
        f.write(f"Detected {len(sizes)} communities\n")
        f.write(f"Modularity: {modularity:.4f}\n")
        
        print(f"Detected {len(sizes)} communities")
        print(f"Modularity: {modularity:.4f}")
        
        # Print summary of top communities
        top = sorted(sizes.items(), key=lambda x: x[1], reverse=True)[:10]
        f.write("\nTop 10 communities (by size):\n")
        print("\nTop communities (id -> size):")
        for cid, sz in top:
            f.write(f"  Community {cid}: {sz} nodes\n")
            print(f"Community {cid}: {sz} nodes")
        
        # Print detailed information for each community
        f.write("\n" + "="*80 + "\n")
        f.write("DETAILED COMMUNITY INFORMATION\n")
        f.write("="*80 + "\n")
        
        print("\n" + "="*80)
        print("DETAILED COMMUNITY INFORMATION")
        print("="*80)
        
        # Group nodes by community
        communities = {}
        for node, com_id in partition.items():
            communities.setdefault(com_id, []).append(node)
        
        # Sort communities by size (descending)
        sorted_communities = sorted(communities.items(), key=lambda x: len(x[1]), reverse=True)
        
        for com_id, members in sorted_communities:
            f.write(f"\n{'─'*80}\n")
            f.write(f"COMMUNITY {com_id} ({len(members)} nodes)\n")
            f.write(f"{'─'*80}\n")
            
            print(f"\n{'─'*80}")
            print(f"COMMUNITY {com_id} ({len(members)} nodes)")
            print(f"{'─'*80}")
            
            # Print members with names
            f.write("\nMembers:\n")
            print("\nMembers:")
            for i, node in enumerate(members, 1):
                name = G.nodes[node].get('name', 'N/A')
                line = f"  {i:3d}. {node:20s} - {name}"
                f.write(line + "\n")
                print(line)
            
            # Count internal edges (edges within this community)
            internal_edges = 0
            internal_edge_list = []
            total_weight = 0
            for u in members:
                for v in members:
                    if u < v and G.has_edge(u, v):
                        internal_edges += 1
                        weight = G[u][v].get('weight', 1)
                        total_weight += weight
                        internal_edge_list.append((u, v, weight))
            
            f.write(f"\nInternal edges: {internal_edges}\n")
            f.write(f"Total internal weight: {total_weight}\n")
            
            print(f"\nInternal edges: {internal_edges}")
            print(f"Total internal weight: {total_weight}")
            
            # Calculate density of this community
            max_edges = len(members) * (len(members) - 1) / 2
            density = internal_edges / max_edges if max_edges > 0 else 0
            f.write(f"Density: {density:.4f}\n")
            print(f"Density: {density:.4f}")
            
            # Print edge details (first 50 edges to avoid too long output)
            if internal_edge_list:
                f.write(f"\nEdge details (showing first 50 of {len(internal_edge_list)}):\n")
                for i, (u, v, weight) in enumerate(internal_edge_list[:50], 1):
                    u_name = G.nodes[u].get('name', 'N/A')
                    v_name = G.nodes[v].get('name', 'N/A')
                    relations = G[u][v].get('relations', set())
                    relations_str = ', '.join(sorted(relations))
                    f.write(f"  {i:3d}. {u} ({u_name}) <-> {v} ({v_name})\n")
                    f.write(f"       Weight: {weight}, Relations: [{relations_str}]\n")

    print(f"\nResults saved to: {output_file}")
    
    # Visualize the communities
    _visualize_communities(G, partition, sizes, modularity)
    
    return partition


def _visualize_communities(G, partition, sizes, modularity):
    """Create visualizations for community detection results."""
    print("\nGenerating visualizations...")
    
    # Create figure with 2 subplots
    fig = plt.figure(figsize=(16, 7))
    
    # Subplot 1: Network graph with communities colored
    ax1 = plt.subplot(121)
    
    # Sample the graph if it's too large (for better performance)
    if G.number_of_nodes() > 200:
        # Sample top communities and random nodes
        top_communities = sorted(sizes.items(), key=lambda x: x[1], reverse=True)[:5]
        top_comm_ids = [c[0] for c in top_communities]
        sampled_nodes = [n for n, c in partition.items() if c in top_comm_ids]
        G_vis = G.subgraph(sampled_nodes).copy()
        partition_vis = {n: partition[n] for n in sampled_nodes}
        ax1.set_title(f'Community Structure (Top 5 Communities, {len(sampled_nodes)} nodes)\nModularity: {modularity:.4f}', 
                     fontsize=12, fontweight='bold')
    else:
        G_vis = G
        partition_vis = partition
        ax1.set_title(f'Community Structure ({G.number_of_nodes()} nodes)\nModularity: {modularity:.4f}', 
                     fontsize=12, fontweight='bold')
    
    # Generate colors for communities
    num_communities = len(set(partition_vis.values()))
    colors = cm.tab20(np.linspace(0, 1, num_communities))
    community_to_color = {com: colors[i] for i, com in enumerate(set(partition_vis.values()))}
    node_colors = [community_to_color[partition_vis[node]] for node in G_vis.nodes()]
    
    # Draw network
    pos = nx.spring_layout(G_vis, k=0.5, iterations=50, seed=42)
    nx.draw_networkx_nodes(G_vis, pos, node_color=node_colors, node_size=50, alpha=0.8, ax=ax1)
    nx.draw_networkx_edges(G_vis, pos, alpha=0.2, width=0.5, ax=ax1)
    ax1.axis('off')
    
    # Subplot 2: Community size distribution
    ax2 = plt.subplot(122)
    
    # Sort communities by size
    sorted_communities = sorted(sizes.items(), key=lambda x: x[1], reverse=True)
    
    # Show top 20 communities
    top_n = min(20, len(sorted_communities))
    comm_ids = [f"C{c[0]}" for c in sorted_communities[:top_n]]
    comm_sizes = [c[1] for c in sorted_communities[:top_n]]
    
    bars = ax2.bar(range(top_n), comm_sizes, color=[community_to_color.get(sorted_communities[i][0], 'gray') 
                                                      for i in range(top_n)])
    ax2.set_xlabel('Community ID', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Number of Nodes', fontsize=11, fontweight='bold')
    ax2.set_title(f'Top {top_n} Communities by Size\nTotal Communities: {len(sizes)}', 
                 fontsize=12, fontweight='bold')
    ax2.set_xticks(range(top_n))
    ax2.set_xticklabels(comm_ids, rotation=45, ha='right')
    ax2.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for i, (bar, size) in enumerate(zip(bars, comm_sizes)):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{size}', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    
    # Save figure
    output_path = '../output/visualization/community_detection_visualization.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to: {output_path}")
    
    plt.show()


if __name__ == '__main__':
    print('Use detect_communities(G) from the main runner')
