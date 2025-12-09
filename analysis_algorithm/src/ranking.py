"""Ranking algorithms: PageRank, degree, betweenness."""
import networkx as nx
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np


def ranking_report(G, top_n=20, output_file='../output/ranking_result.txt'):
    if G.number_of_nodes() == 0:
        print("Graph is empty")
        return

    # PageRank
    print("Computing PageRank...")
    pr = nx.pagerank(G, weight='weight')
    top_pr = sorted(pr.items(), key=lambda x: x[1], reverse=True)[:top_n]

    # Degree centrality
    print("Computing degree centrality...")
    deg = nx.degree_centrality(G)
    top_deg = sorted(deg.items(), key=lambda x: x[1], reverse=True)[:top_n]

    # Betweenness centrality (may be slow for large graphs)
    print("Computing betweenness centrality (can be slow)...")
    bet = nx.betweenness_centrality(G, weight='weight')
    top_bet = sorted(bet.items(), key=lambda x: x[1], reverse=True)[:top_n]

    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        # Header
        f.write("="*80 + "\n")
        f.write("GRAPH RANKING RESULTS\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges\n")
        f.write(f"Top N: {top_n}\n\n")
        
        # PageRank
        f.write("="*80 + "\n")
        f.write("TOP PAGERANK\n")
        f.write("="*80 + "\n")
        f.write("Measures importance based on quality of connections.\n")
        f.write("Higher score = more influential in the network.\n\n")
        
        print("\nTop PageRank:")
        for i, (pid, score) in enumerate(top_pr, 1):
            name = G.nodes[pid].get('name') if pid in G.nodes else ''
            line = f"{i:3d}. {pid:20s} {name:40s} {score:.6f}"
            f.write(line + "\n")
            print(f"{pid}\t{name}\t{score:.6f}")
        
        # Degree Centrality
        f.write("\n" + "="*80 + "\n")
        f.write("TOP DEGREE CENTRALITY\n")
        f.write("="*80 + "\n")
        f.write("Measures number of direct connections.\n")
        f.write("Higher score = more connections to other nodes.\n\n")
        
        print("\nTop Degree Centrality:")
        for i, (pid, score) in enumerate(top_deg, 1):
            name = G.nodes[pid].get('name') if pid in G.nodes else ''
            line = f"{i:3d}. {pid:20s} {name:40s} {score:.6f}"
            f.write(line + "\n")
            print(f"{pid}\t{name}\t{score:.6f}")
        
        # Betweenness Centrality
        f.write("\n" + "="*80 + "\n")
        f.write("TOP BETWEENNESS CENTRALITY\n")
        f.write("="*80 + "\n")
        f.write("Measures bridging role between communities.\n")
        f.write("Higher score = more paths pass through this node.\n\n")
        
        print("\nTop Betweenness Centrality:")
        for i, (pid, score) in enumerate(top_bet, 1):
            name = G.nodes[pid].get('name') if pid in G.nodes else ''
            line = f"{i:3d}. {pid:20s} {name:40s} {score:.6f}"
            f.write(line + "\n")
            print(f"{pid}\t{name}\t{score:.6f}")
    
    print(f"\nResults saved to: {output_file}")
    
    # Visualize the rankings
    _visualize_rankings(G, top_pr, top_deg, top_bet, top_n)


def _visualize_rankings(G, top_pr, top_deg, top_bet, top_n):
    """Create visualizations for ranking results."""
    print("\nGenerating visualizations...")
    
    # Create figure with 3 subplots
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    
    # Prepare data for PageRank
    pr_names = []
    pr_scores = []
    for pid, score in top_pr[:min(15, top_n)]:
        name = G.nodes[pid].get('name', pid) if pid in G.nodes else pid
        # Truncate long names
        if len(name) > 25:
            name = name[:22] + '...'
        pr_names.append(name)
        pr_scores.append(score)
    
    # PageRank bar chart
    ax1 = axes[0]
    bars1 = ax1.barh(range(len(pr_names)), pr_scores, color='#2E86AB')
    ax1.set_yticks(range(len(pr_names)))
    ax1.set_yticklabels(pr_names, fontsize=7)
    ax1.invert_yaxis()
    ax1.set_xlabel('PageRank Score', fontsize=9, fontweight='bold')
    ax1.set_title('Top PageRank\n(Influence)', fontsize=10, fontweight='bold')
    ax1.grid(axis='x', alpha=0.3)
    
    # Add value labels
    for i, (bar, score) in enumerate(zip(bars1, pr_scores)):
        width = bar.get_width()
        ax1.text(width, bar.get_y() + bar.get_height()/2.,
                f' {score:.4f}', ha='left', va='center', fontsize=7)
    
    # Prepare data for Degree Centrality
    deg_names = []
    deg_scores = []
    for pid, score in top_deg[:min(15, top_n)]:
        name = G.nodes[pid].get('name', pid) if pid in G.nodes else pid
        if len(name) > 25:
            name = name[:22] + '...'
        deg_names.append(name)
        deg_scores.append(score)
    
    # Degree Centrality bar chart
    ax2 = axes[1]
    bars2 = ax2.barh(range(len(deg_names)), deg_scores, color='#A23B72')
    ax2.set_yticks(range(len(deg_names)))
    ax2.set_yticklabels(deg_names, fontsize=7)
    ax2.invert_yaxis()
    ax2.set_xlabel('Degree Centrality', fontsize=9, fontweight='bold')
    ax2.set_title('Top Degree Centrality\n(Connections)', fontsize=10, fontweight='bold')
    ax2.grid(axis='x', alpha=0.3)
    
    # Add value labels
    for i, (bar, score) in enumerate(zip(bars2, deg_scores)):
        width = bar.get_width()
        ax2.text(width, bar.get_y() + bar.get_height()/2.,
                f' {score:.4f}', ha='left', va='center', fontsize=7)
    
    # Prepare data for Betweenness Centrality
    bet_names = []
    bet_scores = []
    for pid, score in top_bet[:min(15, top_n)]:
        name = G.nodes[pid].get('name', pid) if pid in G.nodes else pid
        if len(name) > 25:
            name = name[:22] + '...'
        bet_names.append(name)
        bet_scores.append(score)
    
    # Betweenness Centrality bar chart
    ax3 = axes[2]
    bars3 = ax3.barh(range(len(bet_names)), bet_scores, color='#F18F01')
    ax3.set_yticks(range(len(bet_names)))
    ax3.set_yticklabels(bet_names, fontsize=7)
    ax3.invert_yaxis()
    ax3.set_xlabel('Betweenness Centrality', fontsize=9, fontweight='bold')
    ax3.set_title('Top Betweenness Centrality\n(Bridge)', fontsize=10, fontweight='bold')
    ax3.grid(axis='x', alpha=0.3)
    
    # Add value labels
    for i, (bar, score) in enumerate(zip(bars3, bet_scores)):
        width = bar.get_width()
        ax3.text(width, bar.get_y() + bar.get_height()/2.,
                f' {score:.4f}', ha='left', va='center', fontsize=7)
    
    plt.tight_layout()
    
    # Save figure
    output_path = '../output/visualization/ranking_visualization.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to: {output_path}")
    
    plt.show()


if __name__ == '__main__':
    print('Use ranking_report(G, top_n) from the main runner')
