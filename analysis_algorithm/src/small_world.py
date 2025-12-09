"""Small-world analysis: compute average shortest path and clustering.

Usage: call `analyze_small_world(G)` where G is a NetworkX graph.
"""
import networkx as nx
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np


def analyze_small_world(G, compare_random=True, output_file='../output/small_world_result.txt'):
    if G.number_of_nodes() == 0:
        print("Graph is empty")
        return

    # use largest connected component for path computations
    components = list(nx.connected_components(G))
    largest = max(components, key=len)
    Gc = G.subgraph(largest).copy()

    n = Gc.number_of_nodes()
    m = Gc.number_of_edges()

    avg_shortest = nx.average_shortest_path_length(Gc)
    avg_clustering = nx.average_clustering(Gc)

    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        # Header
        f.write("="*80 + "\n")
        f.write("SMALL-WORLD ANALYSIS RESULTS\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"Full graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges\n")
        f.write(f"Largest connected component: {n} nodes, {m} edges\n\n")
        
        f.write("="*80 + "\n")
        f.write("NETWORK METRICS\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"Average shortest path length: {avg_shortest:.4f}\n")
        f.write("  → Measures how many steps it takes to go from one node to another\n")
        f.write("  → Lower value = more tightly connected network\n\n")
        
        f.write(f"Average clustering coefficient: {avg_clustering:.4f}\n")
        f.write("  → Measures how much nodes tend to cluster together\n")
        f.write("  → Higher value = more local communities/triangles\n\n")
        
        print(f"Largest component: {n} nodes, {m} edges")
        print(f"Average shortest path length (largest CC): {avg_shortest:.4f}")
        print(f"Average clustering coefficient (largest CC): {avg_clustering:.4f}")

        if compare_random:
            # random graph with same n and m
            Gr = nx.gnm_random_graph(n, m)
            # pick largest component of random graph
            if nx.is_connected(Gr):
                rand_comp = Gr
            else:
                largest_r = max(nx.connected_components(Gr), key=len)
                rand_comp = Gr.subgraph(largest_r).copy()

            try:
                rand_avg_short = nx.average_shortest_path_length(rand_comp)
            except Exception:
                rand_avg_short = float('inf')
            rand_avg_clust = nx.average_clustering(rand_comp)

            f.write("="*80 + "\n")
            f.write("RANDOM GRAPH COMPARISON\n")
            f.write("="*80 + "\n\n")
            f.write(f"Random graph (same n={n}, m={m}):\n")
            f.write(f"  Average shortest path: {rand_avg_short:.4f}\n")
            f.write(f"  Average clustering: {rand_avg_clust:.4f}\n\n")
            
            f.write("="*80 + "\n")
            f.write("COMPARISON RATIOS\n")
            f.write("="*80 + "\n\n")
            
            path_ratio = avg_shortest / rand_avg_short if rand_avg_short != float('inf') else float('nan')
            clust_ratio = avg_clustering / rand_avg_clust if rand_avg_clust > 0 else float('nan')
            
            f.write(f"Path length ratio (network/random): {path_ratio:.2f}\n")
            f.write(f"Clustering ratio (network/random): {clust_ratio:.2f}\n")

            print("\nRandom graph comparison (same n, m):")
            print(f"Random avg shortest path (largest CC): {rand_avg_short:.4f}")
            print(f"Random avg clustering: {rand_avg_clust:.4f}")
            
            # Visualize with comparison
            _visualize_small_world(avg_shortest, avg_clustering, rand_avg_short, rand_avg_clust, 
                                 path_ratio, clust_ratio, n, m)
        else:
            # Visualize without comparison
            _visualize_small_world(avg_shortest, avg_clustering, None, None, None, None, n, m)
    
    print(f"\nResults saved to: {output_file}")


def _visualize_small_world(avg_shortest, avg_clustering, rand_avg_short=None, rand_avg_clust=None,
                          path_ratio=None, clust_ratio=None, n=0, m=0):
    """Create visualizations for small-world analysis results."""
    print("\nGenerating visualizations...")
    
    if rand_avg_short is not None and rand_avg_clust is not None:
        # Create figure with 2 subplots (with random comparison)
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        
        # Subplot 1: Bar chart comparing metrics
        ax1 = axes[0]
        metrics = ['Avg Shortest\nPath Length', 'Avg Clustering\nCoefficient']
        network_values = [avg_shortest, avg_clustering]
        random_values = [rand_avg_short, rand_avg_clust]
        
        x = np.arange(len(metrics))
        width = 0.35
        
        bars1 = ax1.bar(x - width/2, network_values, width, label='Network', color='#2E86AB', alpha=0.8)
        bars2 = ax1.bar(x + width/2, random_values, width, label='Random Graph', color='#C73E1D', alpha=0.8)
        
        ax1.set_ylabel('Value', fontsize=11, fontweight='bold')
        ax1.set_title(f'Small-World Metrics Comparison\n(n={n}, m={m})', fontsize=12, fontweight='bold')
        ax1.set_xticks(x)
        ax1.set_xticklabels(metrics)
        ax1.legend()
        ax1.grid(axis='y', alpha=0.3)
        
        # Add value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height,
                        f'{height:.4f}', ha='center', va='bottom', fontsize=9)
        
        # Subplot 2: Ratio comparison
        ax2 = axes[1]
        ratio_labels = ['Path Length\nRatio', 'Clustering\nRatio']
        ratio_values = [path_ratio, clust_ratio]
        
        colors = ['#A23B72' if r < 1.5 else '#F18F01' if r < 3 else '#C73E1D' 
                 for r in ratio_values]
        bars3 = ax2.bar(range(len(ratio_labels)), ratio_values, color=colors, alpha=0.8)
        
        # Add reference line at y=1
        ax2.axhline(y=1, color='gray', linestyle='--', linewidth=2, alpha=0.5, label='Random baseline')
        
        ax2.set_ylabel('Ratio (Network / Random)', fontsize=11, fontweight='bold')
        ax2.set_title('Small-World Characteristics\n(Network vs Random)', fontsize=12, fontweight='bold')
        ax2.set_xticks(range(len(ratio_labels)))
        ax2.set_xticklabels(ratio_labels)
        ax2.legend()
        ax2.grid(axis='y', alpha=0.3)
        
        # Add value labels and interpretation
        for i, (bar, ratio) in enumerate(zip(bars3, ratio_values)):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{ratio:.2f}x', ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        # Add interpretation text
        if path_ratio < 1.5 and clust_ratio > 2:
            interpretation = "✓ Small-world network:\nShort paths + High clustering"
            color = 'green'
        elif path_ratio < 2:
            interpretation = "~ Partial small-world:\nShort paths"
            color = 'orange'
        else:
            interpretation = "✗ Not small-world:\nLong paths"
            color = 'red'
        
        ax2.text(0.5, 0.95, interpretation, transform=ax2.transAxes,
                fontsize=10, verticalalignment='top', horizontalalignment='center',
                bbox=dict(boxstyle='round', facecolor=color, alpha=0.2))
        
    else:
        # Create figure with 1 subplot (without random comparison)
        fig, ax = plt.subplots(1, 1, figsize=(8, 6))
        
        metrics = ['Avg Shortest\nPath Length', 'Avg Clustering\nCoefficient']
        values = [avg_shortest, avg_clustering]
        colors = ['#2E86AB', '#A23B72']
        
        bars = ax.bar(range(len(metrics)), values, color=colors, alpha=0.8)
        
        ax.set_ylabel('Value', fontsize=11, fontweight='bold')
        ax.set_title(f'Small-World Metrics\n(n={n}, m={m})', fontsize=12, fontweight='bold')
        ax.set_xticks(range(len(metrics)))
        ax.set_xticklabels(metrics)
        ax.grid(axis='y', alpha=0.3)
        
        # Add value labels
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.4f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    
    # Save figure
    output_path = '../output/visualization/small_world_visualization.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to: {output_path}")
    
    plt.show()


if __name__ == '__main__':
    print('This module provides analyze_small_world(G). Run via the main runner.')
