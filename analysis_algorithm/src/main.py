"""Main runner for graph algorithms.

Usage examples:
  python src/main.py --algo small_world
  python src/main.py --algo ranking --top 30
  python src/main.py --algo community

The script reads Neo4j credentials from `.env` at repository root.
"""
import argparse
import sys
import os

# permit running from repo root: make sure src is in path
sys.path.append(os.path.dirname(__file__))

from graph_projection import build_person_projection
import small_world
import ranking
import community_detection
from neo4j_connector import get_driver


def main():
    parser = argparse.ArgumentParser(description='Run graph algorithms on Nobel network')
    parser.add_argument('--algo', choices=['small_world', 'ranking', 'community'], required=True,
                        help='Algorithm to run')
    parser.add_argument('--top', type=int, default=20, help='Top N nodes to show for ranking')
    parser.add_argument('--no-random-compare', action='store_true', help='Disable random graph comparison for small_world')
    args = parser.parse_args()

    print('Connecting to Neo4j and building projected person graph...')
    drv = get_driver()
    G = build_person_projection(drv)
    print(f'Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges')

    if args.algo == 'small_world':
        small_world.analyze_small_world(G, compare_random=not args.no_random_compare)
    elif args.algo == 'ranking':
        ranking.ranking_report(G, top_n=args.top)
    elif args.algo == 'community':
        community_detection.detect_communities(G)


if __name__ == '__main__':
    main()
