#!/usr/bin/env python3
"""Check whether the graph has municipality nodes linked to facility nodes."""

from __future__ import annotations

import argparse
from pathlib import Path

import networkx as nx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check municipality-to-facility edges in a GEXF graph.")
    parser.add_argument(
        "--graph-input",
        default="model_layer/reports/graph_sih_br_2021.gexf",
        help="Path to the GEXF graph file.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="How many matching edges to print.",
    )
    return parser.parse_args()


def _node_type(attrs: dict[str, object]) -> str:
    return str(attrs.get("node_type", "")).strip().lower()


def main() -> int:
    args = parse_args()
    graph_path = Path(args.graph_input).resolve()

    if not graph_path.exists():
        print(f"Graph not found: {graph_path}")
        return 2

    graph = nx.read_gexf(graph_path)
    if not isinstance(graph, nx.DiGraph):
        graph = nx.DiGraph(graph)

    municipality_to_facility = []
    facility_to_municipality = []

    for source, target, attrs in graph.edges(data=True):
        source_type = _node_type(graph.nodes[source])
        target_type = _node_type(graph.nodes[target])
        edge_type = str(attrs.get("edge_type", "")).strip().lower()

        if source_type == "municipality" and target_type == "facility":
            municipality_to_facility.append((source, target, edge_type, attrs.get("transfer_count", "")))
        elif source_type == "facility" and target_type == "municipality":
            facility_to_municipality.append((source, target, edge_type, attrs.get("transfer_count", "")))

    print(f"Graph: {graph_path}")
    print(f"Nodes: {graph.number_of_nodes()}")
    print(f"Edges: {graph.number_of_edges()}")
    print(f"Municipality -> facility edges: {len(municipality_to_facility)}")
    print(f"Facility -> municipality edges: {len(facility_to_municipality)}")

    if municipality_to_facility:
        print("Sample municipality -> facility edges:")
        for source, target, edge_type, transfer_count in municipality_to_facility[: args.limit]:
            print(f"  {source} -> {target} | edge_type={edge_type} | transfer_count={transfer_count}")
    else:
        print("No municipality -> facility edges found.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())