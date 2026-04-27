#!/usr/bin/env python3
"""Compute graph metrics and render simple plots for degree and component sizes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize graph metrics and plots.")
    parser.add_argument("--graph-input", required=True, help="Input graph file (.gexf or .graphml).")
    parser.add_argument("--out-dir", default="viz_layer/reports", help="Output directory for reports and plots.")
    parser.add_argument("--prefix", default="graph", help="Prefix for output files.")
    parser.add_argument("--bins", type=int, default=40, help="Histogram bins for degree distribution.")
    return parser.parse_args()


def _load_graph(path: Path) -> nx.DiGraph:
    suffix = path.suffix.lower()
    if suffix == ".gexf":
        graph = nx.read_gexf(path)
    elif suffix == ".graphml":
        graph = nx.read_graphml(path)
    else:
        raise ValueError("Unsupported graph input extension. Use .gexf or .graphml")

    if not isinstance(graph, nx.DiGraph):
        graph = nx.DiGraph(graph)
    return graph


def _plot_degree_distribution(degrees: list[int], output_path: Path, bins: int) -> None:
    plt.figure(figsize=(8, 5))
    plt.hist(degrees, bins=bins, color="#4C78A8", edgecolor="#1f2d3d", alpha=0.85)
    plt.title("Degree Distribution")
    plt.xlabel("Degree")
    plt.ylabel("Node count")
    plt.tight_layout()
    plt.savefig(output_path, dpi=140)
    plt.close()


def _plot_component_sizes(sizes: list[int], output_path: Path) -> None:
    if not sizes:
        return
    size_counts: dict[int, int] = {}
    for size in sizes:
        size_counts[size] = size_counts.get(size, 0) + 1

    xs = sorted(size_counts)
    ys = [size_counts[x] for x in xs]

    plt.figure(figsize=(8, 5))
    plt.bar(xs, ys, color="#F58518", edgecolor="#7a3e00", alpha=0.85)
    plt.title("Component Size Distribution")
    plt.xlabel("Component size (k)")
    plt.ylabel("Count of components with k nodes")
    plt.tight_layout()
    plt.savefig(output_path, dpi=140)
    plt.close()


def main() -> int:
    args = parse_args()
    graph_input = Path(args.graph_input).resolve()
    out_dir = Path(args.out_dir).resolve()

    if not graph_input.exists():
        print(f"Graph input not found: {graph_input}")
        return 2

    graph = _load_graph(graph_input)
    out_dir.mkdir(parents=True, exist_ok=True)

    degrees = [int(d) for _, d in graph.degree()]
    avg_degree = sum(degrees) / len(degrees) if degrees else 0.0

    if graph.is_directed():
        components = list(nx.strongly_connected_components(graph))
        component_type = "strongly_connected"
        component_count = len(components)
    else:
        components = list(nx.connected_components(graph))
        component_type = "connected"
        component_count = len(components)

    component_sizes = [len(c) for c in components]

    degree_plot = out_dir / f"{args.prefix}_degree_distribution.png"
    _plot_degree_distribution(degrees, degree_plot, bins=max(5, int(args.bins)))

    component_plot = out_dir / f"{args.prefix}_component_size_distribution.png"
    if component_count > 1:
        _plot_component_sizes(component_sizes, component_plot)
    else:
        component_plot = None

    summary = {
        "graph_input": str(graph_input),
        "nodes": int(graph.number_of_nodes()),
        "edges": int(graph.number_of_edges()),
        "average_degree": round(float(avg_degree), 6),
        "component_type": component_type,
        "component_count": int(component_count),
        "degree_distribution_plot": str(degree_plot),
        "component_size_distribution_plot": str(component_plot) if component_plot else None,
    }

    summary_path = out_dir / f"{args.prefix}_metrics_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"Nodes: {summary['nodes']}")
    print(f"Edges: {summary['edges']}")
    print(f"Average degree: {summary['average_degree']}")
    print(f"{component_type} components: {summary['component_count']}")
    print(f"Degree plot: {degree_plot}")
    if component_plot:
        print(f"Component size plot: {component_plot}")
    print(f"Summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
