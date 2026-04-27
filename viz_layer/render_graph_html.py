#!/usr/bin/env python3
"""Render interactive HTML graph from GEXF/GraphML using PyVis."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import networkx as nx
from pyvis.network import Network


NODE_COLORS = {
    "facility": "#1f77b4",
    "municipality": "#ff7f0e",
    "unknown": "#7f7f7f",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render graph HTML visualization from graph artifact.")
    parser.add_argument("--graph-input", required=True, help="Input graph file (.gexf or .graphml).")
    parser.add_argument("--html-output", required=True, help="Output HTML path.")
    parser.add_argument("--summary-output", help="Optional JSON summary output path.")
    parser.add_argument("--max-nodes", type=int, default=1500, help="Maximum nodes to render after ranking.")
    parser.add_argument("--max-edges", type=int, default=4000, help="Maximum edges to render after ranking.")
    parser.add_argument(
        "--edge-type",
        default="all",
        choices=["all", "transfer", "residence"],
        help="Filter edges by type before rendering.",
    )
    parser.add_argument("--height", default="900px", help="HTML canvas height.")
    parser.add_argument("--width", default="100%", help="HTML canvas width.")
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


def _float_attr(edge_attrs: dict, key: str, default: float) -> float:
    raw = edge_attrs.get(key, default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _select_subgraph(graph: nx.DiGraph, max_nodes: int, max_edges: int) -> nx.DiGraph:
    if graph.number_of_nodes() <= max_nodes and graph.number_of_edges() <= max_edges:
        return graph.copy()

    weighted_degree = sorted(graph.degree(weight="transfer_count"), key=lambda item: item[1], reverse=True)
    keep_nodes = {node_id for node_id, _ in weighted_degree[:max_nodes]}

    subgraph = graph.subgraph(keep_nodes).copy()
    if subgraph.number_of_edges() <= max_edges:
        return subgraph

    ranked_edges = sorted(
        subgraph.edges(data=True),
        key=lambda item: _float_attr(item[2], "transfer_count", 1.0),
        reverse=True,
    )
    keep_edges = ranked_edges[:max_edges]

    filtered = nx.DiGraph()
    for node_id, attrs in subgraph.nodes(data=True):
        filtered.add_node(node_id, **attrs)
    for source, target, attrs in keep_edges:
        filtered.add_edge(source, target, **attrs)

    # Drop isolated nodes after edge filtering for cleaner view.
    isolated = [node_id for node_id in filtered.nodes() if filtered.degree(node_id) == 0]
    filtered.remove_nodes_from(isolated)
    return filtered


def _filter_edges_by_type(graph: nx.DiGraph, edge_type: str) -> nx.DiGraph:
    if edge_type == "all":
        return graph

    filtered = nx.DiGraph()
    for node_id, attrs in graph.nodes(data=True):
        filtered.add_node(node_id, **attrs)
    for source, target, attrs in graph.edges(data=True):
        if str(attrs.get("edge_type", "")) == edge_type:
            filtered.add_edge(source, target, **attrs)

    isolated = [node_id for node_id in filtered.nodes() if filtered.degree(node_id) == 0]
    filtered.remove_nodes_from(isolated)
    return filtered


def render_html(graph: nx.DiGraph, html_output: Path, width: str, height: str) -> None:
    net = Network(height=height, width=width, directed=True, bgcolor="#f8f9fb", font_color="#1a1a1a")
    net.barnes_hut(gravity=-28000, central_gravity=0.2, spring_length=170, spring_strength=0.025, damping=0.09)

    for node_id, attrs in graph.nodes(data=True):
        node_type = str(attrs.get("node_type", "unknown")).strip().lower() or "unknown"
        color = NODE_COLORS.get(node_type, NODE_COLORS["unknown"])

        label = str(attrs.get("name") or node_id)
        title = (
            f"<b>{label}</b><br>"
            f"node_id: {node_id}<br>"
            f"node_type: {node_type}<br>"
            f"municipality_code: {attrs.get('municipality_code', '')}"
        )

        weighted_degree = graph.degree(node_id, weight="transfer_count")
        size = max(8.0, min(40.0, 8.0 + float(weighted_degree) * 0.6))

        net.add_node(
            node_id,
            label=label,
            title=title,
            color=color,
            size=size,
        )

    for source, target, attrs in graph.edges(data=True):
        transfer_count = _float_attr(attrs, "transfer_count", 1.0)
        confidence = _float_attr(attrs, "confidence_score", 0.0)
        width_scale = max(0.8, min(12.0, transfer_count * 0.5))

        title = (
            f"<b>{source} -> {target}</b><br>"
            f"transfer_count: {transfer_count}<br>"
            f"confidence_score: {round(confidence, 4)}<br>"
            f"match_method: {attrs.get('match_method', '')}<br>"
            f"edge_type: {attrs.get('edge_type', '')}"
        )

        net.add_edge(
            source,
            target,
            value=transfer_count,
            width=width_scale,
            title=title,
            color="#6c757d",
            arrows="to",
        )

    html_output.parent.mkdir(parents=True, exist_ok=True)
    net.save_graph(str(html_output))


def main() -> int:
    args = parse_args()
    graph_input = Path(args.graph_input).resolve()
    html_output = Path(args.html_output).resolve()

    if not graph_input.exists():
        print(f"Graph input not found: {graph_input}")
        return 2

    graph = _load_graph(graph_input)
    filtered_graph = _filter_edges_by_type(graph, edge_type=args.edge_type)
    render_graph = _select_subgraph(filtered_graph, max_nodes=args.max_nodes, max_edges=args.max_edges)

    render_html(render_graph, html_output, width=args.width, height=args.height)

    summary = {
        "graph_input": str(graph_input),
        "html_output": str(html_output),
        "original_nodes": int(graph.number_of_nodes()),
        "original_edges": int(graph.number_of_edges()),
        "filtered_nodes": int(filtered_graph.number_of_nodes()),
        "filtered_edges": int(filtered_graph.number_of_edges()),
        "rendered_nodes": int(render_graph.number_of_nodes()),
        "rendered_edges": int(render_graph.number_of_edges()),
        "edge_type": args.edge_type,
        "max_nodes": int(args.max_nodes),
        "max_edges": int(args.max_edges),
    }

    if args.summary_output:
        summary_output = Path(args.summary_output).resolve()
        summary_output.parent.mkdir(parents=True, exist_ok=True)
        summary_output.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"Rendered nodes: {render_graph.number_of_nodes()}")
    print(f"Rendered edges: {render_graph.number_of_edges()}")
    print(f"HTML written: {html_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
