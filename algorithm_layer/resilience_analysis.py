#!/usr/bin/env python3
"""Run filtered graph resilience analysis over the validated SIH graph."""

from __future__ import annotations

import argparse
import csv
import json
import random
from pathlib import Path
import sys
from typing import Iterable

import networkx as nx

try:
    from filter_engine import filter_edges_by_min_count, remove_short_edges
except ModuleNotFoundError:
    current_dir = Path(__file__).resolve().parent
    workspace_root = current_dir.parent
    if str(workspace_root) not in sys.path:
        sys.path.insert(0, str(workspace_root))
    from filter_engine import filter_edges_by_min_count, remove_short_edges  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run national SUS graph resilience analysis.")
    parser.add_argument("--graph-input", required=True, help="Input graph file (.gexf).")
    parser.add_argument("--out-dir", default="algorithm_layer/reports", help="Output directory.")
    parser.add_argument("--prefix", default="resilience_sih_br_2021", help="Output filename prefix.")
    parser.add_argument(
        "--distance-bands-km",
        default="50,100,200",
        help="Comma-separated minimum distance bands to compare.",
    )
    parser.add_argument(
        "--min-residence-count",
        type=float,
        default=5.0,
        help="Minimum occurrence count for municipio -> hospital residence edges.",
    )
    parser.add_argument(
        "--min-transfer-count",
        type=float,
        default=2.0,
        help="Minimum occurrence count for hospital -> hospital transfer edges.",
    )
    parser.add_argument("--centrality-top-n", type=int, default=20, help="Top nodes to keep in centrality reports.")
    parser.add_argument(
        "--centrality-node-type",
        default="facility",
        help="Node type to rank for centrality/suppression. Use ALL to include every node type.",
    )
    parser.add_argument(
        "--centrality-sample-k",
        type=int,
        default=300,
        help="Sample size for approximate betweenness. Use 0 for exact calculation.",
    )
    parser.add_argument("--stress-steps", type=int, default=5, help="Number of top central nodes to suppress.")
    parser.add_argument(
        "--path-sample-size",
        type=int,
        default=120,
        help="Node sample size for average shortest-path estimates.",
    )
    parser.add_argument("--random-seed", type=int, default=42, help="Deterministic sampling seed.")
    return parser.parse_args()


def parse_distance_bands(value: str) -> list[float]:
    bands = [float(item.strip()) for item in value.split(",") if item.strip()]
    if not bands:
        raise ValueError("distance-bands-km must contain at least one value")
    if any(band < 0 for band in bands):
        raise ValueError("distance bands must be non-negative")
    return sorted(set(bands))


def _as_float(value: object, default: float = 0.0) -> float:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    if result != result:
        return default
    return result


def weighted_undirected_projection(graph: nx.Graph) -> nx.Graph:
    """Project a directed flow graph to an undirected weighted graph for resilience metrics."""
    projected = nx.Graph()
    for node_id, attrs in graph.nodes(data=True):
        projected.add_node(node_id, **attrs.copy())

    for source, target, attrs in graph.edges(data=True):
        distance = _as_float(attrs.get("distance_km"), 1.0)
        count = _as_float(attrs.get("transfer_count"), 1.0)
        if projected.has_edge(source, target):
            edge_attrs = projected[source][target]
            edge_attrs["transfer_count"] = _as_float(edge_attrs.get("transfer_count")) + count
            edge_attrs["distance_km"] = min(_as_float(edge_attrs.get("distance_km"), distance), distance)
        else:
            projected.add_edge(source, target, distance_km=max(distance, 0.001), transfer_count=count)
    return projected


def largest_component_view(graph: nx.Graph) -> nx.Graph:
    if graph.number_of_nodes() == 0:
        return graph.copy()
    components = nx.connected_components(graph)
    largest = max(components, key=len)
    return graph.subgraph(largest).copy()


def component_summary(graph: nx.Graph) -> dict[str, object]:
    if graph.number_of_nodes() == 0:
        return {
            "components": 0,
            "largest_component_nodes": 0,
            "largest_component_edges": 0,
            "largest_component_share": 0.0,
        }

    sizes = sorted((len(component) for component in nx.connected_components(graph)), reverse=True)
    largest_nodes = sizes[0] if sizes else 0
    largest = largest_component_view(graph)
    return {
        "components": len(sizes),
        "largest_component_nodes": largest_nodes,
        "largest_component_edges": largest.number_of_edges(),
        "largest_component_share": round(largest_nodes / graph.number_of_nodes(), 6),
    }


def approximate_average_shortest_path_km(
    graph: nx.Graph,
    *,
    sample_size: int,
    seed: int,
    weight: str = "distance_km",
) -> float | None:
    if graph.number_of_nodes() < 2:
        return None

    rng = random.Random(seed)
    nodes = list(graph.nodes())
    if len(nodes) > sample_size:
        nodes = rng.sample(nodes, sample_size)

    distances: list[float] = []
    for source in nodes:
        lengths = nx.single_source_dijkstra_path_length(graph, source, weight=weight)
        for target in nodes:
            if source == target:
                continue
            distance = lengths.get(target)
            if distance is not None:
                distances.append(float(distance))

    if not distances:
        return None
    return round(sum(distances) / len(distances), 6)


def sample_nodes(graph: nx.Graph, *, sample_size: int, seed: int) -> list[object]:
    nodes = list(graph.nodes())
    if len(nodes) <= sample_size:
        return nodes
    return random.Random(seed).sample(nodes, sample_size)


def average_shortest_path_for_sample(
    graph: nx.Graph,
    nodes: Iterable[object],
    *,
    weight: str = "distance_km",
) -> float | None:
    sampled_nodes = [node_id for node_id in nodes if graph.has_node(node_id)]
    if len(sampled_nodes) < 2:
        return None

    distances: list[float] = []
    sampled_set = set(sampled_nodes)
    for source in sampled_nodes:
        lengths = nx.single_source_dijkstra_path_length(graph, source, weight=weight)
        for target, distance in lengths.items():
            if target != source and target in sampled_set:
                distances.append(float(distance))

    if not distances:
        return None
    return round(sum(distances) / len(distances), 6)


def top_betweenness(
    graph: nx.Graph,
    *,
    top_n: int,
    sample_k: int,
    seed: int,
    node_type: str | None = "facility",
) -> list[dict[str, object]]:
    if graph.number_of_nodes() == 0:
        return []

    k = None if sample_k <= 0 else min(sample_k, graph.number_of_nodes())
    values = nx.betweenness_centrality(graph, k=k, weight="distance_km", seed=seed, normalized=True)
    if node_type is None:
        candidates = values.items()
    else:
        candidates = (
            (node_id, value)
            for node_id, value in values.items()
            if str(graph.nodes[node_id].get("node_type", "")).strip().lower() == node_type
        )
    ranked = sorted(candidates, key=lambda item: item[1], reverse=True)[:top_n]
    rows: list[dict[str, object]] = []
    for rank, (node_id, value) in enumerate(ranked, start=1):
        attrs = graph.nodes[node_id]
        rows.append(
            {
                "rank": rank,
                "node_id": node_id,
                "name": attrs.get("name", ""),
                "node_type": attrs.get("node_type", ""),
                "municipality_code": attrs.get("municipality_code", ""),
                "betweenness": round(float(value), 10),
                "weighted_degree": round(float(graph.degree(node_id, weight="transfer_count")), 6),
            }
        )
    return rows


def stress_by_node_suppression(
    graph: nx.Graph,
    centrality_rows: Iterable[dict[str, object]],
    *,
    steps: int,
    path_sample_size: int,
    seed: int,
) -> list[dict[str, object]]:
    working = graph.copy()
    baseline_component = largest_component_view(working)
    baseline_sample = sample_nodes(baseline_component, sample_size=path_sample_size, seed=seed)
    baseline_path = average_shortest_path_for_sample(baseline_component, baseline_sample)
    baseline_nodes = baseline_component.number_of_nodes()
    rows: list[dict[str, object]] = [
        {
            "step": 0,
            "removed_node_id": "",
            "removed_node_name": "",
            "nodes": working.number_of_nodes(),
            "edges": working.number_of_edges(),
            "largest_component_nodes": baseline_nodes,
            "largest_component_share": round(baseline_nodes / working.number_of_nodes(), 6)
            if working.number_of_nodes()
            else 0.0,
            "sampled_average_shortest_path_km": baseline_path,
            "path_increase_ratio": 0.0,
        }
    ]

    for step, centrality_row in enumerate(list(centrality_rows)[:steps], start=1):
        node_id = centrality_row["node_id"]
        if working.has_node(node_id):
            working.remove_node(node_id)
        component = largest_component_view(working)
        component_sample = [node_id for node_id in baseline_sample if component.has_node(node_id)]
        path = average_shortest_path_for_sample(component, component_sample)
        if baseline_path and path is not None:
            path_increase_ratio = round((path - baseline_path) / baseline_path, 6)
        else:
            path_increase_ratio = None

        rows.append(
            {
                "step": step,
                "removed_node_id": node_id,
                "removed_node_name": centrality_row.get("name", ""),
                "nodes": working.number_of_nodes(),
                "edges": working.number_of_edges(),
                "largest_component_nodes": component.number_of_nodes(),
                "largest_component_share": round(component.number_of_nodes() / working.number_of_nodes(), 6)
                if working.number_of_nodes()
                else 0.0,
                "sampled_average_shortest_path_km": path,
                "path_increase_ratio": path_increase_ratio,
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def analyze(args: argparse.Namespace) -> dict[str, object]:
    graph_path = Path(args.graph_input).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    graph = nx.read_gexf(graph_path)
    bands = parse_distance_bands(args.distance_bands_km)
    recurring = filter_edges_by_min_count(
        graph,
        min_counts_by_type={
            "residence": args.min_residence_count,
            "transfer": args.min_transfer_count,
        },
        include_all_nodes=False,
    )

    band_summaries: list[dict[str, object]] = []
    centrality_rows_all: list[dict[str, object]] = []
    stress_rows_all: list[dict[str, object]] = []

    for band in bands:
        band_graph = remove_short_edges(recurring, min_distance_km=band, include_all_nodes=False)
        projection = weighted_undirected_projection(band_graph)
        largest_projection = largest_component_view(projection)
        centrality_rows = top_betweenness(
            largest_projection,
            top_n=args.centrality_top_n,
            sample_k=args.centrality_sample_k,
            seed=args.random_seed,
            node_type=None
            if str(args.centrality_node_type).strip().upper() == "ALL"
            else str(args.centrality_node_type).strip().lower(),
        )
        stress_rows = stress_by_node_suppression(
            largest_projection,
            centrality_rows,
            steps=args.stress_steps,
            path_sample_size=args.path_sample_size,
            seed=args.random_seed,
        )

        edge_type_counts: dict[str, int] = {}
        for _, _, attrs in band_graph.edges(data=True):
            edge_type = str(attrs.get("edge_type", "unknown"))
            edge_type_counts[edge_type] = edge_type_counts.get(edge_type, 0) + 1

        summary = {
            "distance_min_km": band,
            "nodes": band_graph.number_of_nodes(),
            "edges": band_graph.number_of_edges(),
            "residence_edges": edge_type_counts.get("residence", 0),
            "transfer_edges": edge_type_counts.get("transfer", 0),
            **component_summary(projection),
        }
        band_summaries.append(summary)

        for row in centrality_rows:
            centrality_rows_all.append({"distance_min_km": band, **row})
        for row in stress_rows:
            stress_rows_all.append({"distance_min_km": band, **row})

    summary_payload = {
        "graph_input": str(graph_path),
        "base_nodes": graph.number_of_nodes(),
        "base_edges": graph.number_of_edges(),
        "recurring_nodes": recurring.number_of_nodes(),
        "recurring_edges": recurring.number_of_edges(),
        "min_residence_count": args.min_residence_count,
        "min_transfer_count": args.min_transfer_count,
        "distance_bands_km": bands,
        "centrality_sample_k": args.centrality_sample_k,
        "centrality_node_type": args.centrality_node_type,
        "stress_steps": args.stress_steps,
        "band_summaries": band_summaries,
    }

    summary_path = out_dir / f"{args.prefix}_summary.json"
    centrality_path = out_dir / f"{args.prefix}_centrality.csv"
    stress_path = out_dir / f"{args.prefix}_stress.csv"
    bands_path = out_dir / f"{args.prefix}_bands.csv"

    summary_path.write_text(json.dumps(summary_payload, indent=2, ensure_ascii=True), encoding="utf-8")
    write_csv(centrality_path, centrality_rows_all)
    write_csv(stress_path, stress_rows_all)
    write_csv(bands_path, band_summaries)

    return {
        "summary": str(summary_path),
        "centrality": str(centrality_path),
        "stress": str(stress_path),
        "bands": str(bands_path),
        **summary_payload,
    }


def main() -> int:
    args = parse_args()
    result = analyze(args)
    print(json.dumps(result, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
