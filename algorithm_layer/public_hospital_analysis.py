#!/usr/bin/env python3
"""Public-hospital final graph algorithms and report generation."""

from __future__ import annotations

import argparse
import csv
import json
import random
from itertools import islice
from pathlib import Path
from typing import Iterable

import networkx as nx
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run public-hospital final analysis algorithms.")
    parser.add_argument("--node-analysis", default="data_layer/reports/analysis/node_analysis_2021_public_hospitals.csv")
    parser.add_argument("--edge-analysis", default="data_layer/reports/analysis/edge_analysis_2021_public_hospitals.csv")
    parser.add_argument("--facility-panel", default="data_layer/reports/analysis/facility_panel_2021_public_hospitals.csv")
    parser.add_argument("--out-dir", default="algorithm_layer/reports")
    parser.add_argument("--prefix", default="final_2021_public_hospitals")
    parser.add_argument("--delimiter", default=";")
    parser.add_argument("--distance-bands-km", default="50,100,200")
    parser.add_argument("--residence-thresholds", default="3,5,10")
    parser.add_argument("--transfer-thresholds", default="1,2,3")
    parser.add_argument("--primary-min-residence-count", type=float, default=5.0)
    parser.add_argument("--primary-min-transfer-count", type=float, default=2.0)
    parser.add_argument("--centrality-sample-k", type=int, default=200)
    parser.add_argument("--dynamic-stress-steps", type=int, default=5)
    parser.add_argument("--k-path-top-pairs", type=int, default=30)
    parser.add_argument("--k-paths", type=int, default=3)
    parser.add_argument("--flow-top-municipalities", type=int, default=200)
    parser.add_argument("--include-capacity-proxy", action="store_true", help="Write exploratory max-flow/min-cut proxy reports.")
    parser.add_argument("--random-seed", type=int, default=42)
    return parser.parse_args()


def parse_float_list(value: str) -> list[float]:
    values = [float(item.strip()) for item in value.split(",") if item.strip()]
    if not values:
        raise ValueError("list argument must contain at least one value")
    return sorted(set(values))


def as_float(value: object, default: float = 0.0) -> float:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    if result != result:
        return default
    return result


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def load_inputs(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    nodes = pd.read_csv(args.node_analysis, sep=args.delimiter, dtype=str, encoding="utf-8-sig", low_memory=False)
    edges = pd.read_csv(args.edge_analysis, sep=args.delimiter, dtype=str, encoding="utf-8-sig", low_memory=False)
    facilities = pd.read_csv(args.facility_panel, sep=args.delimiter, dtype=str, encoding="utf-8-sig", low_memory=False)
    edges["transfer_count"] = pd.to_numeric(edges["transfer_count"], errors="coerce").fillna(1.0)
    edges["distance_km"] = pd.to_numeric(edges["distance_km"], errors="coerce")
    facilities["admissions"] = pd.to_numeric(facilities.get("admissions", 0), errors="coerce").fillna(0.0)
    return nodes, edges, facilities


def filter_edges(edges: pd.DataFrame, min_residence: float, min_transfer: float, min_distance: float) -> pd.DataFrame:
    edge_type = edges["edge_type"].astype(str)
    count = pd.to_numeric(edges["transfer_count"], errors="coerce").fillna(0.0)
    distance = pd.to_numeric(edges["distance_km"], errors="coerce")
    keep_count = ((edge_type == "residence") & (count >= min_residence)) | ((edge_type == "transfer") & (count >= min_transfer))
    return edges[keep_count & distance.ge(min_distance)].copy()


def build_directed_graph(nodes: pd.DataFrame, edges: pd.DataFrame) -> nx.DiGraph:
    graph = nx.DiGraph()
    node_ids = set(edges["source_node_id"]).union(set(edges["target_node_id"]))
    scoped_nodes = nodes[nodes["node_id"].isin(node_ids)]
    for _, row in scoped_nodes.iterrows():
        graph.add_node(row["node_id"], **{key: value for key, value in row.items() if pd.notna(value) and key != "node_id"})
    for _, row in edges.iterrows():
        graph.add_edge(
            row["source_node_id"],
            row["target_node_id"],
            edge_type=row.get("edge_type", ""),
            transfer_count=as_float(row.get("transfer_count"), 1.0),
            distance_km=max(as_float(row.get("distance_km"), 1.0), 0.001),
        )
    return graph


def weighted_projection(graph: nx.DiGraph) -> nx.Graph:
    projection = nx.Graph()
    projection.add_nodes_from((node, attrs.copy()) for node, attrs in graph.nodes(data=True))
    for source, target, attrs in graph.edges(data=True):
        count = as_float(attrs.get("transfer_count"), 1.0)
        distance = max(as_float(attrs.get("distance_km"), 1.0), 0.001)
        if projection.has_edge(source, target):
            projection[source][target]["transfer_count"] += count
            projection[source][target]["distance_km"] = min(projection[source][target]["distance_km"], distance)
        else:
            projection.add_edge(source, target, transfer_count=count, distance_km=distance)
    return projection


def largest_component(graph: nx.Graph) -> nx.Graph:
    if graph.number_of_nodes() == 0:
        return graph.copy()
    return graph.subgraph(max(nx.connected_components(graph), key=len)).copy()


def sensitivity_matrix(nodes: pd.DataFrame, edges: pd.DataFrame, residence_thresholds: Iterable[float], transfer_thresholds: Iterable[float], distance_bands: Iterable[float]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for residence_min in residence_thresholds:
        for transfer_min in transfer_thresholds:
            for distance_min in distance_bands:
                filtered = filter_edges(edges, residence_min, transfer_min, distance_min)
                graph = weighted_projection(build_directed_graph(nodes, filtered))
                components = list(nx.connected_components(graph)) if graph.number_of_nodes() else []
                largest_nodes = max((len(component) for component in components), default=0)
                rows.append(
                    {
                        "min_residence_count": residence_min,
                        "min_transfer_count": transfer_min,
                        "distance_min_km": distance_min,
                        "nodes": graph.number_of_nodes(),
                        "edges": graph.number_of_edges(),
                        "components": len(components),
                        "largest_component_nodes": largest_nodes,
                        "largest_component_share": round(largest_nodes / graph.number_of_nodes(), 6) if graph.number_of_nodes() else 0.0,
                    }
                )
    return rows


def community_reports(graph: nx.Graph, seed: int) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    if graph.number_of_edges() == 0:
        return [], []
    communities = nx.community.louvain_communities(graph, weight="transfer_count", seed=seed)
    node_to_community = {node: idx for idx, community in enumerate(communities) for node in community}
    rows: list[dict[str, object]] = []
    for idx, community in enumerate(communities):
        subgraph = graph.subgraph(community)
        facility_count = sum(1 for node in community if str(graph.nodes[node].get("node_type", "")) == "facility")
        municipality_count = sum(1 for node in community if str(graph.nodes[node].get("node_type", "")) == "municipality")
        uf_counts: dict[str, int] = {}
        for node in community:
            code = str(graph.nodes[node].get("municipality_code", ""))
            uf = code[:2] if len(code) >= 2 else ""
            uf_counts[uf] = uf_counts.get(uf, 0) + 1
        dominant_uf, dominant_uf_count = max(uf_counts.items(), key=lambda item: item[1]) if uf_counts else ("", 0)
        rows.append(
            {
                "community_id": idx,
                "nodes": len(community),
                "edges": subgraph.number_of_edges(),
                "facility_nodes": facility_count,
                "municipality_nodes": municipality_count,
                "dominant_uf_prefix": dominant_uf,
                "dominant_uf_share": round(dominant_uf_count / len(community), 6) if community else 0.0,
            }
        )

    overlap: dict[tuple[int, str], int] = {}
    for node, community_id in node_to_community.items():
        code = str(graph.nodes[node].get("municipality_code", ""))
        uf = code[:2] if len(code) >= 2 else ""
        overlap[(community_id, uf)] = overlap.get((community_id, uf), 0) + 1
    overlap_rows = [
        {"community_id": community_id, "uf_prefix": uf, "node_count": count}
        for (community_id, uf), count in sorted(overlap.items())
    ]
    return rows, overlap_rows


def average_shortest_path_for_sample(graph: nx.Graph, sample: list[object]) -> float | None:
    sample = [node for node in sample if graph.has_node(node)]
    if len(sample) < 2:
        return None
    sample_set = set(sample)
    distances: list[float] = []
    for source in sample:
        lengths = nx.single_source_dijkstra_path_length(graph, source, weight="distance_km")
        distances.extend(float(distance) for target, distance in lengths.items() if target != source and target in sample_set)
    if not distances:
        return None
    return round(sum(distances) / len(distances), 6)


def dynamic_stress(graph: nx.Graph, steps: int, sample_k: int, seed: int) -> list[dict[str, object]]:
    working = largest_component(graph)
    rng = random.Random(seed)
    sample = list(working.nodes())
    if len(sample) > 120:
        sample = rng.sample(sample, 120)
    baseline_path = average_shortest_path_for_sample(working, sample)
    baseline_nodes = working.number_of_nodes()
    rows = [
        {
            "step": 0,
            "removed_node_id": "",
            "removed_node_name": "",
            "nodes": working.number_of_nodes(),
            "edges": working.number_of_edges(),
            "largest_component_nodes": baseline_nodes,
            "largest_component_share": 1.0 if baseline_nodes else 0.0,
            "sampled_average_shortest_path_km": baseline_path,
            "path_increase_ratio": 0.0,
        }
    ]
    for step in range(1, steps + 1):
        k = min(sample_k, working.number_of_nodes()) if sample_k > 0 else None
        centrality = nx.betweenness_centrality(working, k=k, weight="distance_km", seed=seed + step, normalized=True)
        candidates = [
            (node, value)
            for node, value in centrality.items()
            if str(working.nodes[node].get("node_type", "")) == "facility"
        ]
        if not candidates:
            break
        node_id, _ = max(candidates, key=lambda item: item[1])
        node_name = working.nodes[node_id].get("name", "")
        working.remove_node(node_id)
        component = largest_component(working)
        path = average_shortest_path_for_sample(component, sample)
        rows.append(
            {
                "step": step,
                "removed_node_id": node_id,
                "removed_node_name": node_name,
                "nodes": working.number_of_nodes(),
                "edges": working.number_of_edges(),
                "largest_component_nodes": component.number_of_nodes(),
                "largest_component_share": round(component.number_of_nodes() / working.number_of_nodes(), 6) if working.number_of_nodes() else 0.0,
                "sampled_average_shortest_path_km": path,
                "path_increase_ratio": round((path - baseline_path) / baseline_path, 6) if baseline_path and path is not None else None,
            }
        )
    return rows


def top_facility_centrality(graph: nx.Graph, top_n: int, sample_k: int, seed: int) -> list[dict[str, object]]:
    if graph.number_of_nodes() == 0:
        return []
    k = min(sample_k, graph.number_of_nodes()) if sample_k > 0 else None
    centrality = nx.betweenness_centrality(graph, k=k, weight="distance_km", seed=seed, normalized=True)
    candidates = [
        (node, value)
        for node, value in centrality.items()
        if str(graph.nodes[node].get("node_type", "")) == "facility"
    ]
    rows: list[dict[str, object]] = []
    for rank, (node, value) in enumerate(sorted(candidates, key=lambda item: item[1], reverse=True)[:top_n], start=1):
        attrs = graph.nodes[node]
        rows.append(
            {
                "rank": rank,
                "node_id": node,
                "name": attrs.get("name", ""),
                "municipality_code": attrs.get("municipality_code", ""),
                "betweenness": round(float(value), 10),
                "weighted_degree": round(float(graph.degree(node, weight="transfer_count")), 6),
            }
        )
    return rows


def k_path_redundancy(graph: nx.Graph, residence_edges: pd.DataFrame, top_pairs: int, k_paths: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    top = residence_edges.sort_values("transfer_count", ascending=False).head(top_pairs)
    for _, edge in top.iterrows():
        source = edge["source_node_id"]
        target = edge["target_node_id"]
        if not graph.has_node(source) or not graph.has_node(target):
            continue
        try:
            paths = list(islice(nx.shortest_simple_paths(graph, source, target, weight="distance_km"), k_paths))
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            paths = []
        distances = []
        for path in paths:
            distance = 0.0
            for a, b in zip(path[:-1], path[1:]):
                distance += as_float(graph[a][b].get("distance_km"), 0.0)
            distances.append(distance)
        k1 = distances[0] if distances else None
        k2 = distances[1] if len(distances) > 1 else None
        rows.append(
            {
                "source_node_id": source,
                "target_node_id": target,
                "transfer_count": as_float(edge.get("transfer_count"), 0.0),
                "direct_distance_km": round(as_float(edge.get("distance_km"), 0.0), 6),
                "paths_found": len(paths),
                "k1_distance_km": round(k1, 6) if k1 is not None else None,
                "k2_distance_km": round(k2, 6) if k2 is not None else None,
                "k2_k1_ratio": round(k2 / k1, 6) if k1 and k2 else None,
                "no_alternative": len(paths) < 2,
            }
        )
    return rows


def max_flow_min_cut_proxy(edges: pd.DataFrame, facilities: pd.DataFrame, top_municipalities: int) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    residence = edges[edges["edge_type"].eq("residence")].copy()
    if residence.empty:
        return [], []
    top_sources = residence.groupby("source_node_id")["transfer_count"].sum().sort_values(ascending=False).head(top_municipalities)
    residence = residence[residence["source_node_id"].isin(top_sources.index)].copy()
    targets = sorted(set(residence["target_node_id"]))
    admissions = facilities.set_index("facility_node_id")["admissions"].to_dict() if "facility_node_id" in facilities.columns else {}

    flow_graph = nx.DiGraph()
    source = "__source__"
    sink = "__sink__"
    for node_id, demand in top_sources.items():
        flow_graph.add_edge(source, node_id, capacity=float(demand))
    for _, row in residence.iterrows():
        flow_graph.add_edge(row["source_node_id"], row["target_node_id"], capacity=float(row["transfer_count"]))
    for target in targets:
        capacity = max(float(admissions.get(target, 0.0)), float(residence.loc[residence["target_node_id"].eq(target), "transfer_count"].sum()))
        flow_graph.add_edge(target, sink, capacity=capacity)

    flow_value, _ = nx.maximum_flow(flow_graph, source, sink)
    cut_value, (reachable, non_reachable) = nx.minimum_cut(flow_graph, source, sink)
    demand_total = float(top_sources.sum())
    saturated_facilities = [node for node in reachable if str(node).startswith("facility:") and sink not in reachable]
    summary = [
        {
            "top_municipalities": top_municipalities,
            "demand_total": round(demand_total, 6),
            "max_flow": round(float(flow_value), 6),
            "min_cut_value": round(float(cut_value), 6),
            "unserved_proxy": round(max(0.0, demand_total - float(flow_value)), 6),
            "reachable_nodes": len(reachable),
            "non_reachable_nodes": len(non_reachable),
            "saturated_facility_count": len(saturated_facilities),
        }
    ]
    cut_rows = [{"facility_node_id": node} for node in sorted(saturated_facilities)]
    return summary, cut_rows


def analyze(args: argparse.Namespace) -> dict[str, object]:
    nodes, edges, facilities = load_inputs(args)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    distance_bands = parse_float_list(args.distance_bands_km)
    residence_thresholds = parse_float_list(args.residence_thresholds)
    transfer_thresholds = parse_float_list(args.transfer_thresholds)

    sensitivity = sensitivity_matrix(nodes, edges, residence_thresholds, transfer_thresholds, distance_bands)
    primary_edges = filter_edges(edges, args.primary_min_residence_count, args.primary_min_transfer_count, distance_bands[0])
    primary_graph = build_directed_graph(nodes, primary_edges)
    projection = largest_component(weighted_projection(primary_graph))

    communities, community_overlap = community_reports(projection, args.random_seed)
    centrality = top_facility_centrality(projection, 30, args.centrality_sample_k, args.random_seed)
    stress = dynamic_stress(projection, args.dynamic_stress_steps, args.centrality_sample_k, args.random_seed)
    k_paths = k_path_redundancy(
        projection,
        primary_edges[primary_edges["edge_type"].eq("residence")],
        args.k_path_top_pairs,
        args.k_paths,
    )
    outputs = {
        "sensitivity": out_dir / f"{args.prefix}_sensitivity_matrix.csv",
        "communities": out_dir / f"{args.prefix}_communities.csv",
        "community_overlap": out_dir / f"{args.prefix}_community_region_overlap.csv",
        "centrality": out_dir / f"{args.prefix}_centrality.csv",
        "dynamic_stress": out_dir / f"{args.prefix}_stress_dynamic.csv",
        "k_path_redundancy": out_dir / f"{args.prefix}_k_path_redundancy.csv",
        "summary": out_dir / f"{args.prefix}_summary.json",
    }
    flow_summary: list[dict[str, object]] = []
    flow_cut: list[dict[str, object]] = []
    if args.include_capacity_proxy:
        flow_summary, flow_cut = max_flow_min_cut_proxy(primary_edges, facilities, args.flow_top_municipalities)
        outputs["min_cut_capacity"] = out_dir / f"{args.prefix}_min_cut_capacity.csv"
        outputs["min_cut_facilities"] = out_dir / f"{args.prefix}_min_cut_facilities.csv"

    write_csv(outputs["sensitivity"], sensitivity)
    write_csv(outputs["communities"], communities)
    write_csv(outputs["community_overlap"], community_overlap)
    write_csv(outputs["centrality"], centrality)
    write_csv(outputs["dynamic_stress"], stress)
    write_csv(outputs["k_path_redundancy"], k_paths)
    if args.include_capacity_proxy:
        write_csv(outputs["min_cut_capacity"], flow_summary)
        write_csv(outputs["min_cut_facilities"], flow_cut)

    summary = {
        "node_analysis": args.node_analysis,
        "edge_analysis": args.edge_analysis,
        "facility_panel": args.facility_panel,
        "primary_min_residence_count": args.primary_min_residence_count,
        "primary_min_transfer_count": args.primary_min_transfer_count,
        "distance_bands_km": distance_bands,
        "primary_nodes": primary_graph.number_of_nodes(),
        "primary_edges": primary_graph.number_of_edges(),
        "largest_projection_nodes": projection.number_of_nodes(),
        "largest_projection_edges": projection.number_of_edges(),
        "communities": len(communities),
        "dynamic_stress_steps": len(stress) - 1,
        "k_path_pairs": len(k_paths),
        "capacity_proxy_included": bool(args.include_capacity_proxy),
        "outputs": {key: str(path) for key, path in outputs.items()},
    }
    outputs["summary"].write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")
    return summary


def main() -> int:
    result = analyze(parse_args())
    print(json.dumps(result, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
