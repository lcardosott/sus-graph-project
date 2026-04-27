#!/usr/bin/env python3
"""Build directed weighted graph artifacts from nodes/edges CSV files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import networkx as nx
import pandas as pd

try:
    from data_layer.contract_validation import ValidationThresholds, validate_nodes_edges_contract
except ModuleNotFoundError:
    current_dir = Path(__file__).resolve().parent
    workspace_root = current_dir.parent
    if str(workspace_root) not in sys.path:
        sys.path.insert(0, str(workspace_root))
    from data_layer.contract_validation import ValidationThresholds, validate_nodes_edges_contract  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build directed weighted graph from tabular nodes and edges.")
    parser.add_argument("--nodes-input", required=True, help="Input nodes CSV path.")
    parser.add_argument("--edges-input", required=True, help="Input edges CSV path.")
    parser.add_argument("--graph-output", required=True, help="Output graph file (.gexf or .graphml).")
    parser.add_argument("--summary-output", help="Optional JSON summary output path.")
    parser.add_argument("--delimiter", default=";", help="CSV delimiter for inputs.")
    parser.add_argument("--min-transfer-count", type=float, default=1.0, help="Minimum transfer_count to include an edge.")
    parser.add_argument("--top-n-metrics", type=int, default=15, help="Top N nodes for degree metrics in summary.")
    parser.add_argument("--skip-contract-validation", action="store_true", help="Skip phase-1 node/edge contract validation.")
    parser.add_argument("--contract-report-output", help="Optional JSON report path for contract validation.")
    parser.add_argument(
        "--max-missing-node-ratio",
        type=float,
        default=0.0,
        help="Max tolerated ratio of missing source/target references before graph build.",
    )
    parser.add_argument(
        "--min-node-name-coverage",
        type=float,
        default=0.9,
        help="Minimum node name coverage target used in validation warnings.",
    )
    return parser.parse_args()


def _resolve_edge_endpoints(edges: pd.DataFrame) -> pd.DataFrame:
    if {"source_node_id", "target_node_id"}.issubset(edges.columns):
        resolved = edges.copy()
        resolved["source_node_id"] = resolved["source_node_id"].astype(str).str.strip()
        resolved["target_node_id"] = resolved["target_node_id"].astype(str).str.strip()
        return resolved

    required = {"source_facility_id", "target_location_id", "target_location_type"}
    missing = sorted(required - set(edges.columns))
    if missing:
        raise ValueError("Edge file missing required columns for endpoint resolution: " + ", ".join(missing))

    resolved = edges.copy()
    source = resolved["source_facility_id"].fillna("").astype(str).str.strip()
    target = resolved["target_location_id"].fillna("").astype(str).str.strip()
    target_type = resolved["target_location_type"].fillna("").astype(str).str.strip().str.lower()

    resolved["source_node_id"] = "facility:" + source
    resolved["target_node_id"] = "facility:" + target
    municipality_mask = target_type.eq("municipality")
    resolved.loc[municipality_mask, "target_node_id"] = "municipality:" + target[municipality_mask]
    return resolved


def _top_degree_table(
    graph: nx.DiGraph,
    nodes: pd.DataFrame,
    mode: str,
    top_n: int,
) -> list[dict[str, object]]:
    if mode == "in":
        degree_iter = graph.in_degree(weight="transfer_count")
    elif mode == "out":
        degree_iter = graph.out_degree(weight="transfer_count")
    else:
        degree_iter = graph.degree(weight="transfer_count")

    ranking = sorted(degree_iter, key=lambda item: item[1], reverse=True)[:top_n]
    node_name_lookup = nodes.set_index("node_id")["name"].astype(str).to_dict()
    node_type_lookup = nodes.set_index("node_id")["node_type"].astype(str).to_dict()

    rows: list[dict[str, object]] = []
    for node_id, value in ranking:
        rows.append(
            {
                "node_id": node_id,
                "node_type": node_type_lookup.get(node_id, "unknown"),
                "name": node_name_lookup.get(node_id, ""),
                "degree": round(float(value), 6),
            }
        )
    return rows


def build_graph(nodes: pd.DataFrame, edges: pd.DataFrame, min_transfer_count: float) -> nx.DiGraph:
    graph = nx.DiGraph()

    if "node_id" not in nodes.columns:
        raise ValueError("Nodes file must contain node_id column")

    node_rows = nodes.copy()
    node_rows["node_id"] = node_rows["node_id"].astype(str).str.strip()
    node_rows = node_rows[node_rows["node_id"] != ""]

    for _, row in node_rows.iterrows():
        node_id = row["node_id"]
        attrs = {
            key: value
            for key, value in row.items()
            if key != "node_id" and not pd.isna(value)
        }
        graph.add_node(node_id, **attrs)

    edge_rows = _resolve_edge_endpoints(edges)
    edge_rows["transfer_count"] = pd.to_numeric(edge_rows.get("transfer_count", 1), errors="coerce").fillna(1.0)
    edge_rows["confidence_score"] = pd.to_numeric(
        edge_rows.get("confidence_score", 0),
        errors="coerce",
    ).fillna(0.0)
    edge_rows = edge_rows[edge_rows["transfer_count"] >= min_transfer_count]

    if "edge_type" not in edge_rows.columns:
        edge_rows["edge_type"] = "transfer"

    grouped = (
        edge_rows.groupby(["source_node_id", "target_node_id", "edge_type"], dropna=False)
        .agg(
            transfer_count=("transfer_count", "sum"),
            confidence_score=("confidence_score", "mean") if "confidence_score" in edge_rows.columns else ("transfer_count", "size"),
            match_method=("match_method", "first") if "match_method" in edge_rows.columns else ("source_node_id", "first"),
            rule_version=("rule_version", "first") if "rule_version" in edge_rows.columns else ("source_node_id", "first"),
            assumption_revision=("assumption_revision", "first") if "assumption_revision" in edge_rows.columns else ("source_node_id", "first"),
        )
        .reset_index()
    )

    for _, row in grouped.iterrows():
        source_id = str(row["source_node_id"]).strip()
        target_id = str(row["target_node_id"]).strip()
        if not source_id or not target_id:
            continue

        transfer_count = float(row["transfer_count"])
        confidence_score = float(row["confidence_score"]) if pd.notna(row["confidence_score"]) else 0.0
        graph.add_edge(
            source_id,
            target_id,
            transfer_count=transfer_count,
            weight=transfer_count,
            confidence_score=confidence_score,
            match_method=str(row["match_method"]),
            rule_version=str(row["rule_version"]),
            assumption_revision=str(row["assumption_revision"]),
            edge_type=str(row.get("edge_type", "transfer")),
        )

    return graph


def main() -> int:
    args = parse_args()

    nodes_input = Path(args.nodes_input).resolve()
    edges_input = Path(args.edges_input).resolve()
    graph_output = Path(args.graph_output).resolve()

    if not nodes_input.exists():
        print(f"Nodes input not found: {nodes_input}")
        return 2
    if not edges_input.exists():
        print(f"Edges input not found: {edges_input}")
        return 2

    nodes = pd.read_csv(nodes_input, sep=args.delimiter, encoding="utf-8-sig", dtype=str)
    edges = pd.read_csv(edges_input, sep=args.delimiter, encoding="utf-8-sig", dtype=str)

    contract_validation_report: dict[str, object] | None = None
    contract_report_output: Path | None = None
    if not args.skip_contract_validation:
        resolved_edges = _resolve_edge_endpoints(edges)
        validation_edges = resolved_edges[["source_node_id", "target_node_id"]].copy()
        validation_edges["transfer_count"] = pd.to_numeric(
            resolved_edges.get("transfer_count", 1),
            errors="coerce",
        ).fillna(1.0)

        contract_validation_report = validate_nodes_edges_contract(
            nodes,
            validation_edges,
            thresholds=ValidationThresholds(
                max_missing_node_ratio=float(args.max_missing_node_ratio),
                min_node_name_coverage=float(args.min_node_name_coverage),
            ),
        )
        contract_report_output = (
            Path(args.contract_report_output).resolve()
            if args.contract_report_output
            else graph_output.parent / f"contract_validation_{graph_output.stem}.json"
        )
        contract_report_output.parent.mkdir(parents=True, exist_ok=True)
        contract_report_output.write_text(
            json.dumps(contract_validation_report, indent=2, ensure_ascii=True),
            encoding="utf-8",
        )
        if not bool(contract_validation_report.get("passed", False)):
            print(f"Contract validation failed before graph build. Review: {contract_report_output}")
            return 2

    graph = build_graph(nodes, edges, min_transfer_count=args.min_transfer_count)

    graph_output.parent.mkdir(parents=True, exist_ok=True)
    suffix = graph_output.suffix.lower()
    if suffix == ".gexf":
        nx.write_gexf(graph, graph_output)
    elif suffix == ".graphml":
        nx.write_graphml(graph, graph_output)
    else:
        print("Unsupported graph output extension. Use .gexf or .graphml")
        return 2

    weak_components = nx.number_weakly_connected_components(graph) if graph.number_of_nodes() else 0
    strong_components = nx.number_strongly_connected_components(graph) if graph.number_of_nodes() else 0

    summary = {
        "nodes_input": str(nodes_input),
        "edges_input": str(edges_input),
        "graph_output": str(graph_output),
        "graph_nodes": int(graph.number_of_nodes()),
        "graph_edges": int(graph.number_of_edges()),
        "weakly_connected_components": int(weak_components),
        "strongly_connected_components": int(strong_components),
        "density": round(float(nx.density(graph)), 8) if graph.number_of_nodes() > 1 else 0.0,
        "contract_validation": {
            "enabled": bool(not args.skip_contract_validation),
            "report": str(contract_report_output) if contract_report_output else None,
            "passed": bool(contract_validation_report.get("passed")) if contract_validation_report else None,
        },
        "top_out_degree": _top_degree_table(graph, nodes, mode="out", top_n=args.top_n_metrics),
        "top_in_degree": _top_degree_table(graph, nodes, mode="in", top_n=args.top_n_metrics),
    }

    if args.summary_output:
        summary_output = Path(args.summary_output).resolve()
        summary_output.parent.mkdir(parents=True, exist_ok=True)
        summary_output.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"Graph nodes: {graph.number_of_nodes()}")
    print(f"Graph edges: {graph.number_of_edges()}")
    print(f"Weakly connected components: {weak_components}")
    print(f"Strongly connected components: {strong_components}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
