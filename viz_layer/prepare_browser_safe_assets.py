#!/usr/bin/env python3
"""Create browser-safe JSONL + meta from full nodes/edges CSV outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare browser-safe UI assets from nodes/edges CSV files.")
    parser.add_argument("--nodes-input", required=True, help="Path to nodes CSV (sih_br_YYYY_nodes.csv).")
    parser.add_argument("--edges-input", required=True, help="Path to edges CSV (sih_br_YYYY_edges.csv).")
    parser.add_argument("--out-dir", required=True, help="Output directory for JSONL/meta.")
    parser.add_argument("--prefix", required=True, help="Output file prefix (e.g., sih_br_2021_safe).")
    parser.add_argument("--delimiter", default=";", help="CSV delimiter.")
    parser.add_argument("--max-nodes", type=int, default=20000, help="Maximum nodes to include.")
    parser.add_argument("--max-edges", type=int, default=40000, help="Maximum edges to include.")
    return parser.parse_args()


def _json_safe(value: object) -> object:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    return value


def _coerce_str(frame: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        if column in frame.columns:
            frame[column] = frame[column].where(frame[column].notna(), "").astype(str).str.strip()


def _score_nodes(edges: pd.DataFrame) -> pd.Series:
    edges = edges.copy()
    if "transfer_count" not in edges.columns:
        edges["transfer_count"] = 1.0
    edges["transfer_count"] = pd.to_numeric(edges["transfer_count"], errors="coerce").fillna(1.0)

    source_scores = edges.groupby("source_node_id")["transfer_count"].sum()
    target_scores = edges.groupby("target_node_id")["transfer_count"].sum()

    scores = source_scores.add(target_scores, fill_value=0.0)
    scores.name = "score"
    return scores.sort_values(ascending=False)


def _filter_edges(edges: pd.DataFrame, max_edges: int) -> pd.DataFrame:
    edges = edges.copy()
    if "transfer_count" not in edges.columns:
        edges["transfer_count"] = 1.0
    edges["transfer_count"] = pd.to_numeric(edges["transfer_count"], errors="coerce").fillna(1.0)

    edges = edges.sort_values("transfer_count", ascending=False)
    if len(edges) > max_edges:
        edges = edges.head(max_edges)
    return edges


def main() -> int:
    args = parse_args()
    nodes_path = Path(args.nodes_input).resolve()
    edges_path = Path(args.edges_input).resolve()
    out_dir = Path(args.out_dir).resolve()

    if not nodes_path.exists():
        print(f"Nodes input not found: {nodes_path}")
        return 2
    if not edges_path.exists():
        print(f"Edges input not found: {edges_path}")
        return 2

    nodes = pd.read_csv(nodes_path, sep=args.delimiter, dtype=str, low_memory=False)
    edges = pd.read_csv(edges_path, sep=args.delimiter, dtype=str, low_memory=False)

    _coerce_str(nodes, ["node_id", "node_type", "name", "municipality_code"])
    _coerce_str(edges, ["source_node_id", "target_node_id", "edge_type", "match_method"])

    # Drop rows with missing endpoints early.
    edges = edges[(edges["source_node_id"].astype(str) != "") & (edges["target_node_id"].astype(str) != "")]

    edges = _filter_edges(edges, args.max_edges)
    keep_nodes = set(edges["source_node_id"]).union(set(edges["target_node_id"]))

    if len(keep_nodes) > args.max_nodes:
        scores = _score_nodes(edges)
        keep_nodes = set(scores.head(args.max_nodes).index)
        edges = edges[edges["source_node_id"].isin(keep_nodes) & edges["target_node_id"].isin(keep_nodes)]

    nodes = nodes[nodes["node_id"].isin(keep_nodes)].copy()

    # Filter nodes without coordinates to avoid wasted entries in the map UI.
    if "latitude" in nodes.columns and "longitude" in nodes.columns:
        lat = pd.to_numeric(nodes["latitude"], errors="coerce")
        lon = pd.to_numeric(nodes["longitude"], errors="coerce")
        nodes = nodes[lat.notna() & lon.notna()].copy()
        keep_nodes = set(nodes["node_id"])
        edges = edges[edges["source_node_id"].isin(keep_nodes) & edges["target_node_id"].isin(keep_nodes)]

    nodes_out = out_dir / f"{args.prefix}_nodes.jsonl"
    edges_out = out_dir / f"{args.prefix}_edges.jsonl"
    meta_out = out_dir / f"{args.prefix}_meta.json"
    out_dir.mkdir(parents=True, exist_ok=True)

    with nodes_out.open("w", encoding="utf-8") as handle:
        for row in nodes.itertuples(index=False):
            item = row._asdict()
            handle.write(
                json.dumps(
                    {
                        "node_id": str(item.get("node_id", "")),
                        "node_type": str(item.get("node_type", "")),
                        "name": str(item.get("name", "")),
                        "municipality_code": str(item.get("municipality_code", "")),
                        "latitude": _json_safe(item.get("latitude")),
                        "longitude": _json_safe(item.get("longitude")),
                    },
                    ensure_ascii=True,
                )
            )
            handle.write("\n")

    with edges_out.open("w", encoding="utf-8") as handle:
        for row in edges.itertuples(index=False):
            item = row._asdict()
            handle.write(
                json.dumps(
                    {
                        "source_node_id": str(item.get("source_node_id", "")),
                        "target_node_id": str(item.get("target_node_id", "")),
                        "edge_type": str(item.get("edge_type", "")),
                        "transfer_count": _json_safe(item.get("transfer_count")),
                        "confidence_score": _json_safe(item.get("confidence_score")),
                        "match_method": str(item.get("match_method", "")),
                    },
                    ensure_ascii=True,
                )
            )
            handle.write("\n")

    def _rel(path: Path) -> str:
        try:
            return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
        except ValueError:
            return path.resolve().as_posix()

    meta = {
        "nodes_input": _rel(nodes_path),
        "edges_input": _rel(edges_path),
        "nodes_jsonl": _rel(nodes_out),
        "edges_jsonl": _rel(edges_out),
        "defaults": {
            "max_nodes_default": int(args.max_nodes),
            "max_edges_default": int(args.max_edges),
        },
        "safe_sampling": {
            "max_nodes": int(args.max_nodes),
            "max_edges": int(args.max_edges),
            "notes": "Top edges by transfer_count, then top nodes by weighted degree; drops nodes without coordinates.",
        },
    }

    meta_out.write_text(json.dumps(meta, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"Nodes JSONL: {nodes_out}")
    print(f"Edges JSONL: {edges_out}")
    print(f"Meta JSON: {meta_out}")
    print(f"Nodes kept: {len(nodes)}")
    print(f"Edges kept: {len(edges)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
