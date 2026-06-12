#!/usr/bin/env python3
"""Build lightweight map/dashboard layers for the final presentation UI."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build lightweight final map layers.")
    parser.add_argument("--nodes", default="data_layer/reports/analysis/node_analysis_2021_public_hospitals.csv")
    parser.add_argument("--edges", default="data_layer/reports/analysis/edge_analysis_2021_public_hospitals.csv")
    parser.add_argument("--summary", default="data_layer/reports/analysis/summary_2021_public_hospitals.json")
    parser.add_argument("--regional-overall", default="algorithm_layer/reports/regional_2021_public_hospitals_overall.csv")
    parser.add_argument("--regional-mismatch", default="algorithm_layer/reports/regional_2021_public_hospitals_regional_mismatch.csv")
    parser.add_argument("--municipality-dependency", default="algorithm_layer/reports/regional_2021_public_hospitals_municipality_dependency.csv")
    parser.add_argument("--centrality-25", default="algorithm_layer/reports/final_2021_public_hospitals_25km_centrality.csv")
    parser.add_argument("--centrality-50", default="algorithm_layer/reports/final_2021_public_hospitals_50km_centrality.csv")
    parser.add_argument("--stress-25", default="algorithm_layer/reports/final_2021_public_hospitals_25km_stress_dynamic.csv")
    parser.add_argument("--stress-50", default="algorithm_layer/reports/final_2021_public_hospitals_50km_stress_dynamic.csv")
    parser.add_argument("--communities-25", default="algorithm_layer/reports/final_2021_public_hospitals_25km_communities.csv")
    parser.add_argument("--communities-50", default="algorithm_layer/reports/final_2021_public_hospitals_50km_communities.csv")
    parser.add_argument("--summary-25", default="algorithm_layer/reports/final_2021_public_hospitals_25km_summary.json")
    parser.add_argument("--summary-50", default="algorithm_layer/reports/final_2021_public_hospitals_50km_summary.json")
    parser.add_argument("--out", default="viz_layer/reports/final_map_layers.json")
    parser.add_argument("--delimiter", default=";")
    return parser.parse_args()


def read_json(path: str) -> dict[str, object]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def clean_number(value: object) -> float | None:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if result != result:
        return None
    return result


def node_lookup(nodes: pd.DataFrame) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for _, row in nodes.iterrows():
        lat = clean_number(row.get("latitude"))
        lon = clean_number(row.get("longitude"))
        if lat is None or lon is None:
            continue
        out[str(row["node_id"])] = {
            "id": str(row["node_id"]),
            "name": str(row.get("name", "")),
            "type": str(row.get("node_type", "")),
            "municipality_code": str(row.get("municipality_code", "")),
            "lat": lat,
            "lon": lon,
            "is_public_hospital": str(row.get("is_public_hospital", "")).lower() in {"true", "1"},
        }
    return out


def edge_item(row: pd.Series, lookup: dict[str, dict[str, object]], reason: str) -> dict[str, object] | None:
    source = lookup.get(str(row["source_node_id"]))
    target = lookup.get(str(row["target_node_id"]))
    if not source or not target:
        return None
    return {
        "source": source["id"],
        "target": target["id"],
        "source_name": source["name"],
        "target_name": target["name"],
        "source_lat": source["lat"],
        "source_lon": source["lon"],
        "target_lat": target["lat"],
        "target_lon": target["lon"],
        "edge_type": str(row.get("edge_type", "")),
        "transfer_count": clean_number(row.get("transfer_count")) or 0,
        "distance_km": clean_number(row.get("distance_km")) or 0,
        "same_health_region": str(row.get("same_health_region", "")).lower() == "true",
        "reason": reason,
    }


def top_edges(edges: pd.DataFrame, lookup: dict[str, dict[str, object]], min_distance: float, limit: int) -> list[dict[str, object]]:
    scoped = edges[pd.to_numeric(edges["distance_km"], errors="coerce").ge(min_distance)].copy()
    scoped["transfer_count"] = pd.to_numeric(scoped["transfer_count"], errors="coerce").fillna(0)
    scoped = scoped.sort_values(["transfer_count", "distance_km"], ascending=[False, False]).head(limit)
    items = [edge_item(row, lookup, f"top_{int(min_distance)}km") for _, row in scoped.iterrows()]
    return [item for item in items if item]


def central_nodes(path: str, lookup: dict[str, dict[str, object]], limit: int) -> list[dict[str, object]]:
    rows = pd.read_csv(path).head(limit)
    out: list[dict[str, object]] = []
    for _, row in rows.iterrows():
        node = lookup.get(str(row["node_id"]))
        if not node:
            continue
        item = node.copy()
        item.update(
            {
                "rank": int(row["rank"]),
                "betweenness": clean_number(row["betweenness"]) or 0,
                "weighted_degree": clean_number(row["weighted_degree"]) or 0,
            }
        )
        out.append(item)
    return out


def stress_nodes(path: str, lookup: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    rows = pd.read_csv(path)
    rows = rows[pd.to_numeric(rows["step"], errors="coerce").fillna(0).gt(0)]
    out: list[dict[str, object]] = []
    for _, row in rows.iterrows():
        node = lookup.get(str(row["removed_node_id"]))
        if not node:
            continue
        item = node.copy()
        item.update(
            {
                "step": int(row["step"]),
                "path_increase_ratio": clean_number(row.get("path_increase_ratio")) or 0,
                "largest_component_share": clean_number(row.get("largest_component_share")) or 0,
            }
        )
        out.append(item)
    return out


def dependency_examples(path: str, lookup: dict[str, dict[str, object]], limit: int) -> list[dict[str, object]]:
    rows = pd.read_csv(path).head(limit)
    out: list[dict[str, object]] = []
    for _, row in rows.iterrows():
        source = lookup.get(str(row["source_node_id"]))
        target = lookup.get(str(row["top_target_node_id"]))
        if not source or not target:
            continue
        out.append(
            {
                "source": source,
                "target": target,
                "total_flow": clean_number(row.get("total_flow")) or 0,
                "top_target_share": clean_number(row.get("top_target_share")) or 0,
                "cross_health_region_share": clean_number(row.get("cross_health_region_share")) or 0,
                "weighted_mean_distance_km": clean_number(row.get("weighted_mean_distance_km")) or 0,
            }
        )
    return out


def dataframe_records(path: str, limit: int) -> list[dict[str, object]]:
    return pd.read_csv(path).head(limit).where(pd.notna(pd.read_csv(path).head(limit)), None).to_dict(orient="records")


def main() -> int:
    args = parse_args()
    nodes = pd.read_csv(args.nodes, sep=args.delimiter, dtype=str, low_memory=False)
    edges = pd.read_csv(args.edges, sep=args.delimiter, dtype=str, low_memory=False)
    lookup = node_lookup(nodes)

    payload = {
        "summary": read_json(args.summary),
        "algorithm_25km": read_json(args.summary_25),
        "algorithm_50km": read_json(args.summary_50),
        "regional_overall": pd.read_csv(args.regional_overall).to_dict(orient="records"),
        "regional_mismatch_top": dataframe_records(args.regional_mismatch, 12),
        "communities_25km_top": dataframe_records(args.communities_25, 12),
        "communities_50km_top": dataframe_records(args.communities_50, 12),
        "presets": {
            "25km": {
                "edges": top_edges(edges, lookup, 25, 2200),
                "central_nodes": central_nodes(args.centrality_25, lookup, 20),
                "stress_nodes": stress_nodes(args.stress_25, lookup),
            },
            "50km": {
                "edges": top_edges(edges, lookup, 50, 1800),
                "central_nodes": central_nodes(args.centrality_50, lookup, 20),
                "stress_nodes": stress_nodes(args.stress_50, lookup),
            },
            "dependency": {
                "examples": dependency_examples(args.municipality_dependency, lookup, 12),
            },
        },
        "source_tables": {
            "centrality_25km": args.centrality_25,
            "centrality_50km": args.centrality_50,
            "stress_25km": args.stress_25,
            "stress_50km": args.stress_50,
            "regional_overall": args.regional_overall,
            "municipality_dependency": args.municipality_dependency,
        },
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"output": str(out), "edges_25": len(payload["presets"]["25km"]["edges"]), "edges_50": len(payload["presets"]["50km"]["edges"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
