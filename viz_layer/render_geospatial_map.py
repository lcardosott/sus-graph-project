#!/usr/bin/env python3
"""Render geospatial flow map with background tiles using Folium."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import folium
from folium.plugins import FastMarkerCluster
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render geospatial graph map from nodes and edges CSV files.")
    parser.add_argument("--nodes-input", required=True, help="Input nodes CSV path with latitude/longitude.")
    parser.add_argument("--edges-input", required=True, help="Input edges CSV path.")
    parser.add_argument("--html-output", required=True, help="Output HTML map path.")
    parser.add_argument("--summary-output", help="Optional JSON summary output path.")
    parser.add_argument("--delimiter", default=";", help="CSV delimiter.")
    parser.add_argument("--max-node-markers", type=int, default=5000, help="Maximum node markers rendered.")
    parser.add_argument("--max-edge-lines", type=int, default=3000, help="Maximum edge lines rendered.")
    parser.add_argument("--min-transfer-count", type=float, default=1.0, help="Minimum transfer_count to draw an edge line.")
    parser.add_argument("--zoom-start", type=int, default=7, help="Initial zoom level.")
    parser.add_argument("--cluster-markers", action="store_true", help="Render nodes using FastMarkerCluster for large datasets.")
    return parser.parse_args()


def _resolve_edge_endpoints(edges: pd.DataFrame) -> pd.DataFrame:
    if {"source_node_id", "target_node_id"}.issubset(edges.columns):
        out = edges.copy()
        out["source_node_id"] = out["source_node_id"].astype(str).str.strip()
        out["target_node_id"] = out["target_node_id"].astype(str).str.strip()
        return out

    required = {"source_facility_id", "target_location_id", "target_location_type"}
    missing = sorted(required - set(edges.columns))
    if missing:
        raise ValueError("Edge file missing columns for endpoint resolution: " + ", ".join(missing))

    out = edges.copy()
    source = out["source_facility_id"].fillna("").astype(str).str.strip()
    target = out["target_location_id"].fillna("").astype(str).str.strip()
    target_type = out["target_location_type"].fillna("").astype(str).str.strip().str.lower()

    out["source_node_id"] = "facility:" + source
    out["target_node_id"] = "facility:" + target
    municipality_mask = target_type.eq("municipality")
    out.loc[municipality_mask, "target_node_id"] = "municipality:" + target[municipality_mask]
    return out


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def main() -> int:
    args = parse_args()

    nodes_input = Path(args.nodes_input).resolve()
    edges_input = Path(args.edges_input).resolve()
    html_output = Path(args.html_output).resolve()

    if not nodes_input.exists():
        print(f"Nodes input not found: {nodes_input}")
        return 2
    if not edges_input.exists():
        print(f"Edges input not found: {edges_input}")
        return 2

    nodes = pd.read_csv(nodes_input, sep=args.delimiter, encoding="utf-8-sig", dtype=str)
    edges = pd.read_csv(edges_input, sep=args.delimiter, encoding="utf-8-sig", dtype=str)

    nodes["latitude"] = pd.to_numeric(nodes.get("latitude"), errors="coerce")
    nodes["longitude"] = pd.to_numeric(nodes.get("longitude"), errors="coerce")

    nodes_geo = nodes[nodes["latitude"].notna() & nodes["longitude"].notna()].copy()
    if nodes_geo.empty:
        print("No nodes with valid coordinates were found.")
        return 2

    center_lat = float(nodes_geo["latitude"].mean())
    center_lon = float(nodes_geo["longitude"].mean())

    fmap = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=args.zoom_start,
        tiles="CartoDB positron",
        control_scale=True,
    )

    municipality_layer = folium.FeatureGroup(name="Municipalities", show=True)
    facility_layer = folium.FeatureGroup(name="Facilities", show=True)
    edge_layer = folium.FeatureGroup(name="Flows", show=True)

    # Render nodes.
    node_rows = nodes_geo.head(args.max_node_markers)
    municipality_count = 0
    facility_count = 0
    municipality_points: list[list[float]] = []
    facility_points: list[list[float]] = []

    for _, row in node_rows.iterrows():
        node_id = str(row.get("node_id", "")).strip()
        node_type = str(row.get("node_type", "unknown")).strip().lower()
        name = str(row.get("name", "")).strip() or node_id
        municipality_code = str(row.get("municipality_code", "")).strip()

        popup = (
            f"<b>{name}</b><br>"
            f"node_id: {node_id}<br>"
            f"node_type: {node_type}<br>"
            f"municipality_code: {municipality_code}"
        )

        lat = float(row["latitude"])
        lon = float(row["longitude"])

        if node_type == "municipality":
            municipality_count += 1
            if args.cluster_markers:
                municipality_points.append([lat, lon])
            else:
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=4,
                    color="#f77f00",
                    fill=True,
                    fill_opacity=0.75,
                    weight=1,
                    popup=folium.Popup(popup, max_width=300),
                ).add_to(municipality_layer)
        else:
            facility_count += 1
            if args.cluster_markers:
                facility_points.append([lat, lon])
            else:
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=3,
                    color="#1d3557",
                    fill=True,
                    fill_opacity=0.85,
                    weight=1,
                    popup=folium.Popup(popup, max_width=300),
                ).add_to(facility_layer)

    # Render edges.
    edge_rows = _resolve_edge_endpoints(edges)
    edge_rows["transfer_count"] = pd.to_numeric(edge_rows.get("transfer_count"), errors="coerce").fillna(1.0)
    edge_rows = edge_rows[edge_rows["transfer_count"] >= args.min_transfer_count]
    edge_rows = edge_rows.sort_values("transfer_count", ascending=False, kind="mergesort").head(args.max_edge_lines)

    node_coord = nodes_geo.set_index("node_id")[["latitude", "longitude", "name", "node_type"]]
    edges_drawn = 0
    edges_missing_coordinates = 0

    for _, row in edge_rows.iterrows():
        source_id = str(row.get("source_node_id", "")).strip()
        target_id = str(row.get("target_node_id", "")).strip()
        if not source_id or not target_id:
            continue

        if source_id not in node_coord.index or target_id not in node_coord.index:
            edges_missing_coordinates += 1
            continue

        source = node_coord.loc[source_id]
        target = node_coord.loc[target_id]

        transfer_count = _safe_float(row.get("transfer_count"), 1.0)
        confidence = _safe_float(row.get("confidence_score"), 0.0)
        weight = max(1.0, min(8.0, 1.0 + transfer_count * 0.35))

        popup = (
            f"<b>{source_id} -> {target_id}</b><br>"
            f"transfer_count: {transfer_count}<br>"
            f"confidence_score: {round(confidence, 4)}"
        )

        folium.PolyLine(
            locations=[
                [float(source["latitude"]), float(source["longitude"])],
                [float(target["latitude"]), float(target["longitude"])],
            ],
            color="#c1121f",
            weight=weight,
            opacity=0.55,
            popup=folium.Popup(popup, max_width=320),
        ).add_to(edge_layer)
        edges_drawn += 1

    if args.cluster_markers:
        if municipality_points:
            FastMarkerCluster(municipality_points, name="Municipalities").add_to(fmap)
        if facility_points:
            FastMarkerCluster(facility_points, name="Facilities").add_to(fmap)
    else:
        municipality_layer.add_to(fmap)
        facility_layer.add_to(fmap)

    edge_layer.add_to(fmap)
    folium.LayerControl(collapsed=False).add_to(fmap)

    html_output.parent.mkdir(parents=True, exist_ok=True)
    fmap.save(str(html_output))

    summary = {
        "nodes_input": str(nodes_input),
        "edges_input": str(edges_input),
        "html_output": str(html_output),
        "map_center": {"latitude": center_lat, "longitude": center_lon},
        "nodes_with_coordinates": int(len(nodes_geo)),
        "municipality_markers": int(municipality_count),
        "facility_markers": int(facility_count),
        "edges_considered": int(len(edge_rows)),
        "edges_drawn": int(edges_drawn),
        "edges_missing_coordinates": int(edges_missing_coordinates),
        "cluster_markers": bool(args.cluster_markers),
    }

    if args.summary_output:
        summary_output = Path(args.summary_output).resolve()
        summary_output.parent.mkdir(parents=True, exist_ok=True)
        summary_output.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"Map nodes with coordinates: {len(nodes_geo)}")
    print(f"Map edges drawn: {edges_drawn}")
    print(f"Map HTML written: {html_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
