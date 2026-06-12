#!/usr/bin/env python3
"""Regional access and mismatch analysis for public-hospital flow edges."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze public-hospital flows by official health region.")
    parser.add_argument("--edge-analysis", default="data_layer/reports/analysis/edge_analysis_2021_public_hospitals.csv")
    parser.add_argument("--node-analysis", default="data_layer/reports/analysis/node_analysis_2021_public_hospitals.csv")
    parser.add_argument("--municipality-regions", default="data_layer/reference/catalog/municipality_health_regions_cnes_2101.csv")
    parser.add_argument("--out-dir", default="algorithm_layer/reports")
    parser.add_argument("--prefix", default="regional_2021_public_hospitals")
    parser.add_argument("--delimiter", default=";")
    parser.add_argument("--distance-bands-km", default="25,50")
    parser.add_argument("--min-residence-count", type=float, default=5.0)
    parser.add_argument("--min-transfer-count", type=float, default=2.0)
    return parser.parse_args()


def parse_float_list(value: str) -> list[float]:
    values = [float(item.strip()) for item in value.split(",") if item.strip()]
    if not values:
        raise ValueError("distance-bands-km must contain at least one value")
    return sorted(set(values))


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        if not rows:
            return
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def load_inputs(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    edges = pd.read_csv(args.edge_analysis, sep=args.delimiter, dtype=str, encoding="utf-8-sig", low_memory=False)
    nodes = pd.read_csv(args.node_analysis, sep=args.delimiter, dtype=str, encoding="utf-8-sig", low_memory=False)
    regions = pd.read_csv(args.municipality_regions, sep=args.delimiter, dtype=str, encoding="utf-8-sig")
    edges["transfer_count"] = numeric(edges["transfer_count"]).fillna(0.0)
    edges["distance_km"] = numeric(edges["distance_km"])
    return edges, nodes, regions


def apply_region_join(edges: pd.DataFrame, regions: pd.DataFrame) -> pd.DataFrame:
    region_cols = ["municipality_code", "health_region_code", "health_region_id", "region_conflict"]
    region_lookup = regions[region_cols].copy()
    out = edges.merge(
        region_lookup.rename(
            columns={
                "municipality_code": "source_municipality_code",
                "health_region_code": "source_health_region_code",
                "health_region_id": "source_health_region_id",
                "region_conflict": "source_region_conflict",
            }
        ),
        on="source_municipality_code",
        how="left",
    )
    out = out.merge(
        region_lookup.rename(
            columns={
                "municipality_code": "target_municipality_code",
                "health_region_code": "target_health_region_code",
                "health_region_id": "target_health_region_id",
                "region_conflict": "target_region_conflict",
            }
        ),
        on="target_municipality_code",
        how="left",
    )
    out["same_health_region"] = out["source_health_region_id"].fillna("").eq(out["target_health_region_id"].fillna(""))
    out["known_health_regions"] = out["source_health_region_id"].fillna("").ne("") & out["target_health_region_id"].fillna("").ne("")
    return out


def threshold_edges(edges: pd.DataFrame, min_residence: float, min_transfer: float, min_distance: float) -> pd.DataFrame:
    keep_count = (
        edges["edge_type"].eq("residence") & edges["transfer_count"].ge(min_residence)
    ) | (
        edges["edge_type"].eq("transfer") & edges["transfer_count"].ge(min_transfer)
    )
    return edges[keep_count & edges["distance_km"].ge(min_distance)].copy()


def weighted_average(values: pd.Series, weights: pd.Series) -> float:
    valid = values.notna() & weights.notna() & weights.gt(0)
    if not valid.any():
        return 0.0
    return float((values[valid] * weights[valid]).sum() / weights[valid].sum())


def build_overall_rows(edges: pd.DataFrame, distance_bands: list[float], min_residence: float, min_transfer: float) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for distance_min in distance_bands:
        filtered = threshold_edges(edges, min_residence, min_transfer, distance_min)
        for edge_type, group in filtered.groupby("edge_type", dropna=False):
            total_flow = float(group["transfer_count"].sum())
            cross_region_flow = float(group.loc[group["known_health_regions"] & ~group["same_health_region"], "transfer_count"].sum())
            same_region_flow = float(group.loc[group["known_health_regions"] & group["same_health_region"], "transfer_count"].sum())
            unknown_region_flow = float(group.loc[~group["known_health_regions"], "transfer_count"].sum())
            cross_uf_flow = float(group.loc[~group["source_uf"].astype(str).eq(group["target_uf"].astype(str)), "transfer_count"].sum())
            rows.append(
                {
                    "distance_min_km": distance_min,
                    "edge_type": edge_type,
                    "edges": len(group),
                    "total_flow": round(total_flow, 6),
                    "same_health_region_flow": round(same_region_flow, 6),
                    "cross_health_region_flow": round(cross_region_flow, 6),
                    "cross_health_region_share": round(cross_region_flow / total_flow, 6) if total_flow else 0.0,
                    "unknown_health_region_flow": round(unknown_region_flow, 6),
                    "cross_uf_flow": round(cross_uf_flow, 6),
                    "cross_uf_share": round(cross_uf_flow / total_flow, 6) if total_flow else 0.0,
                    "weighted_mean_distance_km": round(weighted_average(group["distance_km"], group["transfer_count"]), 6),
                    "median_edge_distance_km": round(float(group["distance_km"].median()), 6) if not group.empty else 0.0,
                }
            )
    return rows


def build_region_mismatch_rows(edges: pd.DataFrame, distance_bands: list[float], min_residence: float, min_transfer: float) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for distance_min in distance_bands:
        filtered = threshold_edges(edges, min_residence, min_transfer, distance_min)
        known = filtered[filtered["known_health_regions"]].copy()
        region_totals = known.groupby(["edge_type", "source_health_region_id"])["transfer_count"].sum().rename("source_region_total")
        mismatch = known[~known["same_health_region"]].copy()
        if mismatch.empty:
            continue
        grouped = mismatch.groupby(["edge_type", "source_health_region_id", "target_health_region_id"], dropna=False).agg(
            flow=("transfer_count", "sum"),
            edges=("transfer_count", "size"),
            weighted_mean_distance_km=("distance_km", lambda series: weighted_average(series, mismatch.loc[series.index, "transfer_count"])),
        ).reset_index()
        grouped = grouped.merge(region_totals, on=["edge_type", "source_health_region_id"], how="left")
        grouped["distance_min_km"] = distance_min
        grouped["share_of_source_region_flow"] = grouped["flow"] / grouped["source_region_total"]
        grouped = grouped.sort_values(["distance_min_km", "edge_type", "flow"], ascending=[True, True, False])
        for _, row in grouped.iterrows():
            rows.append(
                {
                    "distance_min_km": row["distance_min_km"],
                    "edge_type": row["edge_type"],
                    "source_health_region_id": row["source_health_region_id"],
                    "target_health_region_id": row["target_health_region_id"],
                    "flow": round(float(row["flow"]), 6),
                    "edges": int(row["edges"]),
                    "source_region_total": round(float(row["source_region_total"]), 6),
                    "share_of_source_region_flow": round(float(row["share_of_source_region_flow"]), 6),
                    "weighted_mean_distance_km": round(float(row["weighted_mean_distance_km"]), 6),
                }
            )
    return rows


def build_municipality_dependency_rows(edges: pd.DataFrame, nodes: pd.DataFrame, distance_bands: list[float], min_residence: float) -> list[dict[str, object]]:
    node_names = nodes.set_index("node_id")["name"].to_dict() if "name" in nodes.columns else {}
    rows: list[dict[str, object]] = []
    residence = edges[edges["edge_type"].eq("residence")].copy()
    for distance_min in distance_bands:
        filtered = residence[residence["transfer_count"].ge(min_residence) & residence["distance_km"].ge(distance_min)].copy()
        for source_node_id, group in filtered.groupby("source_node_id", dropna=False):
            total_flow = float(group["transfer_count"].sum())
            top_idx = group["transfer_count"].idxmax()
            top = group.loc[top_idx]
            cross_region_flow = float(group.loc[group["known_health_regions"] & ~group["same_health_region"], "transfer_count"].sum())
            cross_uf_flow = float(group.loc[~group["source_uf"].astype(str).eq(group["target_uf"].astype(str)), "transfer_count"].sum())
            rows.append(
                {
                    "distance_min_km": distance_min,
                    "source_node_id": source_node_id,
                    "source_name": node_names.get(source_node_id, ""),
                    "source_municipality_code": group["source_municipality_code"].iloc[0],
                    "source_health_region_id": group["source_health_region_id"].fillna("").iloc[0],
                    "total_flow": round(total_flow, 6),
                    "target_facilities": int(group["target_node_id"].nunique()),
                    "top_target_node_id": top["target_node_id"],
                    "top_target_name": node_names.get(top["target_node_id"], ""),
                    "top_target_flow": round(float(top["transfer_count"]), 6),
                    "top_target_share": round(float(top["transfer_count"]) / total_flow, 6) if total_flow else 0.0,
                    "cross_health_region_flow": round(cross_region_flow, 6),
                    "cross_health_region_share": round(cross_region_flow / total_flow, 6) if total_flow else 0.0,
                    "cross_uf_flow": round(cross_uf_flow, 6),
                    "cross_uf_share": round(cross_uf_flow / total_flow, 6) if total_flow else 0.0,
                    "weighted_mean_distance_km": round(weighted_average(group["distance_km"], group["transfer_count"]), 6),
                    "max_edge_distance_km": round(float(group["distance_km"].max()), 6),
                }
            )
    return sorted(rows, key=lambda row: (row["distance_min_km"], row["cross_health_region_share"], row["top_target_share"], row["total_flow"]), reverse=True)


def analyze(args: argparse.Namespace) -> dict[str, object]:
    edges, nodes, regions = load_inputs(args)
    edges = apply_region_join(edges, regions)
    distance_bands = parse_float_list(args.distance_bands_km)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    overall_rows = build_overall_rows(edges, distance_bands, args.min_residence_count, args.min_transfer_count)
    mismatch_rows = build_region_mismatch_rows(edges, distance_bands, args.min_residence_count, args.min_transfer_count)
    dependency_rows = build_municipality_dependency_rows(edges, nodes, distance_bands, args.min_residence_count)

    outputs = {
        "overall": out_dir / f"{args.prefix}_overall.csv",
        "regional_mismatch": out_dir / f"{args.prefix}_regional_mismatch.csv",
        "municipality_dependency": out_dir / f"{args.prefix}_municipality_dependency.csv",
        "summary": out_dir / f"{args.prefix}_summary.json",
    }
    write_csv(outputs["overall"], overall_rows)
    write_csv(outputs["regional_mismatch"], mismatch_rows)
    write_csv(outputs["municipality_dependency"], dependency_rows)

    summary = {
        "edge_analysis": args.edge_analysis,
        "municipality_regions": args.municipality_regions,
        "distance_bands_km": distance_bands,
        "min_residence_count": args.min_residence_count,
        "min_transfer_count": args.min_transfer_count,
        "overall_rows": len(overall_rows),
        "regional_mismatch_rows": len(mismatch_rows),
        "municipality_dependency_rows": len(dependency_rows),
        "outputs": {key: str(value) for key, value in outputs.items()},
    }
    outputs["summary"].write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")
    return summary


def main() -> int:
    result = analyze(parse_args())
    print(json.dumps(result, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
