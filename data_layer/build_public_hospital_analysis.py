#!/usr/bin/env python3
"""Build public-hospital analytical tables from existing 2021 artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

try:
    from filter_engine.spatial_filters import haversine_km
except ModuleNotFoundError:
    workspace_root = Path(__file__).resolve().parent.parent
    if str(workspace_root) not in sys.path:
        sys.path.insert(0, str(workspace_root))
    from filter_engine.spatial_filters import haversine_km  # type: ignore


PROFILE_COLUMNS = [
    "CODMUNRES",
    "CNES",
    "DIAG_PRINC",
    "IDADE",
    "SEXO",
    "DIAS_PERM",
    "MARCA_UTI",
    "PROC_REA",
    "VAL_TOT",
    "RACA_COR",
    "IS_TRANSFER_REGULATED",
    "IS_DEATH",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build public-hospital analytical tables.")
    parser.add_argument("--year", type=int, default=2021)
    parser.add_argument("--curated-root", default="data_layer/curated/parquet/sih")
    parser.add_argument("--nodes-input", default="data_layer/reports/batches/sih_br_2021_nodes.csv")
    parser.add_argument("--edges-input", default="data_layer/reports/batches/sih_br_2021_edges.csv")
    parser.add_argument("--public-hospitals-input", default="data_layer/reference/catalog/cnes_br_2101_public_hospitals.csv")
    parser.add_argument("--out-dir", default="data_layer/reports/analysis")
    parser.add_argument("--prefix", default="2021_public_hospitals")
    parser.add_argument("--delimiter", default=";")
    return parser.parse_args()


def normalize_cnes(value: object) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    return digits[-7:].zfill(7)


def normalize_municipality(value: object) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 6:
        return digits[:6]
    return digits


def node_id_from_cnes(value: object) -> str:
    cnes = normalize_cnes(value)
    return f"facility:{cnes}" if cnes else ""


def node_id_from_municipality(value: object) -> str:
    code = normalize_municipality(value)
    return f"municipality:{code}" if code else ""


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _mode_or_empty(series: pd.Series) -> str:
    values = series.dropna().astype(str).str.strip()
    values = values[values != ""]
    if values.empty:
        return ""
    return str(values.mode().iloc[0])


def read_curated_year(curated_root: Path, year: int, columns: list[str]) -> pd.DataFrame:
    paths = sorted((curated_root / f"year={year}").glob("month=*/uf=*/*.parquet"))
    if not paths:
        raise FileNotFoundError(f"No curated parquet files found under {curated_root / f'year={year}'}")

    frames: list[pd.DataFrame] = []
    for path in paths:
        month = path.parent.parent.name.split("=", 1)[1]
        uf = path.parent.name.split("=", 1)[1]
        frame = pd.read_parquet(path, columns=columns)
        frame["month"] = int(month)
        frame["uf"] = uf
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def iter_curated_year(curated_root: Path, year: int, columns: list[str]):
    paths = sorted((curated_root / f"year={year}").glob("month=*/uf=*/*.parquet"))
    if not paths:
        raise FileNotFoundError(f"No curated parquet files found under {curated_root / f'year={year}'}")
    for path in paths:
        frame = pd.read_parquet(path, columns=columns)
        yield path, frame


def build_facility_panel(records: pd.DataFrame, public_nodes: set[str]) -> pd.DataFrame:
    records = records.copy()
    records["facility_node_id"] = records["CNES"].map(node_id_from_cnes)
    records = records[records["facility_node_id"].isin(public_nodes)].copy()
    records["age"] = _safe_numeric(records["IDADE"])
    records["stay_days"] = _safe_numeric(records["DIAS_PERM"])
    records["value_total"] = _safe_numeric(records["VAL_TOT"]).fillna(0.0)
    records["is_death"] = records["IS_DEATH"].astype(bool)
    records["is_transfer_regulated"] = records["IS_TRANSFER_REGULATED"].astype(bool)
    records["has_icu_marker"] = records["MARCA_UTI"].fillna("").astype(str).str.strip().ne("").astype(int)
    records["icd_chapter"] = records["DIAG_PRINC"].fillna("").astype(str).str.strip().str[:1].str.upper()

    grouped = records.groupby("facility_node_id", dropna=False)
    panel = grouped.agg(
        admissions=("facility_node_id", "size"),
        deaths=("is_death", "sum"),
        regulated_transfer_exits=("is_transfer_regulated", "sum"),
        mean_age=("age", "mean"),
        mean_stay_days=("stay_days", "mean"),
        median_stay_days=("stay_days", "median"),
        icu_marker_count=("has_icu_marker", "sum"),
        total_value=("value_total", "sum"),
        dominant_icd_chapter=("icd_chapter", _mode_or_empty),
        dominant_sex=("SEXO", _mode_or_empty),
        dominant_race_color=("RACA_COR", _mode_or_empty),
    ).reset_index()

    panel["death_rate"] = panel["deaths"] / panel["admissions"]
    panel["regulated_transfer_exit_rate"] = panel["regulated_transfer_exits"] / panel["admissions"]
    panel["icu_marker_rate"] = panel["icu_marker_count"] / panel["admissions"]
    panel["mean_value_per_admission"] = panel["total_value"] / panel["admissions"]
    return panel.round(
        {
            "mean_age": 3,
            "mean_stay_days": 3,
            "median_stay_days": 3,
            "total_value": 2,
            "death_rate": 6,
            "regulated_transfer_exit_rate": 6,
            "icu_marker_rate": 6,
            "mean_value_per_admission": 2,
        }
    )


def build_facility_panel_from_curated(curated_root: Path, year: int, public_nodes: set[str]) -> tuple[pd.DataFrame, int]:
    partials: list[pd.DataFrame] = []
    chapter_counts: list[pd.DataFrame] = []
    sex_counts: list[pd.DataFrame] = []
    race_counts: list[pd.DataFrame] = []
    records_rows = 0

    for _, records in iter_curated_year(curated_root, year, PROFILE_COLUMNS):
        records_rows += len(records)
        records = records.copy()
        records["facility_node_id"] = records["CNES"].map(node_id_from_cnes)
        records = records[records["facility_node_id"].isin(public_nodes)].copy()
        if records.empty:
            continue

        records["age"] = _safe_numeric(records["IDADE"])
        records["stay_days"] = _safe_numeric(records["DIAS_PERM"])
        records["value_total"] = _safe_numeric(records["VAL_TOT"]).fillna(0.0)
        records["is_death"] = records["IS_DEATH"].astype(bool)
        records["is_transfer_regulated"] = records["IS_TRANSFER_REGULATED"].astype(bool)
        records["has_icu_marker"] = records["MARCA_UTI"].fillna("").astype(str).str.strip().ne("").astype(int)
        records["icd_chapter"] = records["DIAG_PRINC"].fillna("").astype(str).str.strip().str[:1].str.upper()

        grouped = records.groupby("facility_node_id", dropna=False)
        partials.append(
            grouped.agg(
                admissions=("facility_node_id", "size"),
                deaths=("is_death", "sum"),
                regulated_transfer_exits=("is_transfer_regulated", "sum"),
                age_sum=("age", "sum"),
                age_count=("age", "count"),
                stay_sum=("stay_days", "sum"),
                stay_count=("stay_days", "count"),
                median_stay_days=("stay_days", "median"),
                icu_marker_count=("has_icu_marker", "sum"),
                total_value=("value_total", "sum"),
            ).reset_index()
        )
        chapter_counts.append(records.groupby(["facility_node_id", "icd_chapter"], dropna=False).size().reset_index(name="count"))
        sex_counts.append(records.groupby(["facility_node_id", "SEXO"], dropna=False).size().reset_index(name="count"))
        race_counts.append(records.groupby(["facility_node_id", "RACA_COR"], dropna=False).size().reset_index(name="count"))

    if not partials:
        return pd.DataFrame(columns=["facility_node_id"]), records_rows

    combined = pd.concat(partials, ignore_index=True)
    panel = combined.groupby("facility_node_id", dropna=False).agg(
        admissions=("admissions", "sum"),
        deaths=("deaths", "sum"),
        regulated_transfer_exits=("regulated_transfer_exits", "sum"),
        age_sum=("age_sum", "sum"),
        age_count=("age_count", "sum"),
        stay_sum=("stay_sum", "sum"),
        stay_count=("stay_count", "sum"),
        median_stay_days=("median_stay_days", "median"),
        icu_marker_count=("icu_marker_count", "sum"),
        total_value=("total_value", "sum"),
    ).reset_index()

    def dominant(count_frames: list[pd.DataFrame], column: str, out_column: str) -> pd.DataFrame:
        if not count_frames:
            return pd.DataFrame(columns=["facility_node_id", out_column])
        counts = pd.concat(count_frames, ignore_index=True)
        counts[column] = counts[column].fillna("").astype(str).str.strip()
        counts = counts[counts[column] != ""]
        if counts.empty:
            return pd.DataFrame(columns=["facility_node_id", out_column])
        counts = counts.groupby(["facility_node_id", column], as_index=False)["count"].sum()
        idx = counts.sort_values(["facility_node_id", "count"], ascending=[True, False]).groupby("facility_node_id").head(1).index
        return counts.loc[idx, ["facility_node_id", column]].rename(columns={column: out_column})

    panel = panel.merge(dominant(chapter_counts, "icd_chapter", "dominant_icd_chapter"), on="facility_node_id", how="left")
    panel = panel.merge(dominant(sex_counts, "SEXO", "dominant_sex"), on="facility_node_id", how="left")
    panel = panel.merge(dominant(race_counts, "RACA_COR", "dominant_race_color"), on="facility_node_id", how="left")
    panel["mean_age"] = panel["age_sum"] / panel["age_count"].replace({0: np.nan})
    panel["mean_stay_days"] = panel["stay_sum"] / panel["stay_count"].replace({0: np.nan})
    panel["death_rate"] = panel["deaths"] / panel["admissions"]
    panel["regulated_transfer_exit_rate"] = panel["regulated_transfer_exits"] / panel["admissions"]
    panel["icu_marker_rate"] = panel["icu_marker_count"] / panel["admissions"]
    panel["mean_value_per_admission"] = panel["total_value"] / panel["admissions"]
    panel = panel.drop(columns=["age_sum", "age_count", "stay_sum", "stay_count"])
    return panel.round(
        {
            "mean_age": 3,
            "mean_stay_days": 3,
            "median_stay_days": 3,
            "total_value": 2,
            "death_rate": 6,
            "regulated_transfer_exit_rate": 6,
            "icu_marker_rate": 6,
            "mean_value_per_admission": 2,
        }
    ), records_rows


def _add_endpoint_coordinates(edges: pd.DataFrame, nodes: pd.DataFrame) -> pd.DataFrame:
    node_lookup = nodes.set_index("node_id")[["latitude", "longitude", "municipality_code"]].copy()
    node_lookup["latitude"] = _safe_numeric(node_lookup["latitude"])
    node_lookup["longitude"] = _safe_numeric(node_lookup["longitude"])

    out = edges.merge(
        node_lookup.rename(columns={"latitude": "source_latitude", "longitude": "source_longitude", "municipality_code": "source_municipality_code"}),
        left_on="source_node_id",
        right_index=True,
        how="left",
    )
    out = out.merge(
        node_lookup.rename(columns={"latitude": "target_latitude", "longitude": "target_longitude", "municipality_code": "target_municipality_code"}),
        left_on="target_node_id",
        right_index=True,
        how="left",
    )

    def compute_distance(row: pd.Series) -> float | np.nan:
        values = [row["source_latitude"], row["source_longitude"], row["target_latitude"], row["target_longitude"]]
        if any(pd.isna(value) for value in values):
            return np.nan
        return haversine_km(float(row["source_latitude"]), float(row["source_longitude"]), float(row["target_latitude"]), float(row["target_longitude"]))

    out["distance_km"] = _safe_numeric(out.get("distance_km", pd.Series(index=out.index)))
    missing_distance = out["distance_km"].isna()
    if missing_distance.any():
        out.loc[missing_distance, "distance_km"] = out.loc[missing_distance].apply(compute_distance, axis=1)
    out["distance_band"] = pd.cut(
        out["distance_km"],
        bins=[-np.inf, 50, 100, 200, np.inf],
        labels=["0-50", "50-100", "100-200", "200+"],
    ).astype(str)
    return out


def build_edge_analysis(
    edges: pd.DataFrame,
    nodes: pd.DataFrame,
    public_nodes: set[str],
    facility_panel: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    edges = edges.copy()
    edges["transfer_count"] = _safe_numeric(edges["transfer_count"]).fillna(1.0)
    source_public = edges["source_node_id"].isin(public_nodes)
    target_public = edges["target_node_id"].isin(public_nodes)
    is_residence = edges["edge_type"].eq("residence")
    is_transfer = edges["edge_type"].eq("transfer")
    scoped_edges = edges[(is_residence & target_public) | (is_transfer & source_public & target_public)].copy()
    scoped_edges["public_hospital_scope"] = True
    scoped_edges = _add_endpoint_coordinates(scoped_edges, nodes)
    scoped_edges["same_municipality"] = scoped_edges["source_municipality_code"].astype(str).eq(scoped_edges["target_municipality_code"].astype(str))
    scoped_edges["source_uf"] = scoped_edges["source_municipality_code"].astype(str).str[:2]
    scoped_edges["target_uf"] = scoped_edges["target_municipality_code"].astype(str).str[:2]
    scoped_edges["same_uf_prefix"] = scoped_edges["source_uf"].eq(scoped_edges["target_uf"])

    panel_cols = [
        "facility_node_id",
        "admissions",
        "deaths",
        "death_rate",
        "mean_stay_days",
        "regulated_transfer_exit_rate",
        "icu_marker_rate",
        "mean_value_per_admission",
        "dominant_icd_chapter",
    ]
    target_panel = facility_panel[panel_cols].rename(columns={col: f"target_{col}" for col in panel_cols if col != "facility_node_id"})
    scoped_edges = scoped_edges.merge(target_panel, left_on="target_node_id", right_on="facility_node_id", how="left").drop(columns=["facility_node_id"], errors="ignore")

    residence = scoped_edges[scoped_edges["edge_type"].eq("residence")].copy()
    transfer = scoped_edges[scoped_edges["edge_type"].eq("transfer")].copy()

    if not residence.empty:
        municipality_totals = residence.groupby("source_node_id")["transfer_count"].transform("sum")
        residence["municipality_flow_share"] = residence["transfer_count"] / municipality_totals
        residence["long_distance_dependency"] = residence["distance_km"].ge(100).astype(int) * residence["transfer_count"]
    if not transfer.empty:
        transfer["self_transfer_edge"] = transfer["source_node_id"].eq(transfer["target_node_id"])

    return scoped_edges, residence, transfer


def build_node_analysis(
    nodes: pd.DataFrame,
    public_ref: pd.DataFrame,
    facility_panel: pd.DataFrame,
    public_nodes: set[str],
) -> pd.DataFrame:
    out = nodes.copy()
    out["is_public_hospital"] = out["node_id"].isin(public_nodes)
    out["analysis_scope"] = np.where(out["node_type"].eq("municipality") | out["is_public_hospital"], "included", "excluded_non_public_hospital")
    cnes_ref = public_ref.copy()
    cnes_ref["node_id"] = cnes_ref["cnes"].map(node_id_from_cnes)
    ref_cols = [
        "node_id",
        "is_sus_linked",
        "has_hospital_care",
        "has_hospital_beds",
        "is_public_admin",
        "is_public_hospital",
        "coordinate_source",
        "name_source",
    ]
    out = out.merge(cnes_ref[[col for col in ref_cols if col in cnes_ref.columns]], on="node_id", how="left", suffixes=("", "_ref"))
    out = out.merge(facility_panel, left_on="node_id", right_on="facility_node_id", how="left").drop(columns=["facility_node_id"], errors="ignore")
    return out


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    nodes = pd.read_csv(args.nodes_input, sep=args.delimiter, dtype=str, encoding="utf-8-sig")
    edges = pd.read_csv(args.edges_input, sep=args.delimiter, dtype=str, encoding="utf-8-sig")
    public_ref = pd.read_csv(args.public_hospitals_input, sep=args.delimiter, dtype=str, encoding="utf-8-sig")
    public_ref["node_id"] = public_ref["cnes"].map(node_id_from_cnes)
    public_nodes = set(public_ref["node_id"])

    facility_panel, records_rows = build_facility_panel_from_curated(Path(args.curated_root), args.year, public_nodes)
    edge_analysis, residence_features, transfer_features = build_edge_analysis(edges, nodes, public_nodes, facility_panel)
    node_analysis = build_node_analysis(nodes, public_ref, facility_panel, public_nodes)

    outputs = {
        "facility_panel": out_dir / f"facility_panel_{args.prefix}.csv",
        "residence_flow_features": out_dir / f"residence_flow_features_{args.prefix}.csv",
        "transfer_flow_features": out_dir / f"transfer_flow_features_{args.prefix}.csv",
        "node_analysis": out_dir / f"node_analysis_{args.prefix}.csv",
        "edge_analysis": out_dir / f"edge_analysis_{args.prefix}.csv",
        "summary": out_dir / f"summary_{args.prefix}.json",
    }
    facility_panel.to_csv(outputs["facility_panel"], sep=args.delimiter, index=False, encoding="utf-8-sig")
    residence_features.to_csv(outputs["residence_flow_features"], sep=args.delimiter, index=False, encoding="utf-8-sig")
    transfer_features.to_csv(outputs["transfer_flow_features"], sep=args.delimiter, index=False, encoding="utf-8-sig")
    node_analysis.to_csv(outputs["node_analysis"], sep=args.delimiter, index=False, encoding="utf-8-sig")
    edge_analysis.to_csv(outputs["edge_analysis"], sep=args.delimiter, index=False, encoding="utf-8-sig")

    summary = {
        "year": args.year,
        "records_rows": int(records_rows),
        "graph_nodes_input": int(len(nodes)),
        "graph_edges_input": int(len(edges)),
        "public_hospital_reference_rows": int(len(public_ref)),
        "public_hospital_nodes_in_graph": int(nodes["node_id"].isin(public_nodes).sum()),
        "facility_panel_rows": int(len(facility_panel)),
        "edge_analysis_rows": int(len(edge_analysis)),
        "residence_flow_rows": int(len(residence_features)),
        "transfer_flow_rows": int(len(transfer_features)),
        "excluded_non_public_facility_nodes": int(((node_analysis["node_type"] == "facility") & (~node_analysis["is_public_hospital"])).sum()),
        "outputs": {key: str(path) for key, path in outputs.items()},
    }
    outputs["summary"].write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
