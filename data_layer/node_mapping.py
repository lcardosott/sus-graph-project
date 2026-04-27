#!/usr/bin/env python3
"""Build graph node artifacts from SUS tabular records.

This module creates canonical node tables for facilities and municipalities,
and can map edge records to node ids for downstream graph loading.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


NODE_COLUMNS = [
    "node_id",
    "node_type",
    "name",
    "municipality_code",
    "latitude",
    "longitude",
    "capacity_beds",
    "habilitation_level",
]

FACILITY_ID_CANDIDATES = (
    "CNES",
    "PA_CODUNI",
    "CGC_HOSP",
)

FACILITY_MUNICIPALITY_CANDIDATES = (
    "MUNIC_MOV",
    "CODMUNRES",
    "MUNIC_RES",
    "PA_MUNPCN",
    "PA_UFMUN",
)

MUNICIPALITY_CANDIDATES = (
    "MUNIC_RES",
    "CODMUNRES",
    "MUNIC_MOV",
    "PA_MUNPCN",
    "PA_UFMUN",
)


@dataclass(frozen=True)
class NodeBuildMetadata:
    facility_id_field: str
    facility_municipality_field: str | None
    municipality_fields: list[str]


def _first_available(columns: pd.Index, candidates: tuple[str, ...]) -> str | None:
    available = set(columns)
    for candidate in candidates:
        if candidate in available:
            return candidate
    return None


def _normalize_string(series: pd.Series) -> pd.Series:
    return series.where(series.notna(), "").astype(str).str.strip()


def _normalize_scalar(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def _normalize_municipality_code(value: object) -> str:
    raw = _normalize_scalar(value)
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 7:
        return digits[:6]
    if len(digits) > 7:
        return digits[-6:]
    return digits


def _mode_non_empty(series: pd.Series) -> str | None:
    normalized = _normalize_string(series)
    normalized = normalized[normalized != ""]
    if normalized.empty:
        return None

    counts = normalized.value_counts()
    max_count = int(counts.iloc[0])
    top_values = sorted(counts[counts == max_count].index.tolist())
    return top_values[0]


def _empty_node_fields(size: int) -> dict[str, pd.Series]:
    return {
        "name": pd.Series([pd.NA] * size, dtype="string"),
        "latitude": pd.Series([pd.NA] * size, dtype="Float64"),
        "longitude": pd.Series([pd.NA] * size, dtype="Float64"),
        "capacity_beds": pd.Series([pd.NA] * size, dtype="Int64"),
        "habilitation_level": pd.Series([pd.NA] * size, dtype="string"),
    }


def append_missing_municipality_nodes(
    nodes: pd.DataFrame,
    municipality_codes: list[str],
) -> tuple[pd.DataFrame, int]:
    """Append municipality nodes for missing municipality codes."""
    required_columns = {"node_id", "node_type", "municipality_code"}
    missing_columns = sorted(required_columns - set(nodes.columns))
    if missing_columns:
        raise ValueError("Nodes frame is missing required columns: " + ", ".join(missing_columns))

    if not municipality_codes:
        return nodes.copy(), 0

    out = nodes.copy()
    municipality_mask = out["node_type"].astype("string").str.lower().eq("municipality")
    existing_codes = {
        _normalize_municipality_code(code)
        for code in out.loc[municipality_mask, "municipality_code"].tolist()
        if _normalize_municipality_code(code)
    }

    requested_codes = {
        _normalize_municipality_code(code)
        for code in municipality_codes
        if _normalize_municipality_code(code)
    }

    missing_codes = sorted(requested_codes - existing_codes)
    if not missing_codes:
        return out, 0

    new_nodes = pd.DataFrame(
        {
            "node_id": [f"municipality:{code}" for code in missing_codes],
            "node_type": "municipality",
            "municipality_code": missing_codes,
            **_empty_node_fields(len(missing_codes)),
        }
    )
    new_nodes = new_nodes[NODE_COLUMNS]

    out = pd.concat([out, new_nodes], ignore_index=True)
    out = out.sort_values(["node_type", "node_id"], kind="mergesort").reset_index(drop=True)
    return out, len(missing_codes)


def build_nodes_from_records(
    records: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, NodeBuildMetadata]:
    """Build canonical nodes and lookup tables from source records."""
    facility_id_field = _first_available(records.columns, FACILITY_ID_CANDIDATES)
    if facility_id_field is None:
        raise ValueError(
            "No facility identifier field found. Expected one of: "
            + ", ".join(FACILITY_ID_CANDIDATES)
        )

    facility_municipality_field = _first_available(records.columns, FACILITY_MUNICIPALITY_CANDIDATES)
    municipality_fields = [field for field in MUNICIPALITY_CANDIDATES if field in records.columns]

    normalized = records.copy()
    normalized["__facility_id"] = _normalize_string(normalized[facility_id_field])
    normalized = normalized[normalized["__facility_id"] != ""]
    if normalized.empty:
        raise ValueError("No non-empty facility identifiers found in records")

    if facility_municipality_field is not None:
        normalized["__facility_municipality"] = _normalize_string(normalized[facility_municipality_field])
        grouped = (
            normalized.groupby("__facility_id", sort=False)["__facility_municipality"]
            .apply(_mode_non_empty)
            .reset_index()
        )
        grouped = grouped.rename(columns={"__facility_municipality": "municipality_code"})
    else:
        grouped = normalized[["__facility_id"]].drop_duplicates().copy()
        grouped["municipality_code"] = pd.NA

    grouped = grouped.sort_values("__facility_id", kind="mergesort").reset_index(drop=True)
    grouped["municipality_code"] = _normalize_string(grouped["municipality_code"])
    grouped.loc[grouped["municipality_code"] == "", "municipality_code"] = pd.NA

    facility_nodes = pd.DataFrame(
        {
            "node_id": "facility:" + grouped["__facility_id"],
            "node_type": "facility",
            "municipality_code": grouped["municipality_code"],
            **_empty_node_fields(len(grouped)),
        }
    )
    facility_nodes = facility_nodes[NODE_COLUMNS]

    municipality_sources: list[pd.Series] = []
    all_municipality_fields = list(dict.fromkeys(municipality_fields + ([facility_municipality_field] if facility_municipality_field else [])))
    for field in all_municipality_fields:
        municipality_sources.append(_normalize_string(records[field]))

    if municipality_sources:
        municipality_codes = pd.concat(municipality_sources, ignore_index=True)
        municipality_codes = municipality_codes[municipality_codes != ""].drop_duplicates().sort_values(kind="mergesort")
        municipality_codes = municipality_codes.reset_index(drop=True)
    else:
        municipality_codes = pd.Series([], dtype="string")

    municipality_nodes = pd.DataFrame(
        {
            "node_id": "municipality:" + municipality_codes,
            "node_type": "municipality",
            "municipality_code": municipality_codes,
            **_empty_node_fields(len(municipality_codes)),
        }
    )
    municipality_nodes = municipality_nodes[NODE_COLUMNS]

    nodes = (
        pd.concat([facility_nodes, municipality_nodes], ignore_index=True)
        .sort_values(["node_type", "node_id"], kind="mergesort")
        .reset_index(drop=True)
    )

    facility_lookup = grouped.rename(columns={"__facility_id": "location_id"})
    facility_lookup["node_id"] = "facility:" + facility_lookup["location_id"]
    facility_lookup = facility_lookup[["location_id", "node_id", "municipality_code"]]

    municipality_lookup = pd.DataFrame(
        {
            "location_id": municipality_codes,
            "node_id": "municipality:" + municipality_codes,
        }
    )

    metadata = NodeBuildMetadata(
        facility_id_field=facility_id_field,
        facility_municipality_field=facility_municipality_field,
        municipality_fields=all_municipality_fields,
    )

    return nodes, facility_lookup, municipality_lookup, metadata


def map_edges_to_node_ids(edges: pd.DataFrame, known_nodes: pd.DataFrame | None = None) -> pd.DataFrame:
    """Add source_node_id and target_node_id columns to edge records."""
    required_columns = {"source_facility_id", "target_location_id", "target_location_type"}
    missing = sorted(required_columns - set(edges.columns))
    if missing:
        raise ValueError("Missing required edge columns: " + ", ".join(missing))

    mapped = edges.copy()
    source_id = _normalize_string(mapped["source_facility_id"])
    target_id = _normalize_string(mapped["target_location_id"])
    target_type = _normalize_string(mapped["target_location_type"]).str.lower()

    mapped["source_node_id"] = "facility:" + source_id
    mapped["target_node_id"] = "facility:" + target_id
    municipality_mask = target_type == "municipality"
    mapped.loc[municipality_mask, "target_node_id"] = "municipality:" + target_id[municipality_mask]

    if known_nodes is not None:
        known_ids = set(_normalize_string(known_nodes["node_id"]))
        mapped["source_node_exists"] = mapped["source_node_id"].isin(known_ids)
        mapped["target_node_exists"] = mapped["target_node_id"].isin(known_ids)

    return mapped


def summarize_edge_node_coverage(mapped_edges: pd.DataFrame) -> dict[str, int | float]:
    """Summarize how many mapped edge node ids are covered by known nodes."""
    required_columns = {"source_node_exists", "target_node_exists"}
    missing = sorted(required_columns - set(mapped_edges.columns))
    if missing:
        raise ValueError("Missing node coverage columns in mapped edges: " + ", ".join(missing))

    edge_rows = len(mapped_edges)
    source_missing = int((~mapped_edges["source_node_exists"]).sum())
    target_missing = int((~mapped_edges["target_node_exists"]).sum())

    return {
        "edge_rows": int(edge_rows),
        "source_node_missing": source_missing,
        "target_node_missing": target_missing,
        "source_coverage_ratio": 0.0 if edge_rows == 0 else round((edge_rows - source_missing) / edge_rows, 6),
        "target_coverage_ratio": 0.0 if edge_rows == 0 else round((edge_rows - target_missing) / edge_rows, 6),
    }
