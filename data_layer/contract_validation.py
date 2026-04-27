#!/usr/bin/env python3
"""Validation helpers for data-layer node and edge contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


NODE_REQUIRED_COLUMNS = {"node_id", "node_type", "municipality_code"}
EDGE_REQUIRED_COLUMNS = {"source_node_id", "target_node_id", "transfer_count"}
VALID_NODE_TYPES = {"facility", "municipality"}


@dataclass(frozen=True)
class ValidationThresholds:
    max_missing_node_ratio: float = 0.0
    min_node_name_coverage: float = 0.9


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def validate_nodes_edges_contract(
    nodes: pd.DataFrame,
    edges: pd.DataFrame,
    thresholds: ValidationThresholds | None = None,
) -> dict[str, Any]:
    cfg = thresholds or ValidationThresholds()

    report: dict[str, Any] = {
        "passed": True,
        "errors": [],
        "warnings": [],
    }

    missing_node_columns = sorted(NODE_REQUIRED_COLUMNS - set(nodes.columns))
    missing_edge_columns = sorted(EDGE_REQUIRED_COLUMNS - set(edges.columns))

    if missing_node_columns:
        report["errors"].append("Nodes missing required columns: " + ", ".join(missing_node_columns))
    if missing_edge_columns:
        report["errors"].append("Edges missing required columns: " + ", ".join(missing_edge_columns))

    if report["errors"]:
        report["passed"] = False
        return report

    node_ids = nodes["node_id"].fillna("").astype(str).str.strip()
    node_types = nodes["node_type"].fillna("").astype(str).str.strip().str.lower()
    node_names = nodes["name"].fillna("").astype(str).str.strip() if "name" in nodes.columns else pd.Series("", index=nodes.index)

    duplicate_node_ids = int(node_ids.duplicated().sum())
    empty_node_ids = int((node_ids == "").sum())
    invalid_node_types = int((~node_types.isin(VALID_NODE_TYPES)).sum())
    named_nodes = int((node_names != "").sum())
    node_name_coverage = _safe_ratio(named_nodes, len(nodes))

    transfer_count = pd.to_numeric(edges["transfer_count"], errors="coerce")
    invalid_transfer_count = int((transfer_count < 1).fillna(True).sum())

    source_ids = edges["source_node_id"].fillna("").astype(str).str.strip()
    target_ids = edges["target_node_id"].fillna("").astype(str).str.strip()
    known_node_ids = set(node_ids[node_ids != ""].tolist())

    source_missing = int((~source_ids.isin(known_node_ids)).sum())
    target_missing = int((~target_ids.isin(known_node_ids)).sum())
    edge_rows = int(len(edges))
    missing_node_refs = int(source_missing + target_missing)
    missing_node_ratio = _safe_ratio(missing_node_refs, max(1, edge_rows * 2))

    report["metrics"] = {
        "nodes_total": int(len(nodes)),
        "edges_total": edge_rows,
        "empty_node_id_rows": empty_node_ids,
        "duplicate_node_id_rows": duplicate_node_ids,
        "invalid_node_type_rows": invalid_node_types,
        "node_name_coverage": node_name_coverage,
        "invalid_transfer_count_rows": invalid_transfer_count,
        "source_node_missing_rows": source_missing,
        "target_node_missing_rows": target_missing,
        "missing_node_reference_ratio": missing_node_ratio,
    }

    if empty_node_ids > 0:
        report["errors"].append(f"Found {empty_node_ids} nodes with empty node_id")
    if duplicate_node_ids > 0:
        report["errors"].append(f"Found {duplicate_node_ids} duplicate node_id rows")
    if invalid_node_types > 0:
        report["errors"].append(f"Found {invalid_node_types} nodes with invalid node_type")
    if invalid_transfer_count > 0:
        report["errors"].append(f"Found {invalid_transfer_count} edges with invalid transfer_count (<1 or null)")

    if missing_node_ratio > float(cfg.max_missing_node_ratio):
        report["errors"].append(
            "Missing edge-node references ratio "
            f"{missing_node_ratio} exceeds threshold {float(cfg.max_missing_node_ratio)}"
        )

    if node_name_coverage < float(cfg.min_node_name_coverage):
        report["warnings"].append(
            "Node name coverage "
            f"{node_name_coverage} below target {float(cfg.min_node_name_coverage)}"
        )

    report["passed"] = len(report["errors"]) == 0
    return report
