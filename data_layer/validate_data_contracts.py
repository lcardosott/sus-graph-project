#!/usr/bin/env python3
"""Validate data-layer nodes and edges contracts before downstream steps."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

try:
    from data_layer.contract_validation import ValidationThresholds, validate_nodes_edges_contract
except ModuleNotFoundError:
    current_dir = Path(__file__).resolve().parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    from contract_validation import ValidationThresholds, validate_nodes_edges_contract  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate node and edge contract integrity.")
    parser.add_argument("--nodes-input", required=True, help="Nodes CSV path.")
    parser.add_argument("--edges-input", required=True, help="Edges CSV path.")
    parser.add_argument("--delimiter", default=";", help="CSV delimiter.")
    parser.add_argument("--report-output", help="Optional JSON output report path.")
    parser.add_argument(
        "--max-missing-node-ratio",
        type=float,
        default=0.0,
        help="Maximum tolerated ratio of missing source/target references in edges.",
    )
    parser.add_argument(
        "--min-node-name-coverage",
        type=float,
        default=0.9,
        help="Minimum node name coverage ratio target (warning only).",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Always return exit code 0 even when validation fails.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    nodes_input = Path(args.nodes_input).resolve()
    edges_input = Path(args.edges_input).resolve()
    if not nodes_input.exists():
        print(f"Nodes input not found: {nodes_input}")
        return 2
    if not edges_input.exists():
        print(f"Edges input not found: {edges_input}")
        return 2

    nodes = pd.read_csv(nodes_input, sep=args.delimiter, encoding="utf-8-sig", dtype=str)
    edges = pd.read_csv(edges_input, sep=args.delimiter, encoding="utf-8-sig", dtype=str)

    report = validate_nodes_edges_contract(
        nodes,
        edges,
        thresholds=ValidationThresholds(
            max_missing_node_ratio=float(args.max_missing_node_ratio),
            min_node_name_coverage=float(args.min_node_name_coverage),
        ),
    )

    print("Contract validation:", "PASS" if report["passed"] else "FAIL")
    if report.get("metrics"):
        metrics = report["metrics"]
        print("Nodes total:", metrics.get("nodes_total"))
        print("Edges total:", metrics.get("edges_total"))
        print("Missing source rows:", metrics.get("source_node_missing_rows"))
        print("Missing target rows:", metrics.get("target_node_missing_rows"))
        print("Name coverage:", metrics.get("node_name_coverage"))

    for error in report.get("errors", []):
        print("ERROR:", error)
    for warning in report.get("warnings", []):
        print("WARNING:", warning)

    if args.report_output:
        output_path = Path(args.report_output).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, ensure_ascii=True), encoding="utf-8")

    if report["passed"] or args.report_only:
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
