#!/usr/bin/env python3
"""Project real DBC/DBF/Parquet files to strict analytics columns."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from pyreaddbc import dbc2dbf

try:
    from data_layer.column_projection import (
        add_outcome_features,
        load_projected_from_dbf,
        load_projected_from_parquet,
        save_projected,
    )
except ModuleNotFoundError:
    current_dir = Path(__file__).resolve().parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    from column_projection import (
        add_outcome_features,
        load_projected_from_dbf,
        load_projected_from_parquet,
        save_projected,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Project datasets to strict analytics columns.")
    parser.add_argument("--input", required=True, help="Input file path (.dbc, .dbf, .parquet).")
    parser.add_argument("--output", required=True, help="Output file path (.csv or .parquet).")
    parser.add_argument("--max-rows", type=int, help="Optional max rows for quick probes.")
    parser.add_argument("--include-linkage", action="store_true", help="Include optional linkage columns if present.")
    parser.add_argument("--summary-output", help="Optional JSON summary output path.")
    parser.add_argument("--delimiter", default=";", help="Delimiter for CSV output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return 2

    source_path = input_path
    suffix = input_path.suffix.lower()

    if suffix == ".dbc":
        dbf_path = input_path.with_suffix(".dbf")
        if not dbf_path.exists():
            dbc2dbf(str(input_path), str(dbf_path))
        source_path = dbf_path
        suffix = ".dbf"

    if suffix == ".dbf":
        projected, projection_map, missing_columns = load_projected_from_dbf(
            source_path,
            include_linkage=args.include_linkage,
            max_rows=args.max_rows,
        )
    elif suffix == ".parquet":
        projected, projection_map, missing_columns = load_projected_from_parquet(
            source_path,
            include_linkage=args.include_linkage,
        )
        if args.max_rows is not None:
            projected = projected.head(args.max_rows)
    else:
        print("Unsupported input format. Use .dbc, .dbf, or .parquet")
        return 2

    enriched = add_outcome_features(projected)
    save_projected(enriched, output_path, delimiter=args.delimiter)

    summary = {
        "input": str(input_path),
        "effective_source": str(source_path),
        "output": str(output_path),
        "rows": int(len(enriched)),
        "columns": list(enriched.columns),
        "projection_map": projection_map,
        "missing_columns": missing_columns,
    }

    if args.summary_output:
        summary_path = Path(args.summary_output).resolve()
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"Projected rows: {len(enriched)}")
    print(f"Output written: {output_path}")
    if missing_columns:
        print("Missing canonical columns:", ", ".join(missing_columns))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
