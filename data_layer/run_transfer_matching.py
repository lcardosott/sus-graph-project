#!/usr/bin/env python3
"""CLI runner for transfer matching workflow.

The runner enforces schema gate checks before attempting to infer transfers.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

try:
    from data_layer.schema_gate import evaluate_schema, format_result, read_header
    from data_layer.transfer_matching import (
        TransferMatchConfig,
        aggregate_transfer_edges,
        build_probabilistic_patient_key,
        infer_transfer_events,
        load_validated_transfer_codes,
    )
except ModuleNotFoundError:
    # Allows `python data_layer/run_transfer_matching.py ...` from repo root.
    current_dir = Path(__file__).resolve().parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    from schema_gate import evaluate_schema, format_result, read_header
    from transfer_matching import (
        TransferMatchConfig,
        aggregate_transfer_edges,
        build_probabilistic_patient_key,
        infer_transfer_events,
        load_validated_transfer_codes,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run transfer matching after schema-gate validation.")
    parser.add_argument("--input", required=True, help="Input CSV file path.")
    parser.add_argument("--events-output", required=True, help="Output path for event-level matches CSV.")
    parser.add_argument("--edges-output", required=True, help="Output path for edge-level CSV.")
    parser.add_argument("--rejections-output", help="Optional JSON output for rejection counts.")
    parser.add_argument("--delimiter", default=";", help="CSV delimiter for input and output files.")
    parser.add_argument(
        "--motsai-codebook",
        default=str(Path(__file__).resolve().parent / "reference" / "motsai_transfer_codes.csv"),
        help="Validated PA_MOTSAI codebook path.",
    )
    parser.add_argument(
        "--linkage-strategy",
        choices=["probabilistic", "deterministic"],
        default="probabilistic",
        help="Patient linkage strategy. Default avoids N_AIH/NUM_PROC episode identifiers.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input).resolve()
    codebook_path = Path(args.motsai_codebook).resolve()

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return 2

    header = read_header(input_path, args.delimiter)
    gate_result = evaluate_schema(header, input_path, codebook_path)
    print(format_result(gate_result))
    if not gate_result.passed:
        return 2

    if (
        gate_result.datetime_pair is None
        or gate_result.sex_field is None
        or gate_result.age_field is None
        or gate_result.diagnosis_field is None
        or gate_result.origin_field is None
    ):
        print("Schema gate did not return required matching configuration.")
        return 2

    if args.linkage_strategy == "deterministic" and gate_result.patient_key_field is None:
        print("Deterministic linkage requested but no patient key field was mapped by schema gate.")
        return 2

    if args.linkage_strategy == "probabilistic" and not gate_result.probabilistic_linkage_supported:
        print("Probabilistic linkage requested but required fields are missing (birthdate/residence/sex/age).")
        return 2

    transfer_codes = load_validated_transfer_codes(codebook_path)
    records = pd.read_csv(input_path, sep=args.delimiter, encoding="utf-8-sig", dtype=str)

    transfer_flag_col = gate_result.transfer_flag_field or "__transfer_flag"
    transfer_reason_col = gate_result.transfer_reason_field or "__transfer_reason"
    if transfer_flag_col not in records.columns:
        records[transfer_flag_col] = "0"
    if transfer_reason_col not in records.columns:
        records[transfer_reason_col] = ""

    patient_key_col: str
    if args.linkage_strategy == "probabilistic":
        probabilistic_key, linkage_metadata = build_probabilistic_patient_key(
            records,
            sex_col=gate_result.sex_field,
            age_col=gate_result.age_field,
        )
        patient_key_col = "__probabilistic_patient_key"
        records[patient_key_col] = probabilistic_key

        valid_keys = int(records[patient_key_col].fillna("").astype(str).str.strip().ne("").sum())
        print(
            "Probabilistic linkage key: "
            f"{linkage_metadata['birthdate_field']} + {gate_result.sex_field} + "
            f"{gate_result.age_field} + {linkage_metadata['residence_field']}"
        )
        print(f"Rows with valid probabilistic key: {valid_keys}/{len(records)}")

        if valid_keys == 0:
            print("No valid probabilistic patient keys were generated from the input rows.")
            return 2
    else:
        patient_key_col = gate_result.patient_key_field
        assert patient_key_col is not None

    cfg = TransferMatchConfig(
        patient_key_col=patient_key_col,
        transfer_flag_col=transfer_flag_col,
        discharge_reason_col=transfer_reason_col,
        sex_col=gate_result.sex_field,
        age_col=gate_result.age_field,
        icd_col=gate_result.diagnosis_field,
        origin_facility_col=gate_result.origin_field,
        admission_datetime_col=gate_result.datetime_pair[0],
        discharge_datetime_col=gate_result.datetime_pair[1],
    )

    events, rejection_counts = infer_transfer_events(records, transfer_codes, cfg)
    edges = aggregate_transfer_edges(events)

    events_output = Path(args.events_output).resolve()
    events_output.parent.mkdir(parents=True, exist_ok=True)
    events.to_csv(events_output, index=False, sep=args.delimiter, encoding="utf-8-sig")

    edges_output = Path(args.edges_output).resolve()
    edges_output.parent.mkdir(parents=True, exist_ok=True)
    edges.to_csv(edges_output, index=False, sep=args.delimiter, encoding="utf-8-sig")

    if args.rejections_output:
        rejection_output = Path(args.rejections_output).resolve()
        rejection_output.parent.mkdir(parents=True, exist_ok=True)
        with rejection_output.open("w", encoding="utf-8") as json_file:
            json.dump(rejection_counts, json_file, indent=2, ensure_ascii=True)

    print(f"Transfer events written: {len(events)}")
    print(f"Transfer edges written: {len(edges)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())