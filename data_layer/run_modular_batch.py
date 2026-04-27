#!/usr/bin/env python3
"""Run a modular, filterable batch build for nodes, edges, and optional map HTML.

This runner is designed for iterative monthly/state workflows with intuitive
artifact naming and reusable filtering controls.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

import pandas as pd

try:
    from data_layer.contract_validation import ValidationThresholds, validate_nodes_edges_contract
    from data_layer.node_enrichment import (
        extract_catalog_municipality_codes,
        enrich_nodes_with_facility_reference,
        enrich_nodes_with_municipality_metadata,
        load_or_refresh_cnes_facility_reference,
        load_or_refresh_ibge_catalog,
        load_or_refresh_ibge_centroids,
        normalize_municipality_code,
        resolve_codes_to_ibge_ids,
    )
    from data_layer.node_mapping import (
        append_missing_municipality_nodes,
        build_nodes_from_records,
        map_edges_to_node_ids,
        summarize_edge_node_coverage,
    )
except ModuleNotFoundError:
    current_dir = Path(__file__).resolve().parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    from contract_validation import ValidationThresholds, validate_nodes_edges_contract  # type: ignore
    from node_enrichment import (  # type: ignore
        extract_catalog_municipality_codes,
        enrich_nodes_with_facility_reference,
        enrich_nodes_with_municipality_metadata,
        load_or_refresh_cnes_facility_reference,
        load_or_refresh_ibge_catalog,
        load_or_refresh_ibge_centroids,
        normalize_municipality_code,
        resolve_codes_to_ibge_ids,
    )
    from node_mapping import (  # type: ignore
        append_missing_municipality_nodes,
        build_nodes_from_records,
        map_edges_to_node_ids,
        summarize_edge_node_coverage,
    )


VALID_UFS = {
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT",
    "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
}
TRUTHY_VALUES = {"1", "S", "SIM", "Y", "YES", "TRUE", "T"}


def _parse_csv_list(raw_value: str) -> list[str]:
    return [token.strip() for token in raw_value.split(",") if token.strip()]


def _parse_ufs(raw_value: str) -> list[str]:
    if raw_value.strip().upper() in {"ALL", "*"}:
        return []
    selected = [token.strip().upper() for token in raw_value.split(",") if token.strip()]
    invalid = [uf for uf in selected if uf not in VALID_UFS]
    if invalid:
        raise ValueError("Invalid UF in --include-ufs: " + ", ".join(sorted(set(invalid))))
    return sorted(set(selected))


def _sanitize_token(raw_value: str) -> str:
    allowed = [ch.lower() for ch in raw_value if ch.isalnum() or ch in {"-", "_"}]
    token = "".join(allowed).strip("_-")
    return token or "all"


def _to_bool_series(series: pd.Series) -> pd.Series:
    normalized = series.fillna("").astype(str).str.strip().str.upper()
    numeric = pd.to_numeric(normalized, errors="coerce")
    return normalized.isin(TRUTHY_VALUES) | numeric.eq(1)


def _resolve_public_hospital_mask(national_ref: pd.DataFrame) -> pd.Series | None:
    if "is_public_hospital" in national_ref.columns:
        return _to_bool_series(national_ref["is_public_hospital"])

    required_raw = {"vinc_sus", "atendhos", "leithosp", "niv_dep"}
    if not required_raw.issubset(set(national_ref.columns)):
        return None

    is_sus = _to_bool_series(national_ref["vinc_sus"])
    has_hospital = _to_bool_series(national_ref["atendhos"]) | _to_bool_series(national_ref["leithosp"])

    if "is_public_admin" in national_ref.columns:
        is_public_admin = _to_bool_series(national_ref["is_public_admin"])
    else:
        niv_dep_public = national_ref["niv_dep"].fillna("").astype(str).str.strip() == "1"
        nat_jur_public = pd.Series(False, index=national_ref.index)
        if "nat_jur" in national_ref.columns:
            nat_jur_public = national_ref["nat_jur"].fillna("").astype(str).str.replace(r"\D", "", regex=True).str.startswith("1")
        is_public_admin = niv_dep_public | nat_jur_public

    return is_sus & has_hospital & is_public_admin


def _build_batch_tag(
    batch_prefix: str,
    year: int,
    month: int,
    include_ufs: list[str],
    icd_prefixes: list[str],
    min_transfer_count: float,
) -> str:
    parts = [f"{_sanitize_token(batch_prefix)}_{year}{month:02d}"]
    if include_ufs:
        parts.append("ufs-" + "-".join(uf.lower() for uf in include_ufs))
    if icd_prefixes:
        parts.append("icd-" + "-".join(_sanitize_token(prefix) for prefix in icd_prefixes))
    if min_transfer_count > 1.0:
        clean_value = str(min_transfer_count).replace(".", "p")
        parts.append(f"tmin-{clean_value}")
    return "_".join(parts)


def _normalize_code6(value: Any) -> str:
    code = normalize_municipality_code(value)
    if len(code) >= 6:
        return code[:6]
    return ""


def _build_code6_to_uf_map(municipality_catalog: list[dict[str, Any]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for item in municipality_catalog:
        ibge_id = normalize_municipality_code(item.get("id"))
        if len(ibge_id) != 7:
            continue
        code6 = ibge_id[:6]
        uf_payload = (((item.get("microrregiao") or {}).get("mesorregiao") or {}).get("UF") or {})
        uf = str(uf_payload.get("sigla") or "").strip().upper()
        if uf:
            mapping[code6] = uf
    return mapping


def _apply_record_filters(
    records: pd.DataFrame,
    include_ufs: list[str],
    code6_to_uf: dict[str, str],
    icd_prefixes: list[str],
) -> pd.DataFrame:
    out = records.copy()

    if icd_prefixes and "DIAG_PRINC" in out.columns:
        normalized_prefixes = tuple(prefix.strip().upper() for prefix in icd_prefixes if prefix.strip())
        if normalized_prefixes:
            diag = out["DIAG_PRINC"].fillna("").astype(str).str.strip().str.upper()
            out = out[diag.str.startswith(normalized_prefixes)]

    if include_ufs:
        municipality_fields = [
            field
            for field in ["MUNIC_RES", "MUNIC_MOV", "CODMUNRES", "PA_MUNPCN"]
            if field in out.columns
        ]

        if municipality_fields:
            keep_mask = pd.Series(False, index=out.index)
            for field in municipality_fields:
                code6 = out[field].map(_normalize_code6)
                uf_series = code6.map(code6_to_uf).fillna("")
                keep_mask = keep_mask | uf_series.isin(include_ufs)
            out = out[keep_mask]

    return out


def _normalize_nodes_for_output(nodes: pd.DataFrame) -> pd.DataFrame:
    out = nodes.copy()

    for column in ["node_id", "node_type", "name", "municipality_code", "habilitation_level"]:
        if column in out.columns:
            out[column] = out[column].astype("string")

    for column in ["latitude", "longitude"]:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce").astype("Float64")

    if "capacity_beds" in out.columns:
        out["capacity_beds"] = pd.to_numeric(out["capacity_beds"], errors="coerce").astype("Int64")

    return out


def _normalize_edges_for_output(edges: pd.DataFrame) -> pd.DataFrame:
    out = edges.copy()
    if "transfer_count" in out.columns:
        out["transfer_count"] = pd.to_numeric(out["transfer_count"], errors="coerce").fillna(1.0)
    if "confidence_score" in out.columns:
        out["confidence_score"] = pd.to_numeric(out["confidence_score"], errors="coerce")
    if "distance_km" in out.columns:
        out["distance_km"] = pd.to_numeric(out["distance_km"], errors="coerce")
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run modular batch artifacts with filters and intuitive naming.")
    parser.add_argument("--batch-prefix", required=True, help="Batch prefix, for example rdsp.")
    parser.add_argument("--year", type=int, required=True, help="Batch year, for example 2021.")
    parser.add_argument("--month", type=int, required=True, help="Batch month (1-12).")
    parser.add_argument("--records-input", required=True, help="Input records CSV used to build nodes.")
    parser.add_argument("--edges-input", required=True, help="Input transfer edges CSV.")
    parser.add_argument("--reports-root", default="data_layer/reports/batches", help="Root directory for batch artifacts.")
    parser.add_argument("--summary-output", help="Optional custom summary output path.")
    parser.add_argument("--delimiter", default=";", help="CSV delimiter.")

    parser.add_argument(
        "--include-ufs",
        default="ALL",
        help="UF filter for records and mapped edges (comma-separated) or ALL.",
    )
    parser.add_argument(
        "--icd-prefixes",
        default="",
        help="Optional ICD prefixes filter (comma-separated), applied on DIAG_PRINC when available.",
    )
    parser.add_argument("--min-transfer-count", type=float, default=1.0, help="Minimum transfer_count to keep edges.")

    parser.add_argument("--enrich-municipalities", action="store_true", help="Enable IBGE municipality enrichment.")
    parser.add_argument(
        "--include-all-ibge-municipalities",
        action="store_true",
        help="Append municipality nodes for all IBGE municipalities.",
    )
    parser.add_argument("--enrich-centroids", action="store_true", help="Fetch municipality centroids.")
    parser.add_argument(
        "--centroids-for-all-ibge-municipalities",
        action="store_true",
        help="When appending all municipalities, fetch centroids for all municipalities.",
    )
    parser.add_argument("--enrich-facilities-cnes-api", action="store_true", help="Refresh facility metadata from CNES API.")

    parser.add_argument("--ibge-cache", default="data_layer/reference/cache/ibge_municipios.json")
    parser.add_argument("--ibge-centroids-cache", default="data_layer/reference/cache/ibge_municipio_centroids.json")
    parser.add_argument("--cnes-cache", default="data_layer/reference/cache/cnes_estabelecimentos.json")
    parser.add_argument("--ibge-timeout", type=int, default=60)
    parser.add_argument("--cnes-timeout", type=int, default=60)

    parser.add_argument("--render-map", action="store_true", help="Render geospatial HTML map for the batch.")
    parser.add_argument(
        "--national-cnes-reference",
        help="Optional CSV generated by build_national_reference.py to append nationwide CNES nodes.",
    )
    parser.add_argument(
        "--national-cnes-append-scope",
        choices=["public_hospitals", "all"],
        default="public_hospitals",
        help="Scope used when appending nodes from national CNES reference (default: public_hospitals).",
    )
    parser.add_argument("--map-zoom-start", type=int, default=4)
    parser.add_argument("--max-node-markers", type=int, default=8000)
    parser.add_argument("--max-edge-lines", type=int, default=3000)
    parser.add_argument("--map-cluster-markers", action="store_true", help="Use clustered markers in map rendering.")

    parser.add_argument(
        "--skip-contract-validation",
        action="store_true",
        help="Skip node/edge contract validation gate.",
    )
    parser.add_argument(
        "--max-missing-node-ratio",
        type=float,
        default=0.0,
        help="Max tolerated ratio of missing edge references to nodes.",
    )
    parser.add_argument(
        "--min-node-name-coverage",
        type=float,
        default=0.9,
        help="Minimum target node name coverage ratio (warning threshold).",
    )
    parser.add_argument(
        "--contract-report-output",
        help="Optional JSON path for contract validation report.",
    )

    parser.add_argument("--write-parquet", action="store_true", help="Also export nodes/edges in Parquet format.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    include_ufs = _parse_ufs(args.include_ufs)
    icd_prefixes = _parse_csv_list(args.icd_prefixes)

    batch_tag = _build_batch_tag(
        batch_prefix=args.batch_prefix,
        year=int(args.year),
        month=int(args.month),
        include_ufs=include_ufs,
        icd_prefixes=icd_prefixes,
        min_transfer_count=float(args.min_transfer_count),
    )

    reports_root = Path(args.reports_root).resolve()
    batch_dir = reports_root / batch_tag
    batch_dir.mkdir(parents=True, exist_ok=True)

    records_input = Path(args.records_input).resolve()
    edges_input = Path(args.edges_input).resolve()
    if not records_input.exists():
        print(f"Records input not found: {records_input}")
        return 2
    if not edges_input.exists():
        print(f"Edges input not found: {edges_input}")
        return 2

    records = pd.read_csv(records_input, sep=args.delimiter, encoding="utf-8-sig", dtype=str)
    edges = pd.read_csv(edges_input, sep=args.delimiter, encoding="utf-8-sig", dtype=str)

    municipality_catalog: list[dict[str, Any]] = []
    code6_to_uf: dict[str, str] = {}
    if include_ufs or args.enrich_municipalities:
        ibge_cache = Path(args.ibge_cache).resolve()
        municipality_catalog = load_or_refresh_ibge_catalog(
            ibge_cache,
            timeout_seconds=max(1, int(args.ibge_timeout)),
        )
        code6_to_uf = _build_code6_to_uf_map(municipality_catalog)

    filtered_records = _apply_record_filters(
        records,
        include_ufs=include_ufs,
        code6_to_uf=code6_to_uf,
        icd_prefixes=icd_prefixes,
    )

    nodes, facility_lookup, municipality_lookup, metadata = build_nodes_from_records(filtered_records)

    all_ibge_municipalities_added = 0
    centroid_scope = "disabled"
    if args.enrich_municipalities:
        if args.include_all_ibge_municipalities:
            all_codes = extract_catalog_municipality_codes(municipality_catalog, output_digits=6)
            nodes, all_ibge_municipalities_added = append_missing_municipality_nodes(nodes, all_codes)

        municipality_centers = None
        if args.enrich_centroids:
            centroid_scope = "all_nodes"
            requested_codes = nodes["municipality_code"].fillna("").astype(str).str.strip().tolist()
            if args.include_all_ibge_municipalities and not args.centroids_for_all_ibge_municipalities:
                requested_codes = filtered_records[[field for field in ["MUNIC_RES", "MUNIC_MOV", "CODMUNRES", "PA_MUNPCN"] if field in filtered_records.columns]].stack().tolist() if any(
                    field in filtered_records.columns for field in ["MUNIC_RES", "MUNIC_MOV", "CODMUNRES", "PA_MUNPCN"]
                ) else requested_codes
                centroid_scope = "observed_records_only"

            requested_ibge = resolve_codes_to_ibge_ids(requested_codes, municipality_catalog)
            municipality_centers = load_or_refresh_ibge_centroids(
                Path(args.ibge_centroids_cache).resolve(),
                requested_ibge,
                timeout_seconds=max(1, int(args.ibge_timeout)),
                continue_on_error=True,
                max_retries=4,
                retry_wait_seconds=0.35,
            )

        nodes, _stats = enrich_nodes_with_municipality_metadata(
            nodes,
            municipality_catalog,
            municipality_centers=municipality_centers,
            assign_facility_coordinates_from_municipality=True,
        )

    if args.enrich_facilities_cnes_api:
        facility_ids = (
            nodes.loc[nodes["node_type"].astype(str).str.lower() == "facility", "node_id"]
            .astype(str)
            .str.replace("^facility:", "", regex=True)
            .str.strip()
            .tolist()
        )
        cnes_reference = load_or_refresh_cnes_facility_reference(
            Path(args.cnes_cache).resolve(),
            facility_ids,
            timeout_seconds=max(1, int(args.cnes_timeout)),
        )
        if not cnes_reference.empty:
            nodes, _ = enrich_nodes_with_facility_reference(
                nodes,
                cnes_reference,
                facility_id_column="cnes",
            )

    appended_national_cnes_nodes = 0
    national_cnes_rows_before_scope = 0
    national_cnes_rows_after_scope = 0
    if args.national_cnes_reference:
        national_ref_path = Path(args.national_cnes_reference).resolve()
        if not national_ref_path.exists():
            print(f"National CNES reference not found: {national_ref_path}")
            return 2

        national_ref = pd.read_csv(national_ref_path, sep=args.delimiter, encoding="utf-8-sig", dtype=str)
        required_columns = {"cnes", "municipality_code", "name", "latitude", "longitude"}
        missing_columns = sorted(required_columns - set(national_ref.columns))
        if missing_columns:
            print(
                "National CNES reference missing columns: " + ", ".join(missing_columns)
            )
            return 2

        national_cnes_rows_before_scope = int(len(national_ref))
        if args.national_cnes_append_scope == "public_hospitals":
            public_hospital_mask = _resolve_public_hospital_mask(national_ref)
            if public_hospital_mask is None:
                print(
                    "National CNES reference lacks public-hospital classifier columns. "
                    "Rebuild it with build_national_reference.py or use --national-cnes-append-scope all."
                )
                return 2
            national_ref = national_ref[public_hospital_mask].copy()

        national_cnes_rows_after_scope = int(len(national_ref))

        if include_ufs and "uf" in national_ref.columns:
            national_ref = national_ref[national_ref["uf"].fillna("").astype(str).str.upper().isin(include_ufs)]

        national_ref = national_ref.copy()
        national_ref["cnes"] = national_ref["cnes"].fillna("").astype(str).str.strip()
        national_ref = national_ref[national_ref["cnes"] != ""]
        national_ref = national_ref.drop_duplicates(subset=["cnes"], keep="first")

        national_nodes = pd.DataFrame(
            {
                "node_id": "facility:" + national_ref["cnes"],
                "node_type": "facility",
                "name": national_ref["name"],
                "municipality_code": national_ref["municipality_code"],
                "latitude": national_ref["latitude"],
                "longitude": national_ref["longitude"],
                "capacity_beds": pd.NA,
                "habilitation_level": pd.NA,
            }
        )

        existing_node_ids = set(nodes["node_id"].astype(str).tolist())
        appended_national_cnes_nodes = int((~national_nodes["node_id"].isin(existing_node_ids)).sum())

        nodes = pd.concat([nodes, national_nodes], ignore_index=True)
        nodes = nodes.drop_duplicates(subset=["node_id"], keep="first")
        nodes = nodes.sort_values(["node_type", "node_id"], kind="mergesort").reset_index(drop=True)

    mapped_edges = map_edges_to_node_ids(edges, known_nodes=nodes)
    mapped_edges["transfer_count"] = pd.to_numeric(mapped_edges.get("transfer_count"), errors="coerce").fillna(1.0)
    mapped_edges = mapped_edges[mapped_edges["transfer_count"] >= float(args.min_transfer_count)]

    if include_ufs and code6_to_uf:
        node_code = nodes.set_index("node_id")["municipality_code"].fillna("").astype(str).str.strip()
        node_uf = node_code.map(lambda code: code6_to_uf.get(_normalize_code6(code), ""))

        mapped_edges = mapped_edges.copy()
        mapped_edges["source_uf"] = mapped_edges["source_node_id"].map(node_uf).fillna("")
        mapped_edges["target_uf"] = mapped_edges["target_node_id"].map(node_uf).fillna("")
        mapped_edges = mapped_edges[
            mapped_edges["source_uf"].isin(include_ufs)
            & mapped_edges["target_uf"].isin(include_ufs)
        ]

    coverage = summarize_edge_node_coverage(mapped_edges)

    nodes_path = batch_dir / f"nodes_{batch_tag}.csv"
    edges_path = batch_dir / f"transfer_edges_{batch_tag}.csv"
    facility_lookup_path = batch_dir / f"facility_lookup_{batch_tag}.csv"
    municipality_lookup_path = batch_dir / f"municipality_lookup_{batch_tag}.csv"

    nodes = _normalize_nodes_for_output(nodes)
    mapped_edges = _normalize_edges_for_output(mapped_edges)

    nodes.to_csv(nodes_path, index=False, sep=args.delimiter, encoding="utf-8-sig")
    mapped_edges.to_csv(edges_path, index=False, sep=args.delimiter, encoding="utf-8-sig")
    facility_lookup.to_csv(facility_lookup_path, index=False, sep=args.delimiter, encoding="utf-8-sig")
    municipality_lookup.to_csv(municipality_lookup_path, index=False, sep=args.delimiter, encoding="utf-8-sig")

    if args.write_parquet:
        nodes.to_parquet(batch_dir / f"nodes_{batch_tag}.parquet", index=False)
        mapped_edges.to_parquet(batch_dir / f"transfer_edges_{batch_tag}.parquet", index=False)

    contract_validation_report: dict[str, Any] | None = None
    contract_validation_output: Path | None = None
    if not args.skip_contract_validation:
        contract_validation_report = validate_nodes_edges_contract(
            nodes,
            mapped_edges,
            thresholds=ValidationThresholds(
                max_missing_node_ratio=float(args.max_missing_node_ratio),
                min_node_name_coverage=float(args.min_node_name_coverage),
            ),
        )
        contract_validation_output = (
            Path(args.contract_report_output).resolve()
            if args.contract_report_output
            else (batch_dir / f"contract_validation_{batch_tag}.json")
        )
        contract_validation_output.parent.mkdir(parents=True, exist_ok=True)
        contract_validation_output.write_text(
            json.dumps(contract_validation_report, indent=2, ensure_ascii=True),
            encoding="utf-8",
        )
        if not bool(contract_validation_report.get("passed", False)):
            print("Contract validation failed. Review:", contract_validation_output)
            return 2

    map_output_path: Path | None = None
    map_summary_path: Path | None = None
    if args.render_map:
        map_output_path = batch_dir / f"geo_map_{batch_tag}.html"
        map_summary_path = batch_dir / f"geo_map_{batch_tag}_summary.json"
        render_script = Path(__file__).resolve().parents[1] / "viz_layer" / "render_geospatial_map.py"
        render_command = [
            sys.executable,
            str(render_script),
            "--nodes-input",
            str(nodes_path),
            "--edges-input",
            str(edges_path),
            "--html-output",
            str(map_output_path),
            "--summary-output",
            str(map_summary_path),
            "--delimiter",
            args.delimiter,
            "--min-transfer-count",
            str(args.min_transfer_count),
            "--zoom-start",
            str(args.map_zoom_start),
            "--max-node-markers",
            str(args.max_node_markers),
            "--max-edge-lines",
            str(args.max_edge_lines),
        ]
        if args.map_cluster_markers:
            render_command.append("--cluster-markers")

        subprocess.run(render_command, check=True)

    summary = {
        "batch_tag": batch_tag,
        "records_input": str(records_input),
        "edges_input": str(edges_input),
        "records_rows": int(len(records)),
        "records_rows_after_filters": int(len(filtered_records)),
        "nodes_total": int(len(nodes)),
        "facility_nodes": int((nodes["node_type"].astype(str).str.lower() == "facility").sum()),
        "municipality_nodes": int((nodes["node_type"].astype(str).str.lower() == "municipality").sum()),
        "edges_total_after_filters": int(len(mapped_edges)),
        "edge_coverage": coverage,
        "filters": {
            "include_ufs": include_ufs,
            "icd_prefixes": icd_prefixes,
            "min_transfer_count": float(args.min_transfer_count),
        },
        "enrichment": {
            "municipalities": bool(args.enrich_municipalities),
            "include_all_ibge_municipalities": bool(args.include_all_ibge_municipalities),
            "all_ibge_municipalities_added": int(all_ibge_municipalities_added),
            "national_cnes_append_scope": args.national_cnes_append_scope if args.national_cnes_reference else "disabled",
            "national_cnes_rows_before_scope": int(national_cnes_rows_before_scope),
            "national_cnes_rows_after_scope": int(national_cnes_rows_after_scope),
            "appended_national_cnes_nodes": int(appended_national_cnes_nodes),
            "centroids": bool(args.enrich_centroids),
            "centroid_scope": centroid_scope,
            "cnes_api": bool(args.enrich_facilities_cnes_api),
        },
        "contract_validation": {
            "enabled": bool(not args.skip_contract_validation),
            "report": str(contract_validation_output) if contract_validation_output else None,
            "passed": bool(contract_validation_report.get("passed")) if contract_validation_report else None,
            "max_missing_node_ratio": float(args.max_missing_node_ratio),
            "min_node_name_coverage": float(args.min_node_name_coverage),
        },
        "outputs": {
            "batch_dir": str(batch_dir),
            "nodes_csv": str(nodes_path),
            "edges_csv": str(edges_path),
            "facility_lookup_csv": str(facility_lookup_path),
            "municipality_lookup_csv": str(municipality_lookup_path),
            "map_html": str(map_output_path) if map_output_path else None,
            "map_summary_json": str(map_summary_path) if map_summary_path else None,
        },
        "mapping_fields": {
            "facility_id_field": metadata.facility_id_field,
            "facility_municipality_field": metadata.facility_municipality_field,
            "municipality_fields": metadata.municipality_fields,
        },
    }

    summary_path = Path(args.summary_output).resolve() if args.summary_output else (batch_dir / f"summary_{batch_tag}.json")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"Batch tag: {batch_tag}")
    print(f"Nodes written: {nodes_path}")
    print(f"Edges written: {edges_path}")
    if map_output_path:
        print(f"Map written: {map_output_path}")
    print(f"Summary written: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
