#!/usr/bin/env python3
"""Run a full-year SIH pipeline from raw DBC to nodes/edges and optional graph.

Steps:
1) Project raw SIH DBC files to Parquet with strict columns (including NASC).
2) For each month, run transfer matching and build nodes/edges with enrichment.
3) Optionally aggregate a year-level graph.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

import pandas as pd
from pyreaddbc import dbc2dbf

try:
    from data_layer.column_projection import add_outcome_features, load_projected_from_dbf, save_projected
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
    from data_layer.transfer_matching import (
        TransferMatchConfig,
        aggregate_transfer_edges,
        build_probabilistic_patient_key,
        infer_transfer_events,
        load_validated_transfer_codes,
    )
except ModuleNotFoundError:
    current_dir = Path(__file__).resolve().parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    from column_projection import add_outcome_features, load_projected_from_dbf, save_projected  # type: ignore
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
    from transfer_matching import (  # type: ignore
        TransferMatchConfig,
        aggregate_transfer_edges,
        build_probabilistic_patient_key,
        infer_transfer_events,
        load_validated_transfer_codes,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a full-year SIH pipeline from raw files to graph artifacts.")
    parser.add_argument("--year", type=int, required=True, help="Year to process (e.g., 2021).")
    parser.add_argument("--months", default="1-12", help="Months expression, e.g. 1-12 or 1,2,3.")
    parser.add_argument("--ufs", default="ALL", help="UF list or ALL.")
    parser.add_argument("--raw-root", default="data_layer/raw/sih", help="Root with raw SIH files.")
    parser.add_argument("--projected-root", default="data_layer/curated/parquet/sih", help="Output root for parquet projection.")
    parser.add_argument("--reports-root", default="data_layer/reports/batches", help="Output root for batch artifacts.")
    parser.add_argument("--delimiter", default=";", help="CSV delimiter.")
    parser.add_argument(
        "--motsai-codebook",
        default="data_layer/reference/motsai_transfer_codes.csv",
        help="Transfer reason codebook path.",
    )
    parser.add_argument(
        "--national-cnes-reference",
        default="data_layer/reference/catalog/cnes_br_2101_public_hospitals.csv",
        help="Scoped national CNES reference for facility append.",
    )
    parser.add_argument("--ibge-cache", default="data_layer/reference/cache/ibge_municipios.json")
    parser.add_argument("--ibge-centroids-cache", default="data_layer/reference/cache/ibge_municipio_centroids.json")
    parser.add_argument("--cnes-cache", default="data_layer/reference/cache/cnes_estabelecimentos.json")
    parser.add_argument("--ibge-timeout", type=int, default=60)
    parser.add_argument("--cnes-timeout", type=int, default=60)
    parser.add_argument("--include-all-ibge-municipalities", action="store_true")
    parser.add_argument("--enrich-centroids", action="store_true")
    parser.add_argument("--enrich-facilities-cnes-api", action="store_true")
    parser.add_argument("--write-parquet", action="store_true")
    parser.add_argument("--skip-projection", action="store_true")
    parser.add_argument("--force-projection", action="store_true")
    parser.add_argument("--skip-graph", action="store_true")
    parser.add_argument("--graph-only", action="store_true", help="Skip monthly processing and aggregate existing outputs.")
    parser.add_argument("--skip-transfer", action="store_true", help="Skip transfer matching steps.")
    parser.add_argument("--skip-residence", action="store_true", help="Skip municipality->hospital residence edges.")
    parser.add_argument("--skip-ui-json", action="store_true", help="Skip UI JSONL export.")
    parser.add_argument("--max-missing-node-ratio", type=float, default=0.0)
    parser.add_argument("--min-node-name-coverage", type=float, default=0.9)
    return parser.parse_args()


def _parse_months(raw_value: str) -> list[int]:
    months: set[int] = set()
    for token in [item.strip() for item in raw_value.split(",") if item.strip()]:
        if "-" in token:
            start_raw, end_raw = token.split("-", maxsplit=1)
            start = int(start_raw)
            end = int(end_raw)
            for value in range(start, end + 1):
                months.add(value)
        else:
            months.add(int(token))
    return sorted([month for month in months if 1 <= month <= 12])


def _parse_ufs(raw_value: str) -> list[str]:
    if raw_value.strip().upper() in {"ALL", "*"}:
        return []
    return sorted({token.strip().upper() for token in raw_value.split(",") if token.strip()})


def _select_residence_field(columns: pd.Index) -> str | None:
    for candidate in ("MUNIC_RES", "CODMUNRES", "PA_MUNPCN"):
        if candidate in columns:
            return candidate
    return None


def _normalize_municipality_code6(series: pd.Series) -> pd.Series:
    raw = series.where(series.notna(), "").astype(str).str.strip()
    normalized = raw.map(lambda value: normalize_municipality_code(value))

    def pick_six(value: str) -> str:
        if not value:
            return ""
        if len(value) == 7:
            return value[:6]
        if len(value) > 7:
            return value[-6:]
        return value if len(value) == 6 else ""

    return normalized.map(pick_six)


def _normalize_nodes(nodes: pd.DataFrame) -> pd.DataFrame:
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


def _normalize_edges(edges: pd.DataFrame) -> pd.DataFrame:
    out = edges.copy()
    if "transfer_count" in out.columns:
        out["transfer_count"] = pd.to_numeric(out["transfer_count"], errors="coerce").fillna(1.0)
    if "confidence_score" in out.columns:
        out["confidence_score"] = pd.to_numeric(out["confidence_score"], errors="coerce")
    if "distance_km" in out.columns:
        out["distance_km"] = pd.to_numeric(out["distance_km"], errors="coerce")
    return out


def _collect_raw_files(raw_root: Path, year: int, months: list[int], ufs: list[str]) -> dict[int, list[Path]]:
    buckets: dict[int, list[Path]] = {month: [] for month in months}
    for month in months:
        month_dir = raw_root / f"year={year}" / f"month={month:02d}"
        if not month_dir.exists():
            continue
        uf_dirs = list(month_dir.glob("uf=*"))
        for uf_dir in uf_dirs:
            uf_token = uf_dir.name.replace("uf=", "").upper()
            if ufs and uf_token not in ufs:
                continue
            for dbc_file in sorted(uf_dir.glob("*.dbc")):
                buckets[month].append(dbc_file)
    return buckets


def _project_file(source_path: Path, output_path: Path, force: bool = False) -> dict[str, Any]:
    effective_source = source_path
    if source_path.suffix.lower() == ".dbc":
        dbf_path = source_path.with_suffix(".dbf")
        if force or not dbf_path.exists():
            dbc2dbf(str(source_path), str(dbf_path))
        effective_source = dbf_path

    projected, projection_map, missing_columns = load_projected_from_dbf(
        effective_source,
        include_linkage=True,
        max_rows=None,
    )
    enriched = add_outcome_features(projected)
    if force and output_path.exists():
        output_path.unlink()
    save_projected(enriched, output_path)
    return {
        "input": str(source_path),
        "effective_source": str(effective_source),
        "output": str(output_path),
        "rows": int(len(enriched)),
        "missing_columns": missing_columns,
        "projection_map": projection_map,
    }


def _load_month_frame(projected_files: list[Path]) -> pd.DataFrame:
    if not projected_files:
        return pd.DataFrame()
    frames = [pd.read_parquet(path) for path in projected_files]
    return pd.concat(frames, ignore_index=True)


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True))
            handle.write("\n")


def _json_safe(value: object) -> object:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    return value


def main() -> int:
    args = parse_args()

    year = int(args.year)
    months = _parse_months(args.months)
    ufs = _parse_ufs(args.ufs)

    print(f"[sih-year] start year={year} months={months} ufs={'ALL' if not ufs else ','.join(ufs)}")

    raw_root = Path(args.raw_root).resolve()
    projected_root = Path(args.projected_root).resolve()
    reports_root = Path(args.reports_root).resolve()
    codebook_path = Path(args.motsai_codebook).resolve()
    national_ref_path = Path(args.national_cnes_reference).resolve()

    if not args.graph_only:
        if not raw_root.exists():
            print(f"Raw root not found: {raw_root}")
            return 2
        if not codebook_path.exists():
            print(f"Codebook not found: {codebook_path}")
            return 2
        if not national_ref_path.exists():
            print(f"National CNES reference not found: {national_ref_path}")
            return 2

        raw_buckets = _collect_raw_files(raw_root, year, months, ufs)
        if not any(raw_buckets.values()):
            print("No raw files found for the requested year/month/UF selection.")
            return 2

    if not args.graph_only:
        projection_summary: dict[str, Any] = {
            "year": year,
            "months": months,
            "ufs": ufs,
            "projected_files": [],
        }

        if not args.skip_projection:
            total_files = sum(len(raw_buckets.get(month, [])) for month in months)
            print(f"[sih-year] projection start files={total_files}")
            file_index = 0
            for month in months:
                for dbc_path in raw_buckets.get(month, []):
                    file_index += 1
                    output_path = (
                        projected_root
                        / f"year={year}"
                        / f"month={month:02d}"
                        / f"uf={dbc_path.parent.name.replace('uf=', '')}"
                        / dbc_path.with_suffix(".parquet").name
                    )
                    if output_path.exists() and not args.force_projection:
                        continue
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    print(f"[sih-year] projection {file_index}/{total_files} -> {dbc_path.name}")
                    projection_summary["projected_files"].append(
                        _project_file(dbc_path, output_path, force=args.force_projection)
                    )

            summary_path = reports_root / f"sih_projection_{year}_summary.json"
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_path.write_text(json.dumps(projection_summary, indent=2, ensure_ascii=True), encoding="utf-8")
            print(f"[sih-year] projection summary: {summary_path}")

        transfer_codes = load_validated_transfer_codes(codebook_path)

        ibge_cache = Path(args.ibge_cache).resolve()
        municipality_catalog = load_or_refresh_ibge_catalog(
            ibge_cache,
            timeout_seconds=max(1, int(args.ibge_timeout)),
        )
        code6_to_uf = {
            normalize_municipality_code(item.get("id"))[:6]: str((((item.get("microrregiao") or {}).get("mesorregiao") or {}).get("UF") or {}).get("sigla") or "").strip().upper()
            for item in municipality_catalog
            if len(normalize_municipality_code(item.get("id"))) == 7
        }

        for month in months:
            print(f"[sih-year] month {year}-{month:02d} start")
            projected_files = list(
                (projected_root / f"year={year}" / f"month={month:02d}").rglob("*.parquet")
            )
            if not projected_files:
                print(f"No projected files for {year}-{month:02d}")
                continue
            print(f"[sih-year] month {year}-{month:02d} projected_files={len(projected_files)}")

            records = _load_month_frame(projected_files)
            if records.empty:
                print(f"Projected month frame is empty for {year}-{month:02d}")
                continue

            transfer_flag_col = "PA_TRANSF"
            if transfer_flag_col not in records.columns:
                records[transfer_flag_col] = "0"

            transfer_reason_col = "MOT_SAIDA"
            if transfer_reason_col not in records.columns:
                records[transfer_reason_col] = ""

            probabilistic_key, linkage_metadata = build_probabilistic_patient_key(
                records,
                sex_col="SEXO",
                age_col="IDADE",
            )
            records["__probabilistic_patient_key"] = probabilistic_key
            valid_keys = int(records["__probabilistic_patient_key"].fillna("").astype(str).str.strip().ne("").sum())
            if valid_keys == 0:
                print(f"No valid probabilistic keys for {year}-{month:02d}. Skipping month.")
                continue

            cfg = TransferMatchConfig(
                patient_key_col="__probabilistic_patient_key",
                transfer_flag_col=transfer_flag_col,
                discharge_reason_col=transfer_reason_col,
                sex_col="SEXO",
                age_col="IDADE",
                icd_col="DIAG_PRINC",
                origin_facility_col="CNES",
                admission_datetime_col="DT_INTER",
                discharge_datetime_col="DT_SAIDA",
            )

            events = pd.DataFrame()
            rejections: dict[str, int] = {}
            edges = pd.DataFrame()
            if not args.skip_transfer:
                events, rejections = infer_transfer_events(records, transfer_codes, cfg)
                edges = aggregate_transfer_edges(events)

            batch_tag = f"sih_br_{year}{month:02d}"
            batch_dir = reports_root / batch_tag
            batch_dir.mkdir(parents=True, exist_ok=True)

            events_output = batch_dir / f"transfer_events_{batch_tag}.csv"
            edges_output = batch_dir / f"transfer_edges_{batch_tag}.csv"
            rejections_output = batch_dir / f"transfer_rejections_{batch_tag}.json"
            if not args.skip_transfer:
                events.to_csv(events_output, index=False, sep=args.delimiter, encoding="utf-8-sig")
                edges.to_csv(edges_output, index=False, sep=args.delimiter, encoding="utf-8-sig")
                rejections_output.write_text(json.dumps(rejections, indent=2, ensure_ascii=True), encoding="utf-8")

            nodes, facility_lookup, municipality_lookup, metadata = build_nodes_from_records(records)

            if args.include_all_ibge_municipalities:
                all_codes = extract_catalog_municipality_codes(municipality_catalog, output_digits=6)
                nodes, _ = append_missing_municipality_nodes(nodes, all_codes)

            municipality_centers = None
            if args.enrich_centroids:
                requested_codes = nodes["municipality_code"].fillna("").astype(str).str.strip().tolist()
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

            national_ref = pd.read_csv(national_ref_path, sep=args.delimiter, encoding="utf-8-sig", dtype=str)
            if ufs and "uf" in national_ref.columns:
                national_ref = national_ref[national_ref["uf"].fillna("").astype(str).str.upper().isin(ufs)]
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
            nodes = pd.concat([nodes, national_nodes], ignore_index=True)
            nodes = nodes.drop_duplicates(subset=["node_id"], keep="first")
            nodes = nodes.sort_values(["node_type", "node_id"], kind="mergesort").reset_index(drop=True)

            mapped_edges = pd.DataFrame()
            if not args.skip_transfer:
                edges = edges.copy()
                edges["edge_type"] = "transfer"
                mapped_edges = map_edges_to_node_ids(edges, known_nodes=nodes)
                mapped_edges = _normalize_edges(mapped_edges)

            residence_edges = pd.DataFrame()
            residence_resolved_field = None
            if not args.skip_residence:
                residence_resolved_field = _select_residence_field(records.columns)
                if residence_resolved_field is None:
                    print(f"No residence municipality field found for {year}-{month:02d}. Skipping residence edges.")
                else:
                    residence_raw = _normalize_municipality_code6(records[residence_resolved_field])
                    facility_raw = records[metadata.facility_id_field].where(records[metadata.facility_id_field].notna(), "").astype(str).str.strip()
                    residence_frame = pd.DataFrame(
                        {
                            "source_node_id": "municipality:" + residence_raw,
                            "target_node_id": "facility:" + facility_raw,
                            "match_method": "residence",
                            "confidence_score": pd.NA,
                            "rule_version": "residence_v1_2026_04_26",
                            "assumption_revision": "2026-04-26",
                            "edge_type": "residence",
                        }
                    )
                    residence_frame = residence_frame[
                        (residence_raw != "")
                        & (facility_raw != "")
                    ]
                    if not residence_frame.empty:
                        residence_edges = (
                            residence_frame.groupby(
                                [
                                    "source_node_id",
                                    "target_node_id",
                                    "match_method",
                                    "rule_version",
                                    "assumption_revision",
                                    "edge_type",
                                ],
                                dropna=False,
                            )
                            .size()
                            .reset_index(name="transfer_count")
                        )
                        residence_edges["distance_km"] = pd.NA
                        residence_edges = _normalize_edges(residence_edges)

            if ufs and code6_to_uf:
                node_code = nodes.set_index("node_id")["municipality_code"].fillna("").astype(str).str.strip()
                node_uf = node_code.map(lambda code: code6_to_uf.get(normalize_municipality_code(code)[:6], ""))
                mapped_edges = mapped_edges.copy()
                mapped_edges["source_uf"] = mapped_edges["source_node_id"].map(node_uf).fillna("")
                mapped_edges["target_uf"] = mapped_edges["target_node_id"].map(node_uf).fillna("")
                mapped_edges = mapped_edges[
                    mapped_edges["source_uf"].isin(ufs)
                    & mapped_edges["target_uf"].isin(ufs)
                ]

            coverage = {
                "edge_rows": 0,
                "source_node_missing": 0,
                "target_node_missing": 0,
                "source_coverage_ratio": 0.0,
                "target_coverage_ratio": 0.0,
            }
            if not args.skip_transfer:
                coverage = summarize_edge_node_coverage(mapped_edges)

            nodes = _normalize_nodes(nodes)
            nodes_path = batch_dir / f"nodes_{batch_tag}.csv"
            edges_path = batch_dir / f"transfer_edges_{batch_tag}.csv"
            residence_edges_path = batch_dir / f"residence_edges_{batch_tag}.csv"
            facility_lookup_path = batch_dir / f"facility_lookup_{batch_tag}.csv"
            municipality_lookup_path = batch_dir / f"municipality_lookup_{batch_tag}.csv"

            nodes.to_csv(nodes_path, index=False, sep=args.delimiter, encoding="utf-8-sig")
            if not args.skip_transfer:
                mapped_edges.to_csv(edges_path, index=False, sep=args.delimiter, encoding="utf-8-sig")
            if not args.skip_residence and not residence_edges.empty:
                residence_edges.to_csv(residence_edges_path, index=False, sep=args.delimiter, encoding="utf-8-sig")
            facility_lookup.to_csv(facility_lookup_path, index=False, sep=args.delimiter, encoding="utf-8-sig")
            municipality_lookup.to_csv(municipality_lookup_path, index=False, sep=args.delimiter, encoding="utf-8-sig")

            if args.write_parquet:
                nodes.to_parquet(batch_dir / f"nodes_{batch_tag}.parquet", index=False)
                if not args.skip_transfer:
                    mapped_edges.to_parquet(batch_dir / f"transfer_edges_{batch_tag}.parquet", index=False)

            if not args.skip_transfer:
                contract_report = validate_nodes_edges_contract(
                    nodes,
                    mapped_edges,
                    thresholds=ValidationThresholds(
                        max_missing_node_ratio=float(args.max_missing_node_ratio),
                        min_node_name_coverage=float(args.min_node_name_coverage),
                    ),
                )
                contract_path = batch_dir / f"contract_validation_{batch_tag}.json"
                contract_path.write_text(json.dumps(contract_report, indent=2, ensure_ascii=True), encoding="utf-8")
                if not bool(contract_report.get("passed")):
                    print(f"Contract validation failed for {batch_tag}: {contract_path}")
                    return 2

            summary = {
                "batch_tag": batch_tag,
                "records_rows": int(len(records)),
                "nodes_total": int(len(nodes)),
                "facility_nodes": int((nodes["node_type"].astype(str).str.lower() == "facility").sum()),
                "municipality_nodes": int((nodes["node_type"].astype(str).str.lower() == "municipality").sum()),
                "edges_total": int(len(mapped_edges)) if not args.skip_transfer else 0,
                "edge_coverage": coverage,
                "probabilistic_key": {
                    "birthdate_field": linkage_metadata.get("birthdate_field"),
                    "residence_field": linkage_metadata.get("residence_field"),
                    "valid_keys": int(valid_keys),
                },
                "outputs": {
                    "nodes_csv": str(nodes_path),
                    "edges_csv": str(edges_path) if not args.skip_transfer else None,
                    "events_csv": str(events_output) if not args.skip_transfer else None,
                    "rejections_json": str(rejections_output) if not args.skip_transfer else None,
                    "contract_report": str(contract_path) if not args.skip_transfer else None,
                    "residence_edges_csv": str(residence_edges_path) if not args.skip_residence else None,
                },
                "mapping_fields": {
                    "facility_id_field": metadata.facility_id_field,
                    "facility_municipality_field": metadata.facility_municipality_field,
                    "municipality_fields": metadata.municipality_fields,
                    "residence_field": residence_resolved_field,
                },
            }
            (batch_dir / f"summary_{batch_tag}.json").write_text(
                json.dumps(summary, indent=2, ensure_ascii=True),
                encoding="utf-8",
            )
            print(f"[sih-year] month {year}-{month:02d} done nodes={len(nodes)} edges={len(mapped_edges)}")

    if args.skip_graph:
        return 0

    year_nodes: list[pd.DataFrame] = []
    year_edges: list[pd.DataFrame] = []
    for month in months:
        batch_tag = f"sih_br_{year}{month:02d}"
        batch_dir = reports_root / batch_tag
        nodes_path = batch_dir / f"nodes_{batch_tag}.csv"
        edges_path = batch_dir / f"transfer_edges_{batch_tag}.csv"
        residence_edges_path = batch_dir / f"residence_edges_{batch_tag}.csv"
        if not nodes_path.exists():
            continue
        year_nodes.append(pd.read_csv(nodes_path, sep=args.delimiter, encoding="utf-8-sig", dtype=str))
        if edges_path.exists():
            transfer_edges = pd.read_csv(edges_path, sep=args.delimiter, encoding="utf-8-sig", dtype=str)
            if "edge_type" not in transfer_edges.columns:
                transfer_edges["edge_type"] = "transfer"
            year_edges.append(transfer_edges)
        if residence_edges_path.exists():
            residence_edges = pd.read_csv(residence_edges_path, sep=args.delimiter, encoding="utf-8-sig", dtype=str)
            if "edge_type" not in residence_edges.columns:
                residence_edges["edge_type"] = "residence"
            year_edges.append(residence_edges)

    if not year_nodes or not year_edges:
        print("No monthly outputs found for graph aggregation.")
        return 2
    print(f"[sih-year] year aggregation months={len(year_nodes)}")

    nodes_year = pd.concat(year_nodes, ignore_index=True).drop_duplicates(subset=["node_id"]).reset_index(drop=True)
    edges_year = pd.concat(year_edges, ignore_index=True)

    if "edge_type" not in edges_year.columns:
        edges_year["edge_type"] = "transfer"
    edges_year["transfer_count"] = pd.to_numeric(edges_year["transfer_count"], errors="coerce").fillna(1.0)
    if "confidence_score" in edges_year.columns:
        edges_year["confidence_score"] = pd.to_numeric(edges_year["confidence_score"], errors="coerce")
    grouped_edges = (
        edges_year.groupby(["source_node_id", "target_node_id", "edge_type"], dropna=False)
        .agg(
            transfer_count=("transfer_count", "sum"),
            confidence_score=("confidence_score", "mean") if "confidence_score" in edges_year.columns else ("transfer_count", "size"),
            match_method=("match_method", "first") if "match_method" in edges_year.columns else ("source_node_id", "first"),
            rule_version=("rule_version", "first") if "rule_version" in edges_year.columns else ("source_node_id", "first"),
            assumption_revision=("assumption_revision", "first") if "assumption_revision" in edges_year.columns else ("source_node_id", "first"),
        )
        .reset_index()
    )

    nodes_year = _normalize_nodes(nodes_year)
    nodes_year_path = reports_root / f"sih_br_{year}_nodes.csv"
    edges_year_path = reports_root / f"sih_br_{year}_edges.csv"
    nodes_year.to_csv(nodes_year_path, index=False, sep=args.delimiter, encoding="utf-8-sig")
    grouped_edges.to_csv(edges_year_path, index=False, sep=args.delimiter, encoding="utf-8-sig")

    contract_report = validate_nodes_edges_contract(
        nodes_year,
        grouped_edges,
        thresholds=ValidationThresholds(
            max_missing_node_ratio=float(args.max_missing_node_ratio),
            min_node_name_coverage=float(args.min_node_name_coverage),
        ),
    )
    contract_path = reports_root / f"sih_br_{year}_contract_validation.json"
    contract_path.write_text(json.dumps(contract_report, indent=2, ensure_ascii=True), encoding="utf-8")
    if not bool(contract_report.get("passed")):
        print(f"Year contract validation failed: {contract_path}")
        return 2

    if not args.skip_ui_json:
        ui_defaults = {
            "max_nodes_default": 0,
            "max_edges_default": 0,
        }
        ui_dir = reports_root / "ui"
        nodes_ui_path = ui_dir / f"sih_br_{year}_nodes.jsonl"
        edges_ui_path = ui_dir / f"sih_br_{year}_edges.jsonl"
        meta_ui_path = ui_dir / f"sih_br_{year}_meta.json"

        node_rows = []
        for row in nodes_year.itertuples(index=False):
            item = row._asdict()
            node_rows.append(
                {
                    "node_id": str(item.get("node_id", "")),
                    "node_type": str(item.get("node_type", "")),
                    "name": str(item.get("name", "")),
                    "municipality_code": str(item.get("municipality_code", "")),
                    "latitude": _json_safe(item.get("latitude")),
                    "longitude": _json_safe(item.get("longitude")),
                }
            )
        _write_jsonl(nodes_ui_path, node_rows)

        edge_rows = []
        for row in grouped_edges.itertuples(index=False):
            item = row._asdict()
            edge_rows.append(
                {
                    "source_node_id": str(item.get("source_node_id", "")),
                    "target_node_id": str(item.get("target_node_id", "")),
                    "edge_type": str(item.get("edge_type", "")),
                    "transfer_count": _json_safe(item.get("transfer_count")),
                    "confidence_score": _json_safe(item.get("confidence_score")),
                    "match_method": str(item.get("match_method", "")),
                }
            )
        _write_jsonl(edges_ui_path, edge_rows)

        def _relpath(path: Path) -> str:
            try:
                return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
            except ValueError:
                return path.resolve().as_posix()

        meta_ui = {
            "nodes_input": _relpath(nodes_year_path),
            "edges_input": _relpath(edges_year_path),
            "nodes_jsonl": _relpath(nodes_ui_path),
            "edges_jsonl": _relpath(edges_ui_path),
            "defaults": ui_defaults,
        }
        meta_ui_path.write_text(json.dumps(meta_ui, indent=2, ensure_ascii=True), encoding="utf-8")

    try:
        from model_layer.build_graph import build_graph
    except ModuleNotFoundError:
        current_dir = Path(__file__).resolve().parents[1] / "model_layer"
        if str(current_dir) not in sys.path:
            sys.path.insert(0, str(current_dir))
        from build_graph import build_graph  # type: ignore

    graph = build_graph(nodes_year, grouped_edges, min_transfer_count=1.0)
    graph_output = Path("model_layer/reports") / f"graph_sih_br_{year}.gexf"
    graph_output.parent.mkdir(parents=True, exist_ok=True)
    graph_output.write_text("")
    import networkx as nx

    nx.write_gexf(graph, graph_output)
    graph_summary = {
        "nodes": int(graph.number_of_nodes()),
        "edges": int(graph.number_of_edges()),
        "graph_output": str(graph_output),
        "nodes_input": str(nodes_year_path),
        "edges_input": str(edges_year_path),
        "contract_report": str(contract_path),
    }
    summary_path = Path("model_layer/reports") / f"graph_sih_br_{year}_summary.json"
    summary_path.write_text(json.dumps(graph_summary, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"Year graph written: {graph_output}")
    print(f"Year graph summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
