#!/usr/bin/env python3
"""Build nodes.csv and optional edge-node mapping artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

try:
    from data_layer.node_enrichment import (
        extract_catalog_municipality_codes,
        enrich_nodes_with_facility_reference,
        enrich_nodes_with_municipality_metadata,
        load_or_refresh_cnes_facility_reference,
        load_or_refresh_ibge_catalog,
        load_or_refresh_ibge_centroids,
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
    from node_enrichment import (  # type: ignore
        extract_catalog_municipality_codes,
        enrich_nodes_with_facility_reference,
        enrich_nodes_with_municipality_metadata,
        load_or_refresh_cnes_facility_reference,
        load_or_refresh_ibge_catalog,
        load_or_refresh_ibge_centroids,
        resolve_codes_to_ibge_ids,
    )
    from node_mapping import (  # type: ignore
        append_missing_municipality_nodes,
        build_nodes_from_records,
        map_edges_to_node_ids,
        summarize_edge_node_coverage,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build node artifacts from SIH/SIASUS records.")
    parser.add_argument("--records-input", required=True, help="Input CSV with source records.")
    parser.add_argument("--nodes-output", required=True, help="Output CSV for canonical nodes.")
    parser.add_argument("--facility-lookup-output", help="Optional CSV output for facility lookup mapping.")
    parser.add_argument("--municipality-lookup-output", help="Optional CSV output for municipality lookup mapping.")
    parser.add_argument("--edges-input", help="Optional edge CSV to map source/target to node ids.")
    parser.add_argument("--edges-output", help="Output CSV for mapped edges (required when --edges-input is used).")
    parser.add_argument(
        "--enrich-municipalities",
        action="store_true",
        help="Enrich municipality/facility node names from IBGE municipality catalog.",
    )
    parser.add_argument(
        "--include-all-ibge-municipalities",
        action="store_true",
        help="Append municipality nodes for all municipalities in IBGE catalog.",
    )
    parser.add_argument(
        "--ibge-cache",
        default="data_layer/reference/cache/ibge_municipios.json",
        help="Cache file for IBGE municipality catalog JSON.",
    )
    parser.add_argument(
        "--enrich-centroids",
        action="store_true",
        help="Fetch and assign municipality centroid coordinates from IBGE malha GeoJSON.",
    )
    parser.add_argument(
        "--centroids-for-all-ibge-municipalities",
        action="store_true",
        help="When used with --include-all-ibge-municipalities, fetch centroids for all municipalities.",
    )
    parser.add_argument(
        "--ibge-centroids-cache",
        default="data_layer/reference/cache/ibge_municipio_centroids.json",
        help="Cache file for IBGE municipality centroid coordinates.",
    )
    parser.add_argument(
        "--disable-facility-centroid-fallback",
        action="store_true",
        help="Do not assign municipality centroid coordinates to facility nodes.",
    )
    parser.add_argument(
        "--facility-reference-input",
        help="Optional CSV with facility metadata for CNES enrichment.",
    )
    parser.add_argument(
        "--enrich-facilities-cnes-api",
        action="store_true",
        help="Enrich facility metadata and geocoordinates from CNES open-data API.",
    )
    parser.add_argument(
        "--cnes-cache",
        default="data_layer/reference/cache/cnes_estabelecimentos.json",
        help="Cache file for CNES facility metadata fetched from API.",
    )
    parser.add_argument(
        "--cnes-timeout",
        type=int,
        default=60,
        help="Timeout (seconds) for each CNES API request.",
    )
    parser.add_argument(
        "--facility-reference-id-column",
        default="cnes",
        help="Facility id column in facility reference CSV.",
    )
    parser.add_argument("--summary-output", help="Optional JSON summary output path.")
    parser.add_argument("--delimiter", default=";", help="CSV delimiter for input and output.")
    return parser.parse_args()


def _write_csv(frame: pd.DataFrame, output_path: Path, delimiter: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False, sep=delimiter, encoding="utf-8-sig")


def main() -> int:
    args = parse_args()

    records_input = Path(args.records_input).resolve()
    if not records_input.exists():
        print(f"Records input not found: {records_input}")
        return 2

    if args.edges_input and not args.edges_output:
        print("--edges-output is required when --edges-input is provided")
        return 2

    if args.enrich_centroids and not args.enrich_municipalities:
        print("--enrich-centroids requires --enrich-municipalities")
        return 2

    if args.include_all_ibge_municipalities and not args.enrich_municipalities:
        print("--include-all-ibge-municipalities requires --enrich-municipalities")
        return 2

    records = pd.read_csv(records_input, sep=args.delimiter, encoding="utf-8-sig", dtype=str)

    nodes, facility_lookup, municipality_lookup, metadata = build_nodes_from_records(records)
    observed_municipality_codes = nodes["municipality_code"].fillna("").astype(str).str.strip().tolist()

    enrichment_summary: dict[str, object] = {}
    municipality_catalog: list[dict[str, object]] | None = None
    all_ibge_municipalities_added = 0
    if args.enrich_municipalities:
        ibge_cache = Path(args.ibge_cache).resolve()
        municipality_catalog = load_or_refresh_ibge_catalog(ibge_cache)

        if args.include_all_ibge_municipalities:
            all_catalog_codes = extract_catalog_municipality_codes(
                municipality_catalog,
                output_digits=6,
            )
            nodes, all_ibge_municipalities_added = append_missing_municipality_nodes(nodes, all_catalog_codes)

        municipality_centers = None
        if args.enrich_centroids:
            centroids_cache = Path(args.ibge_centroids_cache).resolve()
            requested_codes = nodes["municipality_code"].fillna("").astype(str).str.strip().tolist()
            centroid_scope = "all_nodes"
            if args.include_all_ibge_municipalities and not args.centroids_for_all_ibge_municipalities:
                requested_codes = observed_municipality_codes
                centroid_scope = "observed_nodes_only"

            requested_ibge_ids = resolve_codes_to_ibge_ids(requested_codes, municipality_catalog)
            municipality_centers = load_or_refresh_ibge_centroids(
                centroids_cache,
                requested_ibge_ids,
            )

        nodes, municipality_stats = enrich_nodes_with_municipality_metadata(
            nodes,
            municipality_catalog,
            municipality_centers=municipality_centers,
            assign_facility_coordinates_from_municipality=not args.disable_facility_centroid_fallback,
        )
        enrichment_summary["municipality_enrichment"] = {
            "ibge_cache": str(ibge_cache),
            "municipality_nodes_total": municipality_stats.municipality_nodes_total,
            "municipality_nodes_enriched": municipality_stats.municipality_nodes_enriched,
            "municipality_nodes_with_coordinates": municipality_stats.municipality_nodes_with_coordinates,
            "facility_nodes_total": municipality_stats.facility_nodes_total,
            "facility_nodes_named": municipality_stats.facility_nodes_named,
            "facility_nodes_with_coordinates": municipality_stats.facility_nodes_with_coordinates,
            "missing_municipality_metadata": municipality_stats.missing_municipality_metadata,
            "facility_centroid_fallback_enabled": not args.disable_facility_centroid_fallback,
            "all_ibge_municipalities_added": int(all_ibge_municipalities_added),
        }
        if args.enrich_centroids:
            enrichment_summary["municipality_enrichment"]["ibge_centroids_cache"] = str(centroids_cache)
            enrichment_summary["municipality_enrichment"]["centroid_entries_loaded"] = int(
                len(municipality_centers or {})
            )
            enrichment_summary["municipality_enrichment"]["centroid_scope"] = centroid_scope

    if args.enrich_facilities_cnes_api:
        cnes_cache = Path(args.cnes_cache).resolve()
        facility_ids = (
            nodes.loc[nodes["node_type"].astype(str).str.lower() == "facility", "node_id"]
            .astype(str)
            .str.replace("^facility:", "", regex=True)
            .str.strip()
            .tolist()
        )
        cnes_reference = load_or_refresh_cnes_facility_reference(
            cnes_cache,
            facility_ids,
            timeout_seconds=max(1, int(args.cnes_timeout)),
        )
        cnes_updated_rows = 0
        if not cnes_reference.empty:
            nodes, cnes_updated_rows = enrich_nodes_with_facility_reference(
                nodes,
                cnes_reference,
                facility_id_column="cnes",
            )

        enrichment_summary["facility_cnes_api_enrichment"] = {
            "cache": str(cnes_cache),
            "requested_facilities": int(len(facility_ids)),
            "resolved_facilities": int(len(cnes_reference)),
            "updated_rows": int(cnes_updated_rows),
            "timeout_seconds": int(args.cnes_timeout),
        }

    if args.facility_reference_input:
        reference_path = Path(args.facility_reference_input).resolve()
        if not reference_path.exists():
            print(f"Facility reference input not found: {reference_path}")
            return 2

        facility_reference = pd.read_csv(reference_path, sep=args.delimiter, encoding="utf-8-sig", dtype=str)
        nodes, updated_rows = enrich_nodes_with_facility_reference(
            nodes,
            facility_reference,
            facility_id_column=args.facility_reference_id_column,
        )
        enrichment_summary["facility_reference_enrichment"] = {
            "reference_input": str(reference_path),
            "id_column": args.facility_reference_id_column,
            "updated_rows": int(updated_rows),
        }

    nodes_output = Path(args.nodes_output).resolve()
    _write_csv(nodes, nodes_output, args.delimiter)

    if args.facility_lookup_output:
        _write_csv(facility_lookup, Path(args.facility_lookup_output).resolve(), args.delimiter)

    if args.municipality_lookup_output:
        _write_csv(municipality_lookup, Path(args.municipality_lookup_output).resolve(), args.delimiter)

    summary: dict[str, object] = {
        "records_input": str(records_input),
        "records_rows": int(len(records)),
        "nodes_output": str(nodes_output),
        "nodes_total": int(len(nodes)),
        "facility_nodes": int((nodes["node_type"] == "facility").sum()),
        "municipality_nodes": int((nodes["node_type"] == "municipality").sum()),
        "facility_lookup_rows": int(len(facility_lookup)),
        "municipality_lookup_rows": int(len(municipality_lookup)),
        "mapping_fields": {
            "facility_id_field": metadata.facility_id_field,
            "facility_municipality_field": metadata.facility_municipality_field,
            "municipality_fields": metadata.municipality_fields,
        },
        "nodes_with_name": int(nodes["name"].fillna("").astype(str).str.strip().ne("").sum()),
        "nodes_with_latitude": int(nodes["latitude"].notna().sum()) if "latitude" in nodes.columns else 0,
        "nodes_with_longitude": int(nodes["longitude"].notna().sum()) if "longitude" in nodes.columns else 0,
    }

    if enrichment_summary:
        summary["enrichment"] = enrichment_summary

    if args.edges_input:
        edges_input = Path(args.edges_input).resolve()
        if not edges_input.exists():
            print(f"Edges input not found: {edges_input}")
            return 2

        edges = pd.read_csv(edges_input, sep=args.delimiter, encoding="utf-8-sig", dtype=str)
        mapped_edges = map_edges_to_node_ids(edges, known_nodes=nodes)
        edges_output = Path(args.edges_output).resolve()
        _write_csv(mapped_edges, edges_output, args.delimiter)

        coverage = summarize_edge_node_coverage(mapped_edges)
        summary.update(
            {
                "edges_input": str(edges_input),
                "edges_output": str(edges_output),
                "edge_node_coverage": coverage,
            }
        )

    if args.summary_output:
        summary_output = Path(args.summary_output).resolve()
        summary_output.parent.mkdir(parents=True, exist_ok=True)
        summary_output.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"Nodes written: {len(nodes)}")
    print(f"Facility nodes: {(nodes['node_type'] == 'facility').sum()}")
    print(f"Municipality nodes: {(nodes['node_type'] == 'municipality').sum()}")
    if "edge_node_coverage" in summary:
        coverage = summary["edge_node_coverage"]
        assert isinstance(coverage, dict)
        print(
            "Edge-node coverage: "
            f"source={coverage['source_coverage_ratio']} "
            f"target={coverage['target_coverage_ratio']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
