#!/usr/bin/env python3
"""Node enrichment utilities for municipality and facility metadata.

Enrichment strategy:
- municipality metadata (name/UF) from IBGE localidades API with local cache,
- optional facility metadata merge from a user-provided reference CSV,
- optional facility georeference lookup from CNES public API with local cache.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Any

import pandas as pd
import requests


IBGE_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
IBGE_MUNICIPIO_MALHA_GEOJSON_URL = (
    "https://servicodados.ibge.gov.br/api/v3/malhas/municipios/"
    "{municipality_code}?formato=application/vnd.geo+json"
)
CNES_ESTABELECIMENTO_URL = "https://apidadosabertos.saude.gov.br/cnes/estabelecimentos/{cnes}"


@dataclass(frozen=True)
class MunicipalityMetadata:
    input_code: str
    ibge_id: str
    name: str
    uf: str


@dataclass(frozen=True)
class MunicipalityCenter:
    ibge_id: str
    latitude: float
    longitude: float


@dataclass(frozen=True)
class MunicipalityEnrichmentStats:
    municipality_nodes_total: int
    municipality_nodes_enriched: int
    municipality_nodes_with_coordinates: int
    facility_nodes_total: int
    facility_nodes_named: int
    facility_nodes_with_coordinates: int
    missing_municipality_metadata: int


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def normalize_municipality_code(value: Any) -> str:
    """Normalize municipality code to digit-only representation."""
    raw = _normalize_text(value)
    float_like_match = re.match(r"^(\d+)\.0+$", raw)
    if float_like_match:
        return float_like_match.group(1)

    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return ""
    if len(digits) > 7:
        return digits[-7:]
    return digits


def normalize_cnes_code(value: Any) -> str:
    """Normalize CNES code to a zero-padded 7-digit string."""
    raw = _normalize_text(value)
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return ""
    if len(digits) > 7:
        digits = digits[-7:]
    return digits.zfill(7)


def extract_catalog_municipality_codes(
    municipality_catalog: list[dict[str, Any]],
    output_digits: int = 6,
) -> list[str]:
    """Extract normalized municipality codes from IBGE catalog.

    output_digits=6 returns the common DataSUS municipality format used in records.
    output_digits=7 returns full IBGE municipality ids.
    """
    if output_digits not in (6, 7):
        raise ValueError("output_digits must be 6 or 7")

    codes: set[str] = set()
    for item in municipality_catalog:
        ibge_id = normalize_municipality_code(item.get("id"))
        if len(ibge_id) != 7:
            continue
        if output_digits == 6:
            codes.add(ibge_id[:6])
        else:
            codes.add(ibge_id)

    return sorted(codes)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _request_json_with_retries(
    url: str,
    timeout_seconds: int,
    session: requests.Session | None = None,
    max_retries: int = 4,
    retry_wait_seconds: float = 0.35,
    allow_404: bool = False,
) -> Any:
    request_client = session or requests
    attempts = max(1, int(max_retries))
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            response = request_client.get(url, timeout=timeout_seconds)
            if response.status_code == 404 and allow_404:
                return None
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(max(0.0, float(retry_wait_seconds)))

    if last_error is not None:
        raise RuntimeError(f"Request failed for {url}: {last_error}") from last_error
    raise RuntimeError(f"Request failed for {url}")


def fetch_cnes_facility_reference(
    cnes_code: str,
    timeout_seconds: int = 60,
    session: requests.Session | None = None,
    max_retries: int = 4,
    retry_wait_seconds: float = 0.35,
) -> dict[str, Any] | None:
    """Fetch facility georeference record from CNES open-data API."""
    normalized_cnes = normalize_cnes_code(cnes_code)
    if not normalized_cnes:
        return None

    url = CNES_ESTABELECIMENTO_URL.format(cnes=normalized_cnes)
    payload = _request_json_with_retries(
        url,
        timeout_seconds=timeout_seconds,
        session=session,
        max_retries=max_retries,
        retry_wait_seconds=retry_wait_seconds,
        allow_404=True,
    )
    if payload is None:
        return None
    if not isinstance(payload, dict):
        return None

    municipality_code = normalize_municipality_code(payload.get("codigo_municipio"))
    if len(municipality_code) == 7:
        municipality_code = municipality_code[:6]
    elif len(municipality_code) > 6:
        municipality_code = municipality_code[-6:]

    name = _normalize_text(payload.get("nome_fantasia"))
    if not name:
        name = _normalize_text(payload.get("nome_razao_social"))

    latitude = _safe_float(payload.get("latitude_estabelecimento_decimo_grau"))
    longitude = _safe_float(payload.get("longitude_estabelecimento_decimo_grau"))

    api_cnes = normalize_cnes_code(payload.get("codigo_cnes"))
    if api_cnes:
        normalized_cnes = api_cnes

    return {
        "cnes": normalized_cnes,
        "name": name,
        "municipality_code": municipality_code,
        "latitude": latitude,
        "longitude": longitude,
    }


def load_or_refresh_cnes_facility_reference(
    cache_path: Path,
    cnes_codes: list[str],
    timeout_seconds: int = 60,
    max_retries: int = 4,
    retry_wait_seconds: float = 0.35,
) -> pd.DataFrame:
    """Load facility georeference records from cache and fetch missing CNES codes."""
    requested_codes = sorted(
        {
            normalize_cnes_code(code)
            for code in cnes_codes
            if normalize_cnes_code(code)
        }
    )

    if not requested_codes:
        return pd.DataFrame(columns=["cnes", "name", "municipality_code", "latitude", "longitude"])

    cached_entries: dict[str, dict[str, Any]] = {}
    if cache_path.exists():
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            facilities = payload.get("facilities")
            if isinstance(facilities, dict):
                for key, value in facilities.items():
                    normalized = normalize_cnes_code(key)
                    if not normalized or not isinstance(value, dict):
                        continue

                    cached_entries[normalized] = {
                        "cnes": normalized,
                        "name": _normalize_text(value.get("name")),
                        "municipality_code": normalize_municipality_code(value.get("municipality_code"))[:6],
                        "latitude": _safe_float(value.get("latitude")),
                        "longitude": _safe_float(value.get("longitude")),
                    }
        except json.JSONDecodeError:
            cached_entries = {}

    missing_codes = [code for code in requested_codes if code not in cached_entries]
    if missing_codes:
        session = requests.Session()
        try:
            for code in missing_codes:
                try:
                    record = fetch_cnes_facility_reference(
                        code,
                        timeout_seconds=timeout_seconds,
                        session=session,
                        max_retries=max_retries,
                        retry_wait_seconds=retry_wait_seconds,
                    )
                except RuntimeError:
                    record = None

                if record is None:
                    continue

                cached_entries[code] = {
                    "cnes": normalize_cnes_code(record.get("cnes")) or code,
                    "name": _normalize_text(record.get("name")),
                    "municipality_code": normalize_municipality_code(record.get("municipality_code"))[:6],
                    "latitude": _safe_float(record.get("latitude")),
                    "longitude": _safe_float(record.get("longitude")),
                }
        finally:
            session.close()

    serialized = {
        code: {
            "name": entry.get("name"),
            "municipality_code": entry.get("municipality_code"),
            "latitude": entry.get("latitude"),
            "longitude": entry.get("longitude"),
        }
        for code, entry in sorted(cached_entries.items())
    }
    cache_payload = {
        "source": CNES_ESTABELECIMENTO_URL,
        "cached_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_cached_facilities": len(serialized),
        "facilities": serialized,
    }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache_payload, ensure_ascii=True), encoding="utf-8")

    records = [cached_entries[code] for code in requested_codes if code in cached_entries]
    if not records:
        return pd.DataFrame(columns=["cnes", "name", "municipality_code", "latitude", "longitude"])

    return pd.DataFrame.from_records(records, columns=["cnes", "name", "municipality_code", "latitude", "longitude"])


def _build_municipality_indexes(catalog: list[dict[str, Any]]) -> tuple[dict[str, MunicipalityMetadata], dict[str, MunicipalityMetadata]]:
    by_7: dict[str, MunicipalityMetadata] = {}
    by_6: dict[str, MunicipalityMetadata] = {}

    for item in catalog:
        ibge_id = normalize_municipality_code(item.get("id"))
        if len(ibge_id) != 7:
            continue

        name = _normalize_text(item.get("nome"))
        microrregiao = item.get("microrregiao") or {}
        if not isinstance(microrregiao, dict):
            microrregiao = {}
        mesorregiao = microrregiao.get("mesorregiao") or {}
        if not isinstance(mesorregiao, dict):
            mesorregiao = {}
        uf_payload = mesorregiao.get("UF") or {}
        if not isinstance(uf_payload, dict):
            uf_payload = {}

        uf = _normalize_text(
            uf_payload.get("sigla")
        )
        metadata = MunicipalityMetadata(
            input_code=ibge_id,
            ibge_id=ibge_id,
            name=name,
            uf=uf,
        )

        by_7[ibge_id] = metadata
        by_6.setdefault(ibge_id[:6], metadata)

    return by_7, by_6


def _resolve_municipality(code: Any, by_7: dict[str, MunicipalityMetadata], by_6: dict[str, MunicipalityMetadata]) -> MunicipalityMetadata | None:
    normalized = normalize_municipality_code(code)
    if not normalized:
        return None

    if len(normalized) == 7:
        return by_7.get(normalized)
    if len(normalized) == 6:
        return by_6.get(normalized)

    if len(normalized) > 7:
        normalized = normalized[-7:]
        return by_7.get(normalized)

    return None


def _resolve_municipality_center(
    code: Any,
    by_7: dict[str, MunicipalityMetadata],
    by_6: dict[str, MunicipalityMetadata],
    centers_by_7: dict[str, MunicipalityCenter],
) -> MunicipalityCenter | None:
    municipality = _resolve_municipality(code, by_7, by_6)
    if municipality is None:
        return None
    return centers_by_7.get(municipality.ibge_id)


def resolve_codes_to_ibge_ids(
    municipality_codes: list[str],
    municipality_catalog: list[dict[str, Any]],
) -> list[str]:
    """Resolve mixed municipality code formats to canonical 7-digit IBGE ids."""
    by_7, by_6 = _build_municipality_indexes(municipality_catalog)
    resolved_ids: set[str] = set()
    for code in municipality_codes:
        municipality = _resolve_municipality(code, by_7, by_6)
        if municipality is not None:
            resolved_ids.add(municipality.ibge_id)
    return sorted(resolved_ids)


def fetch_ibge_municipality_catalog(
    timeout_seconds: int = 60,
    max_retries: int = 4,
    retry_wait_seconds: float = 0.35,
) -> list[dict[str, Any]]:
    payload = _request_json_with_retries(
        IBGE_MUNICIPIOS_URL,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_wait_seconds=retry_wait_seconds,
    )
    if not isinstance(payload, list):
        raise ValueError("Unexpected IBGE municipality catalog payload")

    return payload


def load_or_refresh_ibge_catalog(
    cache_path: Path,
    timeout_seconds: int = 60,
    max_retries: int = 4,
    retry_wait_seconds: float = 0.35,
) -> list[dict[str, Any]]:
    """Load IBGE municipality catalog from cache or refresh from API."""
    if cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            municipios = cached.get("municipios")
            if isinstance(municipios, list) and municipios:
                return municipios
        except json.JSONDecodeError:
            pass

    municipios = fetch_ibge_municipality_catalog(
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_wait_seconds=retry_wait_seconds,
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_payload = {
        "source": IBGE_MUNICIPIOS_URL,
        "cached_at_utc": datetime.now(timezone.utc).isoformat(),
        "municipios": municipios,
    }
    cache_path.write_text(json.dumps(cache_payload, ensure_ascii=True), encoding="utf-8")
    return municipios


def _normalize_ring(ring_payload: Any) -> list[tuple[float, float]]:
    points: list[tuple[float, float]] = []
    if not isinstance(ring_payload, list):
        return points

    for point in ring_payload:
        if not isinstance(point, (list, tuple)) or len(point) < 2:
            continue
        try:
            lon = float(point[0])
            lat = float(point[1])
        except (TypeError, ValueError):
            continue
        points.append((lon, lat))

    if len(points) >= 2 and points[0] != points[-1]:
        points.append(points[0])
    return points


def _iter_exterior_rings(geometry: dict[str, Any]) -> list[list[tuple[float, float]]]:
    geometry_type = _normalize_text(geometry.get("type"))
    coordinates = geometry.get("coordinates")

    rings: list[list[tuple[float, float]]] = []
    if geometry_type == "Polygon":
        if isinstance(coordinates, list) and coordinates:
            ring = _normalize_ring(coordinates[0])
            if len(ring) >= 4:
                rings.append(ring)
        return rings

    if geometry_type == "MultiPolygon":
        if not isinstance(coordinates, list):
            return rings
        for polygon in coordinates:
            if not isinstance(polygon, list) or not polygon:
                continue
            ring = _normalize_ring(polygon[0])
            if len(ring) >= 4:
                rings.append(ring)
        return rings

    return rings


def _ring_centroid(ring: list[tuple[float, float]]) -> tuple[float, float, float] | None:
    if len(ring) < 4:
        return None

    signed_area2 = 0.0
    cx_acc = 0.0
    cy_acc = 0.0

    for idx in range(len(ring) - 1):
        x0, y0 = ring[idx]
        x1, y1 = ring[idx + 1]
        cross = (x0 * y1) - (x1 * y0)
        signed_area2 += cross
        cx_acc += (x0 + x1) * cross
        cy_acc += (y0 + y1) * cross

    if abs(signed_area2) < 1e-12:
        lon = sum(point[0] for point in ring[:-1]) / (len(ring) - 1)
        lat = sum(point[1] for point in ring[:-1]) / (len(ring) - 1)
        return lon, lat, 0.0

    lon = cx_acc / (3.0 * signed_area2)
    lat = cy_acc / (3.0 * signed_area2)
    weight = abs(signed_area2) / 2.0
    return lon, lat, weight


def _feature_geometry_centroid(geometry: dict[str, Any]) -> tuple[float, float] | None:
    rings = _iter_exterior_rings(geometry)
    if not rings:
        return None

    weighted_lon = 0.0
    weighted_lat = 0.0
    total_weight = 0.0
    fallback_lon = 0.0
    fallback_lat = 0.0
    fallback_count = 0

    for ring in rings:
        centroid = _ring_centroid(ring)
        if centroid is None:
            continue
        lon, lat, weight = centroid
        if weight > 0:
            weighted_lon += lon * weight
            weighted_lat += lat * weight
            total_weight += weight
        else:
            fallback_lon += lon
            fallback_lat += lat
            fallback_count += 1

    if total_weight > 0:
        return weighted_lon / total_weight, weighted_lat / total_weight
    if fallback_count > 0:
        return fallback_lon / fallback_count, fallback_lat / fallback_count
    return None


def fetch_ibge_municipality_center(
    municipality_code: str,
    timeout_seconds: int = 60,
    session: requests.Session | None = None,
) -> MunicipalityCenter | None:
    municipality_id = normalize_municipality_code(municipality_code)
    if len(municipality_id) != 7:
        return None

    request_client = session or requests
    url = IBGE_MUNICIPIO_MALHA_GEOJSON_URL.format(municipality_code=municipality_id)
    response = request_client.get(url, timeout=timeout_seconds)
    if response.status_code == 404:
        return None
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, dict):
        return None

    features = payload.get("features")
    if not isinstance(features, list):
        return None

    weighted_lon = 0.0
    weighted_lat = 0.0
    centroid_count = 0
    for feature in features:
        if not isinstance(feature, dict):
            continue
        geometry = feature.get("geometry")
        if not isinstance(geometry, dict):
            continue
        centroid = _feature_geometry_centroid(geometry)
        if centroid is None:
            continue
        lon, lat = centroid
        weighted_lon += lon
        weighted_lat += lat
        centroid_count += 1

    if centroid_count == 0:
        return None

    longitude = weighted_lon / centroid_count
    latitude = weighted_lat / centroid_count
    return MunicipalityCenter(
        ibge_id=municipality_id,
        latitude=latitude,
        longitude=longitude,
    )


def load_or_refresh_ibge_centroids(
    cache_path: Path,
    municipality_codes: list[str],
    timeout_seconds: int = 60,
    continue_on_error: bool = False,
    max_retries: int = 3,
    retry_wait_seconds: float = 0.35,
) -> dict[str, MunicipalityCenter]:
    """Load municipality centroids from cache and fetch missing ones from IBGE."""
    requested_codes = sorted(
        {
            normalize_municipality_code(code)
            for code in municipality_codes
            if len(normalize_municipality_code(code)) == 7
        }
    )

    cached_centroids_raw: dict[str, dict[str, Any]] = {}
    if cache_path.exists():
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            centroids = payload.get("centroids")
            if isinstance(centroids, dict):
                cached_centroids_raw = {
                    str(key): value
                    for key, value in centroids.items()
                    if isinstance(value, dict)
                }
        except json.JSONDecodeError:
            cached_centroids_raw = {}

    centers: dict[str, MunicipalityCenter] = {}
    for code in requested_codes:
        raw_entry = cached_centroids_raw.get(code)
        if raw_entry is None:
            continue
        try:
            latitude = float(raw_entry.get("latitude"))
            longitude = float(raw_entry.get("longitude"))
        except (TypeError, ValueError):
            continue
        centers[code] = MunicipalityCenter(
            ibge_id=code,
            latitude=latitude,
            longitude=longitude,
        )

    missing_codes = [code for code in requested_codes if code not in centers]
    failed_codes: dict[str, str] = {}
    if missing_codes:
        session = requests.Session()
        try:
            for code in missing_codes:
                center: MunicipalityCenter | None = None
                last_error: str | None = None
                for attempt in range(1, max(1, int(max_retries)) + 1):
                    try:
                        center = fetch_ibge_municipality_center(
                            municipality_code=code,
                            timeout_seconds=timeout_seconds,
                            session=session,
                        )
                        last_error = None
                        break
                    except requests.RequestException as exc:
                        last_error = f"{type(exc).__name__}: {exc}"
                        if attempt < max(1, int(max_retries)):
                            time.sleep(max(0.0, float(retry_wait_seconds)))

                if center is None:
                    if last_error:
                        failed_codes[code] = last_error
                        if not continue_on_error:
                            raise RuntimeError(
                                "Failed to fetch centroid for municipality "
                                f"{code} after {max(1, int(max_retries))} attempts: {last_error}"
                            )
                    continue

                centers[code] = center
        finally:
            session.close()

    serialized_centroids = {
        code: {
            "latitude": center.latitude,
            "longitude": center.longitude,
        }
        for code, center in centers.items()
    }
    cache_payload = {
        "source": IBGE_MUNICIPIO_MALHA_GEOJSON_URL,
        "cached_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_cached_centroids": len(serialized_centroids),
        "failed_codes": failed_codes,
        "centroids": serialized_centroids,
    }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache_payload, ensure_ascii=True), encoding="utf-8")

    return centers


def enrich_nodes_with_municipality_metadata(
    nodes: pd.DataFrame,
    municipality_catalog: list[dict[str, Any]],
    municipality_centers: dict[str, MunicipalityCenter] | None = None,
    assign_facility_coordinates_from_municipality: bool = True,
) -> tuple[pd.DataFrame, MunicipalityEnrichmentStats]:
    """Enrich nodes with municipality names and facility labels."""
    required_columns = {"node_id", "node_type", "name", "municipality_code"}
    missing = sorted(required_columns - set(nodes.columns))
    if missing:
        raise ValueError("Nodes frame is missing required columns: " + ", ".join(missing))

    by_7, by_6 = _build_municipality_indexes(municipality_catalog)

    out = nodes.copy()
    out["name"] = out["name"].astype("string")
    out["municipality_code"] = out["municipality_code"].astype("string")
    if "latitude" in out.columns:
        out["latitude"] = pd.to_numeric(out["latitude"], errors="coerce")
    if "longitude" in out.columns:
        out["longitude"] = pd.to_numeric(out["longitude"], errors="coerce")

    centers_by_7 = municipality_centers or {}

    municipality_mask = out["node_type"].astype("string").str.lower().eq("municipality")
    facility_mask = out["node_type"].astype("string").str.lower().eq("facility")

    municipality_total = int(municipality_mask.sum())
    facility_total = int(facility_mask.sum())

    municipality_enriched = 0
    missing_municipality_metadata = 0

    for idx in out[municipality_mask].index:
        metadata = _resolve_municipality(out.at[idx, "municipality_code"], by_7, by_6)
        if metadata is None:
            missing_municipality_metadata += 1
            continue

        if _normalize_text(out.at[idx, "name"]) == "":
            label = metadata.name
            if metadata.uf:
                label = f"{label} - {metadata.uf}"
            out.at[idx, "name"] = label

        center = _resolve_municipality_center(
            out.at[idx, "municipality_code"],
            by_7,
            by_6,
            centers_by_7,
        )
        if center is not None and "latitude" in out.columns and "longitude" in out.columns:
            if pd.isna(out.at[idx, "latitude"]) or pd.isna(out.at[idx, "longitude"]):
                out.at[idx, "latitude"] = center.latitude
                out.at[idx, "longitude"] = center.longitude

        municipality_enriched += 1

    facility_named = 0
    for idx in out[facility_mask].index:
        current_name = _normalize_text(out.at[idx, "name"])
        if current_name:
            facility_named += 1
            continue

        node_id = _normalize_text(out.at[idx, "node_id"])
        facility_id = node_id.split(":", maxsplit=1)[1] if ":" in node_id else node_id
        municipality_metadata = _resolve_municipality(out.at[idx, "municipality_code"], by_7, by_6)

        if municipality_metadata is not None and municipality_metadata.uf:
            out.at[idx, "name"] = f"CNES {facility_id} ({municipality_metadata.name} - {municipality_metadata.uf})"
        elif municipality_metadata is not None:
            out.at[idx, "name"] = f"CNES {facility_id} ({municipality_metadata.name})"
        else:
            out.at[idx, "name"] = f"CNES {facility_id}"

        if assign_facility_coordinates_from_municipality:
            center = _resolve_municipality_center(
                out.at[idx, "municipality_code"],
                by_7,
                by_6,
                centers_by_7,
            )
            if center is not None and "latitude" in out.columns and "longitude" in out.columns:
                if pd.isna(out.at[idx, "latitude"]) or pd.isna(out.at[idx, "longitude"]):
                    out.at[idx, "latitude"] = center.latitude
                    out.at[idx, "longitude"] = center.longitude

        facility_named += 1

    municipality_with_coordinates = int(
        out.loc[municipality_mask, ["latitude", "longitude"]].notna().all(axis=1).sum()
    ) if {"latitude", "longitude"}.issubset(out.columns) else 0
    facility_with_coordinates = int(
        out.loc[facility_mask, ["latitude", "longitude"]].notna().all(axis=1).sum()
    ) if {"latitude", "longitude"}.issubset(out.columns) else 0

    if "latitude" in out.columns:
        out["latitude"] = out["latitude"].astype("Float64")
    if "longitude" in out.columns:
        out["longitude"] = out["longitude"].astype("Float64")

    stats = MunicipalityEnrichmentStats(
        municipality_nodes_total=municipality_total,
        municipality_nodes_enriched=municipality_enriched,
        municipality_nodes_with_coordinates=municipality_with_coordinates,
        facility_nodes_total=facility_total,
        facility_nodes_named=facility_named,
        facility_nodes_with_coordinates=facility_with_coordinates,
        missing_municipality_metadata=missing_municipality_metadata,
    )
    return out, stats


def enrich_nodes_with_facility_reference(
    nodes: pd.DataFrame,
    facility_reference: pd.DataFrame,
    facility_id_column: str,
    reference_column_map: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, int]:
    """Merge optional facility metadata from an external reference CSV."""
    if facility_id_column not in facility_reference.columns:
        raise ValueError(f"Facility reference missing id column: {facility_id_column}")

    column_map = reference_column_map or {
        "name": "name",
        "municipality_code": "municipality_code",
        "latitude": "latitude",
        "longitude": "longitude",
        "capacity_beds": "capacity_beds",
        "habilitation_level": "habilitation_level",
    }

    ref = facility_reference.copy()
    ref[facility_id_column] = ref[facility_id_column].astype(str).str.strip()
    ref = ref[ref[facility_id_column] != ""]
    ref = ref.drop_duplicates(subset=[facility_id_column], keep="first")

    nodes_out = nodes.copy()
    is_facility = nodes_out["node_type"].astype(str).str.lower() == "facility"
    facility_ids = nodes_out.loc[is_facility, "node_id"].astype(str).str.replace("^facility:", "", regex=True)
    nodes_out.loc[is_facility, "__facility_id"] = facility_ids

    merged = nodes_out.merge(
        ref,
        how="left",
        left_on="__facility_id",
        right_on=facility_id_column,
        suffixes=("", "__ref"),
    )

    updated_rows = 0
    for target_col, ref_col in column_map.items():
        if target_col not in merged.columns:
            continue

        if f"{ref_col}__ref" in merged.columns:
            actual_ref_col = f"{ref_col}__ref"
        else:
            actual_ref_col = ref_col

        if actual_ref_col not in merged.columns:
            continue

        ref_values = merged[actual_ref_col]
        can_update = is_facility.reindex(merged.index, fill_value=False) & ref_values.notna() & (ref_values.astype(str).str.strip() != "")
        updated_rows += int(can_update.sum())

        current = merged[target_col]
        merged[target_col] = current.where(~can_update, ref_values)

    if "__facility_id" in merged.columns:
        merged = merged.drop(columns=["__facility_id"])

    if facility_id_column in merged.columns:
        merged = merged.drop(columns=[facility_id_column])

    return merged[nodes.columns], updated_rows
