#!/usr/bin/env python3
"""Build national municipality and CNES reference catalogs for modular batches.

This script creates reusable, country-wide reference files that can be shared
across monthly/state batches:
- municipality catalog with coordinates,
- CNES facility catalog with municipality linkage and coordinates.

CNES coordinates are resolved by priority:
1) CNES open-data API cache coordinates,
2) municipality centroid coordinates.
"""

from __future__ import annotations

import argparse
import json
from ftplib import FTP
from pathlib import Path
import re
import sys
import time
from typing import Any

from dbfread import DBF
import pandas as pd
from pyreaddbc import dbc2dbf

try:
    from data_layer.node_enrichment import (
        extract_catalog_municipality_codes,
        load_or_refresh_cnes_facility_reference,
        load_or_refresh_ibge_catalog,
        load_or_refresh_ibge_centroids,
        normalize_cnes_code,
        normalize_municipality_code,
    )
except ModuleNotFoundError:
    current_dir = Path(__file__).resolve().parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    from node_enrichment import (  # type: ignore
        extract_catalog_municipality_codes,
        load_or_refresh_cnes_facility_reference,
        load_or_refresh_ibge_catalog,
        load_or_refresh_ibge_centroids,
        normalize_cnes_code,
        normalize_municipality_code,
    )


DATASUS_FTP_HOST = "ftp.datasus.gov.br"
CNES_ST_REMOTE_DIR = "/dissemin/publicos/CNES/200508_/Dados/ST"

BRAZIL_UFS = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]


TRUE_VALUES = {"1", "S", "SIM", "Y", "YES", "TRUE", "T"}


def _normalize_flag(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw == "":
        return ""
    if raw in {"0", "1"}:
        return raw
    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits in {"0", "1"}:
        return digits
    return raw


def _is_truthy(value: Any) -> bool:
    normalized = _normalize_flag(value)
    return normalized in TRUE_VALUES


def _is_public_administration(niv_dep: Any, nat_jur: Any) -> bool:
    normalized_niv_dep = _normalize_flag(niv_dep)
    if normalized_niv_dep == "1":
        return True

    nat_jur_digits = "".join(ch for ch in str(nat_jur or "") if ch.isdigit())
    return nat_jur_digits.startswith("1")


def _normalize_competence_token(raw_value: str) -> str:
    digits = "".join(ch for ch in str(raw_value).strip() if ch.isdigit())
    if len(digits) == 6:
        # YYYYMM -> YYMM
        return digits[2:]
    if len(digits) == 4:
        return digits
    raise ValueError("competence must be YYMM or YYYYMM")


def _parse_uf_filter(raw_ufs: str) -> list[str]:
    normalized = raw_ufs.strip().upper()
    if normalized in {"ALL", "*"}:
        return BRAZIL_UFS

    selected = sorted({token.strip().upper() for token in normalized.split(",") if token.strip()})
    invalid = [uf for uf in selected if uf not in BRAZIL_UFS]
    if invalid:
        raise ValueError("Invalid UF in --ufs: " + ", ".join(invalid))
    if not selected:
        raise ValueError("--ufs produced an empty selection")
    return selected


def _load_cnes_cache_entries(cache_path: Path) -> pd.DataFrame:
    if not cache_path.exists():
        return pd.DataFrame(columns=["cnes", "name", "municipality_code", "latitude", "longitude"])

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return pd.DataFrame(columns=["cnes", "name", "municipality_code", "latitude", "longitude"])

    facilities = payload.get("facilities")
    if not isinstance(facilities, dict):
        return pd.DataFrame(columns=["cnes", "name", "municipality_code", "latitude", "longitude"])

    rows: list[dict[str, Any]] = []
    for key, value in facilities.items():
        if not isinstance(value, dict):
            continue
        cnes = normalize_cnes_code(key)
        if not cnes:
            continue

        municipality_code = normalize_municipality_code(value.get("municipality_code"))
        if len(municipality_code) >= 6:
            municipality_code = municipality_code[:6]
        else:
            municipality_code = ""

        latitude = pd.to_numeric(value.get("latitude"), errors="coerce")
        longitude = pd.to_numeric(value.get("longitude"), errors="coerce")

        rows.append(
            {
                "cnes": cnes,
                "name": str(value.get("name") or "").strip(),
                "municipality_code": municipality_code,
                "latitude": float(latitude) if pd.notna(latitude) else pd.NA,
                "longitude": float(longitude) if pd.notna(longitude) else pd.NA,
            }
        )

    if not rows:
        return pd.DataFrame(columns=["cnes", "name", "municipality_code", "latitude", "longitude"])

    out = pd.DataFrame(rows)
    out = out.drop_duplicates(subset=["cnes"], keep="first")
    return out


def _open_datasus_ftp(timeout_seconds: int) -> FTP:
    ftp = FTP(DATASUS_FTP_HOST, timeout=timeout_seconds)
    ftp.login()
    ftp.cwd(CNES_ST_REMOTE_DIR)
    return ftp


def _download_cnes_st_files(
    competence: str,
    ufs: list[str],
    download_dir: Path,
    ftp_timeout_seconds: int,
    max_retries: int,
    retry_wait_seconds: float,
) -> list[Path]:
    download_dir.mkdir(parents=True, exist_ok=True)

    ftp = _open_datasus_ftp(ftp_timeout_seconds)
    available = set(ftp.nlst())

    downloaded_files: list[Path] = []
    try:
        for uf in ufs:
            file_name = f"ST{uf}{competence}.dbc"
            if file_name not in available:
                continue

            local_path = download_dir / file_name
            if not local_path.exists() or local_path.stat().st_size == 0:
                temp_path = local_path.with_suffix(local_path.suffix + ".part")
                success = False
                last_error: str | None = None
                for attempt in range(1, max(1, int(max_retries)) + 1):
                    try:
                        if ftp is None:
                            ftp = _open_datasus_ftp(ftp_timeout_seconds)
                        with temp_path.open("wb") as file_handle:
                            ftp.retrbinary(f"RETR {file_name}", file_handle.write)
                        temp_path.replace(local_path)
                        success = True
                        break
                    except Exception as exc:  # pylint: disable=broad-except
                        last_error = f"{type(exc).__name__}: {exc}"
                        if temp_path.exists():
                            temp_path.unlink(missing_ok=True)
                        if ftp is not None:
                            try:
                                ftp.quit()
                            except Exception:  # pylint: disable=broad-except
                                pass
                            ftp = None
                        if attempt < max(1, int(max_retries)):
                            time.sleep(max(0.0, float(retry_wait_seconds)))

                if not success:
                    raise RuntimeError(
                        f"Failed to download {file_name} after {max(1, int(max_retries))} attempts: {last_error}"
                    )

            downloaded_files.append(local_path)
    finally:
        if ftp is not None:
            ftp.quit()

    return downloaded_files


def _ensure_dbf(dbc_path: Path) -> Path:
    dbf_path = dbc_path.with_suffix(".dbf")
    if dbf_path.exists() and dbf_path.stat().st_size > 0:
        return dbf_path

    dbc2dbf(str(dbc_path), str(dbf_path))
    return dbf_path


def _parse_cnes_st_files(dbc_files: list[Path]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for dbc_path in sorted(dbc_files):
        dbf_path = _ensure_dbf(dbc_path)
        table = DBF(str(dbf_path), load=False, encoding="latin1")

        match = re.match(r"^ST([A-Z]{2})(\d{4})\.dbc$", dbc_path.name, flags=re.IGNORECASE)
        uf_from_name = match.group(1).upper() if match else ""

        for row in table:
            cnes = normalize_cnes_code(row.get("CNES"))
            municipality_raw = normalize_municipality_code(row.get("CODUFMUN"))
            if not cnes:
                continue

            if len(municipality_raw) == 7:
                municipality_code = municipality_raw[:6]
                ibge_id = municipality_raw
            elif len(municipality_raw) == 6:
                municipality_code = municipality_raw
                ibge_id = ""
            else:
                continue

            rows.append(
                {
                    "cnes": cnes,
                    "municipality_code": municipality_code,
                    "ibge_id": ibge_id,
                    "uf": uf_from_name,
                    "vinc_sus": _normalize_flag(row.get("VINC_SUS")),
                    "atendhos": _normalize_flag(row.get("ATENDHOS")),
                    "leithosp": _normalize_flag(row.get("LEITHOSP")),
                    "niv_dep": _normalize_flag(row.get("NIV_DEP")),
                    "nat_jur": "".join(ch for ch in str(row.get("NAT_JUR") or "") if ch.isdigit()),
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "cnes",
                "municipality_code",
                "ibge_id",
                "uf",
                "vinc_sus",
                "atendhos",
                "leithosp",
                "niv_dep",
                "nat_jur",
                "is_sus_linked",
                "has_hospital_care",
                "has_hospital_beds",
                "is_public_admin",
                "is_public_hospital",
            ]
        )

    out = pd.DataFrame(rows)
    out = out.drop_duplicates(subset=["cnes"], keep="first")

    out["is_sus_linked"] = out["vinc_sus"].apply(lambda value: 1 if _is_truthy(value) else 0)
    out["has_hospital_care"] = out["atendhos"].apply(lambda value: 1 if _is_truthy(value) else 0)
    out["has_hospital_beds"] = out["leithosp"].apply(lambda value: 1 if _is_truthy(value) else 0)
    out["is_public_admin"] = [
        1 if _is_public_administration(niv_dep, nat_jur) else 0
        for niv_dep, nat_jur in zip(out["niv_dep"], out["nat_jur"])
    ]
    out["is_public_hospital"] = (
        (out["is_sus_linked"] == 1)
        & (out["is_public_admin"] == 1)
        & ((out["has_hospital_care"] == 1) | (out["has_hospital_beds"] == 1))
    ).astype("int64")

    return out


def _build_municipality_reference(
    municipality_catalog: list[dict[str, Any]],
    centroids_by_ibge: dict[str, Any],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in municipality_catalog:
        ibge_id = normalize_municipality_code(item.get("id"))
        if len(ibge_id) != 7:
            continue

        municipality_code = ibge_id[:6]
        name = str(item.get("nome") or "").strip()

        uf_payload = (((item.get("microrregiao") or {}).get("mesorregiao") or {}).get("UF") or {})
        uf = str(uf_payload.get("sigla") or "").strip().upper()

        center = centroids_by_ibge.get(ibge_id)
        latitude = float(center.latitude) if center is not None else pd.NA
        longitude = float(center.longitude) if center is not None else pd.NA

        rows.append(
            {
                "ibge_id": ibge_id,
                "municipality_code": municipality_code,
                "municipality_name": name,
                "uf": uf,
                "latitude": latitude,
                "longitude": longitude,
                "coordinate_source": "ibge_malha" if center is not None else "missing",
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["latitude"] = pd.to_numeric(out["latitude"], errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"], errors="coerce")

    has_coordinates = out["latitude"].notna() & out["longitude"].notna()
    uf_means = (
        out.loc[has_coordinates]
        .groupby("uf", dropna=False)[["latitude", "longitude"]]
        .mean()
        .reset_index()
    )
    uf_mean_map = {
        row["uf"]: (float(row["latitude"]), float(row["longitude"]))
        for _, row in uf_means.iterrows()
    }

    national_lat = float(out.loc[has_coordinates, "latitude"].mean()) if has_coordinates.any() else -15.793889
    national_lon = float(out.loc[has_coordinates, "longitude"].mean()) if has_coordinates.any() else -47.882778

    for idx in out.index:
        if pd.notna(out.at[idx, "latitude"]) and pd.notna(out.at[idx, "longitude"]):
            continue

        uf = str(out.at[idx, "uf"] or "").strip().upper()
        fallback = uf_mean_map.get(uf)
        if fallback is not None:
            out.at[idx, "latitude"] = fallback[0]
            out.at[idx, "longitude"] = fallback[1]
            out.at[idx, "coordinate_source"] = "uf_mean_fallback"
        else:
            out.at[idx, "latitude"] = national_lat
            out.at[idx, "longitude"] = national_lon
            out.at[idx, "coordinate_source"] = "national_mean_fallback"

    return out


def _merge_cnes_with_municipalities(
    cnes_base: pd.DataFrame,
    municipalities: pd.DataFrame,
    cnes_cache: pd.DataFrame,
    competence: str,
) -> pd.DataFrame:
    if cnes_base.empty:
        return pd.DataFrame(
            columns=[
                "cnes",
                "competence",
                "ibge_id",
                "municipality_code",
                "municipality_name",
                "uf",
                "vinc_sus",
                "atendhos",
                "leithosp",
                "niv_dep",
                "nat_jur",
                "is_sus_linked",
                "has_hospital_care",
                "has_hospital_beds",
                "is_public_admin",
                "is_public_hospital",
                "name",
                "latitude",
                "longitude",
                "coordinate_source",
                "name_source",
            ]
        )

    municipality_cols = [
        "ibge_id",
        "municipality_code",
        "municipality_name",
        "uf",
        "latitude",
        "longitude",
        "coordinate_source",
    ]

    merged = cnes_base.merge(
        municipalities[municipality_cols],
        on=["municipality_code"],
        how="left",
        suffixes=("", "_municipality"),
    )

    if "uf_municipality" in merged.columns:
        merged["uf"] = merged["uf"].fillna("").astype(str).str.strip().str.upper()
        uf_municipality = merged["uf_municipality"].fillna("").astype(str).str.strip().str.upper()
        merged["uf"] = merged["uf"].where(merged["uf"] != "", uf_municipality)
        merged = merged.drop(columns=["uf_municipality"])

    if "ibge_id_municipality" in merged.columns:
        merged["ibge_id"] = merged["ibge_id"].fillna("").astype(str).str.strip()
        merged["ibge_id"] = merged["ibge_id"].where(merged["ibge_id"] != "", merged["ibge_id_municipality"])
        merged = merged.drop(columns=["ibge_id_municipality"])

    cache = cnes_cache.copy()
    if not cache.empty:
        cache["cnes"] = cache["cnes"].astype(str).str.strip()
        cache = cache.drop_duplicates(subset=["cnes"], keep="first")

    merged = merged.merge(
        cache[["cnes", "name", "latitude", "longitude"]],
        on="cnes",
        how="left",
        suffixes=("", "_api"),
    )

    merged["competence"] = competence
    merged["name"] = merged["name"].fillna("").astype(str).str.strip()

    missing_name = merged["name"] == ""
    merged.loc[missing_name, "name"] = (
        "CNES "
        + merged.loc[missing_name, "cnes"].astype(str)
        + " ("
        + merged.loc[missing_name, "municipality_name"].fillna("").astype(str)
        + " - "
        + merged.loc[missing_name, "uf"].fillna("").astype(str)
        + ")"
    )

    merged["name_source"] = "cnes_api"
    merged.loc[missing_name, "name_source"] = "synthetic_municipality"

    merged["latitude"] = pd.to_numeric(merged["latitude"], errors="coerce")
    merged["longitude"] = pd.to_numeric(merged["longitude"], errors="coerce")
    merged["latitude_api"] = pd.to_numeric(merged["latitude_api"], errors="coerce")
    merged["longitude_api"] = pd.to_numeric(merged["longitude_api"], errors="coerce")

    has_api_coordinates = merged["latitude_api"].notna() & merged["longitude_api"].notna()
    merged.loc[has_api_coordinates, "latitude"] = merged.loc[has_api_coordinates, "latitude_api"]
    merged.loc[has_api_coordinates, "longitude"] = merged.loc[has_api_coordinates, "longitude_api"]

    merged["coordinate_source"] = merged["coordinate_source"].fillna("missing")
    merged.loc[has_api_coordinates, "coordinate_source"] = "cnes_api"

    return merged[
        [
            "cnes",
            "competence",
            "ibge_id",
            "municipality_code",
            "municipality_name",
            "uf",
            "vinc_sus",
            "atendhos",
            "leithosp",
            "niv_dep",
            "nat_jur",
            "is_sus_linked",
            "has_hospital_care",
            "has_hospital_beds",
            "is_public_admin",
            "is_public_hospital",
            "name",
            "latitude",
            "longitude",
            "coordinate_source",
            "name_source",
        ]
    ]


def _resolve_output_path(template_path: str, competence: str) -> Path:
    return Path(template_path.format(competence=competence)).resolve()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build national municipality and CNES reference catalogs.")
    parser.add_argument(
        "--competence",
        default="2101",
        help="Competence token for CNES ST files, in YYMM or YYYYMM format.",
    )
    parser.add_argument(
        "--ufs",
        default="ALL",
        help="UF filter (comma-separated, e.g. SP,RJ) or ALL.",
    )
    parser.add_argument(
        "--download-dir",
        default="data_layer/raw/cnes_st/{competence}",
        help="Download directory template for CNES ST files.",
    )
    parser.add_argument(
        "--municipalities-output",
        default="data_layer/reference/catalog/municipalities_br.csv",
        help="Municipality reference output CSV path.",
    )
    parser.add_argument(
        "--cnes-output",
        default="data_layer/reference/catalog/cnes_br_{competence}.csv",
        help="CNES reference output CSV path template.",
    )
    parser.add_argument(
        "--summary-output",
        default="data_layer/reference/catalog/reference_br_{competence}_summary.json",
        help="Summary JSON output path template.",
    )
    parser.add_argument(
        "--ibge-cache",
        default="data_layer/reference/cache/ibge_municipios.json",
        help="IBGE municipalities cache JSON path.",
    )
    parser.add_argument(
        "--ibge-centroids-cache",
        default="data_layer/reference/cache/ibge_municipio_centroids.json",
        help="IBGE municipality centroids cache JSON path.",
    )
    parser.add_argument(
        "--cnes-api-cache",
        default="data_layer/reference/cache/cnes_estabelecimentos.json",
        help="CNES API cache JSON path.",
    )
    parser.add_argument(
        "--cnes-api-refresh-limit",
        type=int,
        default=0,
        help="Optional number of CNES codes to refresh from API (0 keeps cache-only mode).",
    )
    parser.add_argument(
        "--cnes-scope",
        choices=["public_hospitals", "all"],
        default="public_hospitals",
        help="CNES output scope. Default keeps only SUS-linked public hospitals with hospital care/beds.",
    )
    parser.add_argument(
        "--delimiter",
        default=";",
        help="Output CSV delimiter.",
    )
    parser.add_argument(
        "--ibge-timeout",
        type=int,
        default=60,
        help="IBGE API timeout in seconds.",
    )
    parser.add_argument(
        "--cnes-timeout",
        type=int,
        default=60,
        help="CNES API timeout in seconds.",
    )
    parser.add_argument(
        "--ftp-timeout",
        type=int,
        default=180,
        help="DATASUS FTP timeout in seconds.",
    )
    parser.add_argument(
        "--ftp-max-retries",
        type=int,
        default=4,
        help="Maximum retry attempts per CNES ST file download.",
    )
    parser.add_argument(
        "--ftp-retry-wait-seconds",
        type=float,
        default=1.0,
        help="Wait interval between FTP download retries.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    competence = _normalize_competence_token(args.competence)
    selected_ufs = _parse_uf_filter(args.ufs)

    download_dir = _resolve_output_path(args.download_dir, competence)
    municipalities_output = _resolve_output_path(args.municipalities_output, competence)
    cnes_output = _resolve_output_path(args.cnes_output, competence)
    summary_output = _resolve_output_path(args.summary_output, competence)

    ibge_cache = Path(args.ibge_cache).resolve()
    ibge_centroids_cache = Path(args.ibge_centroids_cache).resolve()
    cnes_api_cache = Path(args.cnes_api_cache).resolve()

    municipality_catalog = load_or_refresh_ibge_catalog(ibge_cache, timeout_seconds=max(1, int(args.ibge_timeout)))
    requested_ibge_ids = extract_catalog_municipality_codes(municipality_catalog, output_digits=7)

    centroids = load_or_refresh_ibge_centroids(
        ibge_centroids_cache,
        requested_ibge_ids,
        timeout_seconds=max(1, int(args.ibge_timeout)),
        continue_on_error=True,
        max_retries=4,
        retry_wait_seconds=0.35,
    )

    municipalities_df = _build_municipality_reference(municipality_catalog, centroids)

    dbc_files = _download_cnes_st_files(
        competence=competence,
        ufs=selected_ufs,
        download_dir=download_dir,
        ftp_timeout_seconds=max(30, int(args.ftp_timeout)),
        max_retries=max(1, int(args.ftp_max_retries)),
        retry_wait_seconds=max(0.0, float(args.ftp_retry_wait_seconds)),
    )
    cnes_base = _parse_cnes_st_files(dbc_files)

    if int(args.cnes_api_refresh_limit) > 0 and not cnes_base.empty:
        refresh_codes = cnes_base["cnes"].drop_duplicates().sort_values().head(int(args.cnes_api_refresh_limit)).tolist()
        load_or_refresh_cnes_facility_reference(
            cnes_api_cache,
            refresh_codes,
            timeout_seconds=max(1, int(args.cnes_timeout)),
        )

    cnes_cache_frame = _load_cnes_cache_entries(cnes_api_cache)
    cnes_reference_df = _merge_cnes_with_municipalities(
        cnes_base=cnes_base,
        municipalities=municipalities_df,
        cnes_cache=cnes_cache_frame,
        competence=competence,
    )

    cnes_total_before_scope = int(len(cnes_reference_df))
    if args.cnes_scope == "public_hospitals" and not cnes_reference_df.empty:
        cnes_reference_df = cnes_reference_df[cnes_reference_df["is_public_hospital"] == 1].copy()

    municipalities_output.parent.mkdir(parents=True, exist_ok=True)
    cnes_output.parent.mkdir(parents=True, exist_ok=True)
    summary_output.parent.mkdir(parents=True, exist_ok=True)

    municipalities_df.to_csv(municipalities_output, index=False, sep=args.delimiter, encoding="utf-8-sig")
    cnes_reference_df.to_csv(cnes_output, index=False, sep=args.delimiter, encoding="utf-8-sig")

    municipality_with_ibge = int((municipalities_df["coordinate_source"] == "ibge_malha").sum()) if not municipalities_df.empty else 0
    municipality_with_fallback = int((municipalities_df["coordinate_source"] != "ibge_malha").sum()) if not municipalities_df.empty else 0

    cnes_with_api_coordinates = int((cnes_reference_df["coordinate_source"] == "cnes_api").sum()) if not cnes_reference_df.empty else 0
    cnes_with_municipality_coordinates = int((cnes_reference_df["coordinate_source"] != "cnes_api").sum()) if not cnes_reference_df.empty else 0

    summary = {
        "competence": competence,
        "ufs": selected_ufs,
        "cnes_scope": args.cnes_scope,
        "download_dir": str(download_dir),
        "municipalities_output": str(municipalities_output),
        "cnes_output": str(cnes_output),
        "ibge_cache": str(ibge_cache),
        "ibge_centroids_cache": str(ibge_centroids_cache),
        "cnes_api_cache": str(cnes_api_cache),
        "downloaded_st_files": len(dbc_files),
        "ftp_max_retries": int(args.ftp_max_retries),
        "ftp_retry_wait_seconds": float(args.ftp_retry_wait_seconds),
        "municipalities_total": int(len(municipalities_df)),
        "municipalities_with_ibge_centroid": municipality_with_ibge,
        "municipalities_with_fallback_coordinates": municipality_with_fallback,
        "cnes_total_before_scope": cnes_total_before_scope,
        "cnes_total": int(len(cnes_reference_df)),
        "cnes_public_hospitals": int((cnes_reference_df["is_public_hospital"] == 1).sum()) if not cnes_reference_df.empty else 0,
        "cnes_with_api_coordinates": cnes_with_api_coordinates,
        "cnes_with_municipality_coordinates": cnes_with_municipality_coordinates,
        "cnes_api_refresh_limit": int(args.cnes_api_refresh_limit),
    }

    summary_output.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"Municipalities reference written: {municipalities_output}")
    print(f"CNES reference written: {cnes_output}")
    print(f"Municipalities total: {len(municipalities_df)}")
    print(f"CNES total before scope: {cnes_total_before_scope}")
    print(f"CNES total after scope ({args.cnes_scope}): {len(cnes_reference_df)}")
    print(f"CNES with API coordinates: {cnes_with_api_coordinates}")
    print(f"CNES with municipality fallback coordinates: {cnes_with_municipality_coordinates}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
