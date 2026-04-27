#!/usr/bin/env python3
"""Strict column projection for SIH/SIASUS data files.

This module enforces a whitelist of analytics columns to reduce memory usage
and standardize downstream processing.
"""

from __future__ import annotations

import itertools
import importlib
from pathlib import Path

import pandas as pd
from dbfread import DBF
from dbfread.field_parser import FieldParser


STRICT_ANALYTICS_COLUMNS = [
    "CODMUNRES",
    "CNES",
    "DIAG_PRINC",
    "NASC",
    "IDADE",
    "SEXO",
    "DT_INTER",
    "DT_SAIDA",
    "MOT_SAIDA",
    "PA_TRANSF",
    "DIAS_PERM",
    "MARCA_UTI",
    "PROC_REA",
    "VAL_TOT",
    "RACA_COR",
]

OPTIONAL_LINKAGE_COLUMNS = [
    "N_AIH",
    "NUM_PROC",
    "PA_CNSMED",
    "PA_CNPJCPF",
]

COLUMN_ALIASES = {
    "CODMUNRES": ("CODMUNRES", "MUNIC_RES", "PA_MUNPCN"),
    "CNES": ("CNES", "PA_CODUNI", "CGC_HOSP"),
    "DIAG_PRINC": ("DIAG_PRINC", "PA_CIDPRI"),
    "NASC": ("NASC", "DT_NASC", "DT_NASCIMENTO", "PA_NASC"),
    "IDADE": ("IDADE", "PA_IDADE"),
    "SEXO": ("SEXO", "PA_SEXO"),
    "DT_INTER": ("DT_INTER",),
    "DT_SAIDA": ("DT_SAIDA",),
    "MOT_SAIDA": ("MOT_SAIDA", "COBRANCA", "PA_MOTSAI"),
    "PA_TRANSF": ("PA_TRANSF",),
    "DIAS_PERM": ("DIAS_PERM", "PA_PERMAN"),
    "MARCA_UTI": ("MARCA_UTI", "UTI_MES_TO", "UTI_INT_TO"),
    "PROC_REA": ("PROC_REA", "PA_PROC_ID"),
    "VAL_TOT": ("VAL_TOT", "PA_VALAPR", "PA_VALPRO"),
    "RACA_COR": ("RACA_COR", "PA_RACACOR"),
}

TRANSFER_CODES = {"31", "32"}
DISCHARGE_CODES = {"11", "12", "14", "15", "16", "18"}
PERMANENCE_CODES = {"21"}
DEATH_CODES = {"41", "42", "43"}


class SafeFieldParser(FieldParser):
    """DBF parser that tolerates malformed numeric values by returning None."""

    def parseN(self, field, data):  # noqa: N802
        try:
            return super().parseN(field, data)
        except Exception:  # pylint: disable=broad-except
            return None


def _find_first_available(header_set: set[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in header_set:
            return candidate
    return None


def resolve_projection_map(
    columns: list[str],
    include_linkage: bool = False,
) -> tuple[dict[str, str | None], list[str]]:
    """Resolve canonical columns to available source columns."""
    header_set = set(columns)
    projection_map: dict[str, str | None] = {}
    missing_columns: list[str] = []

    for canonical in STRICT_ANALYTICS_COLUMNS:
        source = _find_first_available(header_set, COLUMN_ALIASES[canonical])
        projection_map[canonical] = source
        if source is None:
            missing_columns.append(canonical)

    if include_linkage:
        for linkage_column in OPTIONAL_LINKAGE_COLUMNS:
            if linkage_column in header_set:
                projection_map[linkage_column] = linkage_column

    return projection_map, missing_columns


def _project_record(record: dict[str, object], projection_map: dict[str, str | None]) -> dict[str, object]:
    projected: dict[str, object] = {}
    for canonical, source in projection_map.items():
        projected[canonical] = record.get(source) if source is not None else None
    return projected


def project_dataframe(
    frame: pd.DataFrame,
    projection_map: dict[str, str | None],
) -> pd.DataFrame:
    projected = {}
    for canonical, source in projection_map.items():
        if source is None:
            projected[canonical] = pd.Series([pd.NA] * len(frame))
        else:
            projected[canonical] = frame[source]
    return pd.DataFrame(projected)


def load_projected_from_dbf(
    dbf_path: Path,
    include_linkage: bool = False,
    max_rows: int | None = None,
) -> tuple[pd.DataFrame, dict[str, str | None], list[str]]:
    """Load a DBF file and keep only strict analytics columns."""
    table = DBF(str(dbf_path), load=False, encoding="latin1", parserclass=SafeFieldParser)
    source_columns = list(table.field_names)
    projection_map, missing_columns = resolve_projection_map(source_columns, include_linkage=include_linkage)

    iterator = iter(table)
    if max_rows is not None:
        iterator = itertools.islice(iterator, max_rows)

    rows = [_project_record(row, projection_map) for row in iterator]
    frame = pd.DataFrame(rows)
    if frame.empty:
        frame = pd.DataFrame(columns=list(projection_map.keys()))

    return frame, projection_map, missing_columns


def load_projected_from_parquet(
    parquet_path: Path,
    include_linkage: bool = False,
) -> tuple[pd.DataFrame, dict[str, str | None], list[str]]:
    """Load a Parquet file and keep only strict analytics columns."""
    pq = importlib.import_module("pyarrow.parquet")

    schema_names = pq.ParquetFile(str(parquet_path)).schema.names
    projection_map, missing_columns = resolve_projection_map(list(schema_names), include_linkage=include_linkage)

    selected_actual_columns = sorted({source for source in projection_map.values() if source is not None})
    raw = pd.read_parquet(parquet_path, columns=selected_actual_columns)
    projected = project_dataframe(raw, projection_map)
    return projected, projection_map, missing_columns


def add_outcome_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add standardized outcome flags based on MOT_SAIDA code family."""
    enriched = frame.copy()
    if "MOT_SAIDA" not in enriched.columns:
        enriched["MOT_SAIDA"] = pd.NA

    mot = (
        enriched["MOT_SAIDA"]
        .astype("string")
        .str.strip()
        .str.zfill(2)
    )

    enriched["IS_TRANSFER_REGULATED"] = mot.isin(TRANSFER_CODES)
    enriched["IS_DISCHARGE"] = mot.isin(DISCHARGE_CODES)
    enriched["IS_PERMANENCE"] = mot.isin(PERMANENCE_CODES)
    enriched["IS_DEATH"] = mot.isin(DEATH_CODES)

    def classify(code: str | None) -> str:
        if code in TRANSFER_CODES:
            return "transfer"
        if code in DISCHARGE_CODES:
            return "discharge"
        if code in PERMANENCE_CODES:
            return "permanence"
        if code in DEATH_CODES:
            return "death"
        return "other"

    enriched["MOT_SAIDA_GROUP"] = mot.apply(classify)
    return enriched


def save_projected(frame: pd.DataFrame, output_path: Path, delimiter: str = ";") -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = output_path.suffix.lower()
    if suffix == ".parquet":
        frame.to_parquet(output_path, index=False)
    elif suffix == ".csv":
        frame.to_csv(output_path, index=False, sep=delimiter, encoding="utf-8-sig")
    else:
        raise ValueError(f"Unsupported output suffix: {output_path.suffix}")
