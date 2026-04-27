#!/usr/bin/env python3
"""Transfer matching engine based on the 4-pillar heuristic.

This module assumes schema-gate preconditions are satisfied before execution.
"""

from __future__ import annotations

import csv
import hashlib
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


TRUE_VALUES = {"1", "true", "t", "yes", "y", "sim", "s"}

PROBABILISTIC_BIRTHDATE_CANDIDATES = (
    "NASC",
    "DT_NASC",
    "DT_NASCIMENTO",
    "PA_NASC",
)

PROBABILISTIC_RESIDENCE_CANDIDATES = (
    "MUNIC_RES",
    "CODMUNRES",
    "PA_MUNPCN",
)


@dataclass(frozen=True)
class TransferMatchConfig:
    patient_key_col: str = "PA_CNSMED"
    transfer_flag_col: str = "PA_TRANSF"
    discharge_reason_col: str = "PA_MOTSAI"
    sex_col: str = "PA_SEXO"
    age_col: str = "PA_IDADE"
    icd_col: str = "PA_CIDPRI"
    origin_facility_col: str = "PA_CODUNI"
    admission_datetime_col: str = "DT_INTER"
    discharge_datetime_col: str = "DT_SAIDA"
    min_window_hours: int = 24
    max_window_hours: int = 48
    match_method: str = "4pillars_strict"
    rule_version: str = "v1_4pillars_strict_2026_04_08"
    assumption_revision: str = "2026-04-08"
    target_location_type: str = "facility"


def _to_bool(raw_value: str | None) -> bool:
    if raw_value is None:
        return False
    return raw_value.strip().lower() in TRUE_VALUES


def load_validated_transfer_codes(codebook_path: Path) -> set[str]:
    """Load validated PA_MOTSAI transfer-related codes from CSV codebook."""
    if not codebook_path.exists():
        raise FileNotFoundError(f"Transfer codebook file not found: {codebook_path}")

    validated_codes: set[str] = set()
    with codebook_path.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        required_columns = {"code", "is_transfer", "validated"}
        if not required_columns.issubset(set(reader.fieldnames or [])):
            raise ValueError("Transfer codebook must include: code,is_transfer,validated")

        for row in reader:
            code = (row.get("code") or "").strip()
            if not code:
                continue
            if _to_bool(row.get("is_transfer")) and _to_bool(row.get("validated")):
                validated_codes.add(code)

    return validated_codes


def _stable_hash(parts: list[str]) -> str:
    payload = "|".join(parts).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()[:20]


def _normalize_age_series(series: pd.Series) -> pd.Series:
    numeric_age = pd.to_numeric(series, errors="coerce")
    in_range = numeric_age.where((numeric_age >= 0) & (numeric_age <= 130))
    integer_only = in_range.where((in_range % 1 == 0) | in_range.isna())
    return integer_only.astype("Int64")


def _first_existing_column(columns: pd.Index, candidates: tuple[str, ...]) -> str | None:
    column_set = set(columns)
    for candidate in candidates:
        if candidate in column_set:
            return candidate
    return None


def build_probabilistic_patient_key(
    records: pd.DataFrame,
    sex_col: str,
    age_col: str,
    birthdate_candidates: tuple[str, ...] = PROBABILISTIC_BIRTHDATE_CANDIDATES,
    residence_candidates: tuple[str, ...] = PROBABILISTIC_RESIDENCE_CANDIDATES,
) -> tuple[pd.Series, dict[str, str | None]]:
    """Build a patient linkage key from demographic fields (no episode identifiers)."""
    birthdate_field = _first_existing_column(records.columns, birthdate_candidates)
    residence_field = _first_existing_column(records.columns, residence_candidates)

    metadata = {
        "birthdate_field": birthdate_field,
        "residence_field": residence_field,
        "sex_field": sex_col if sex_col in records.columns else None,
        "age_field": age_col if age_col in records.columns else None,
    }

    empty = pd.Series([""] * len(records), index=records.index, dtype="string")
    if birthdate_field is None or residence_field is None:
        return empty, metadata
    if sex_col not in records.columns or age_col not in records.columns:
        return empty, metadata

    birthdate_norm = pd.to_datetime(records[birthdate_field], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    sex_norm = records[sex_col].where(records[sex_col].notna(), "").astype(str).str.strip().str.upper()
    age_norm = _normalize_age_series(records[age_col]).astype("string").fillna("")
    residence_norm = (
        records[residence_field]
        .where(records[residence_field].notna(), "")
        .astype(str)
        .str.strip()
    )

    valid_rows = (
        birthdate_norm.ne("")
        & sex_norm.ne("")
        & age_norm.ne("")
        & residence_norm.ne("")
    )
    key_values = np.where(
        valid_rows,
        birthdate_norm + "|" + sex_norm + "|" + age_norm + "|" + residence_norm,
        "",
    )
    return pd.Series(key_values, index=records.index, dtype="string"), metadata


def _normalize_icd(icd_value: object) -> str | None:
    if pd.isna(icd_value):
        return None
    sanitized = re.sub(r"[^A-Z0-9]", "", str(icd_value).upper())
    if not sanitized:
        return None
    return sanitized


def icd10_chapter(icd_code: str | None) -> str | None:
    """Return an ICD-10 chapter key used for continuity checks."""
    if not icd_code:
        return None

    match = re.match(r"^([A-Z])([0-9]{2})?", icd_code)
    if not match:
        return None

    letter = match.group(1)
    number = int(match.group(2)) if match.group(2) is not None else None

    if letter in {"A", "B"}:
        return "I"
    if letter == "C":
        return "II"
    if letter == "D":
        if number is None:
            return None
        return "II" if number <= 48 else "III"
    if letter == "E":
        return "IV"
    if letter == "F":
        return "V"
    if letter == "G":
        return "VI"
    if letter == "H":
        if number is None:
            return None
        return "VII" if number <= 59 else "VIII"
    if letter == "I":
        return "IX"
    if letter == "J":
        return "X"
    if letter == "K":
        return "XI"
    if letter == "L":
        return "XII"
    if letter == "M":
        return "XIII"
    if letter == "N":
        return "XIV"
    if letter == "O":
        return "XV"
    if letter == "P":
        return "XVI"
    if letter == "Q":
        return "XVII"
    if letter == "R":
        return "XVIII"
    if letter in {"S", "T"}:
        return "XIX"
    if letter in {"V", "W", "X", "Y"}:
        return "XX"
    if letter == "Z":
        return "XXI"
    if letter == "U":
        return "XXII"
    return None


def _prepare_matching_frame(
    records: pd.DataFrame,
    transfer_reason_codes: set[str],
    config: TransferMatchConfig,
) -> pd.DataFrame:
    frame = records.copy()
    frame["__row_id"] = np.arange(len(frame), dtype=np.int64)

    patient_raw = frame[config.patient_key_col]
    frame["__patient"] = patient_raw.where(patient_raw.notna(), "").astype(str).str.strip()
    frame["__admission_dt"] = pd.to_datetime(frame[config.admission_datetime_col], errors="coerce")
    frame["__discharge_dt"] = pd.to_datetime(frame[config.discharge_datetime_col], errors="coerce")
    frame["__sex"] = frame[config.sex_col].where(frame[config.sex_col].notna(), "").astype(str).str.strip().str.upper()
    frame["__age"] = _normalize_age_series(frame[config.age_col])
    frame["__icd"] = frame[config.icd_col].apply(_normalize_icd)
    frame["__icd_chapter"] = frame["__icd"].apply(icd10_chapter)

    transfer_flag = pd.to_numeric(frame[config.transfer_flag_col], errors="coerce").fillna(0).astype(int)
    discharge_reason = frame[config.discharge_reason_col].where(frame[config.discharge_reason_col].notna(), "").astype(str).str.strip()
    frame["__is_anchor"] = transfer_flag.eq(1) | discharge_reason.isin(transfer_reason_codes)
    return frame


def infer_transfer_events(
    records: pd.DataFrame,
    transfer_reason_codes: set[str],
    config: TransferMatchConfig | None = None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Infer transfer events based on anchor + time + demographic + clinical continuity."""
    cfg = config or TransferMatchConfig()

    if cfg.min_window_hours < 0 or cfg.max_window_hours < 0:
        raise ValueError("Time window hours must be non-negative")
    if cfg.min_window_hours > cfg.max_window_hours:
        raise ValueError("min_window_hours must be <= max_window_hours")

    required_columns = {
        cfg.patient_key_col,
        cfg.transfer_flag_col,
        cfg.discharge_reason_col,
        cfg.sex_col,
        cfg.age_col,
        cfg.icd_col,
        cfg.origin_facility_col,
        cfg.admission_datetime_col,
        cfg.discharge_datetime_col,
    }
    missing_columns = sorted(required_columns - set(records.columns))
    if missing_columns:
        raise ValueError("Missing required columns: " + ", ".join(missing_columns))

    normalized_codes = {str(code).strip() for code in transfer_reason_codes if str(code).strip()}
    frame = _prepare_matching_frame(records, normalized_codes, cfg)

    rejection_counts: Counter[str] = Counter()
    events: list[dict[str, object]] = []

    anchor_rows = frame[frame["__is_anchor"]]
    if anchor_rows.empty:
        return pd.DataFrame(columns=_event_columns()), {}

    missing_patient = anchor_rows[anchor_rows["__patient"] == ""]
    missing_patient_count = int(len(missing_patient))
    if missing_patient_count:
        rejection_counts["missing_patient_key"] += missing_patient_count

    valid_anchors = anchor_rows[anchor_rows["__patient"] != ""]
    for patient_key, patient_group in frame.groupby("__patient", sort=False):
        if not patient_key:
            continue

        group_with_adm = patient_group[patient_group["__admission_dt"].notna()].copy()
        if group_with_adm.empty:
            continue

        group_with_adm = group_with_adm.sort_values(["__admission_dt", "__row_id"], kind="mergesort")
        admission_values = group_with_adm["__admission_dt"].to_numpy(dtype="datetime64[ns]")

        patient_anchors = valid_anchors[valid_anchors["__patient"] == patient_key]
        for _, anchor in patient_anchors.iterrows():
            if pd.isna(anchor["__discharge_dt"]):
                rejection_counts["missing_discharge_datetime"] += 1
                continue

            if not anchor["__sex"] or pd.isna(anchor["__age"]):
                rejection_counts["missing_demographic_data"] += 1
                continue

            if anchor["__icd_chapter"] is None:
                rejection_counts["missing_or_invalid_icd"] += 1
                continue

            window_start = anchor["__discharge_dt"] + pd.Timedelta(hours=cfg.min_window_hours)
            window_end = anchor["__discharge_dt"] + pd.Timedelta(hours=cfg.max_window_hours)

            left = admission_values.searchsorted(np.datetime64(window_start), side="left")
            right = admission_values.searchsorted(np.datetime64(window_end), side="right")
            candidates = group_with_adm.iloc[left:right].copy()
            candidates = candidates[candidates["__row_id"] != anchor["__row_id"]]

            if candidates.empty:
                rejection_counts["no_candidate_in_time_window"] += 1
                continue

            candidates = candidates[
                (candidates["__sex"] == anchor["__sex"])
                & (candidates["__age"] == anchor["__age"])
            ]
            if candidates.empty:
                rejection_counts["demographic_mismatch"] += 1
                continue

            candidates = candidates[candidates["__icd_chapter"] == anchor["__icd_chapter"]]
            if candidates.empty:
                rejection_counts["clinical_discontinuity"] += 1
                continue

            candidates["__delta_hours"] = (
                candidates["__admission_dt"] - anchor["__discharge_dt"]
            ).dt.total_seconds() / 3600.0
            candidates = candidates[
                (candidates["__delta_hours"] >= cfg.min_window_hours)
                & (candidates["__delta_hours"] <= cfg.max_window_hours)
            ]
            if candidates.empty:
                rejection_counts["no_candidate_in_time_window"] += 1
                continue

            candidates["__exact_icd"] = candidates["__icd"] == anchor["__icd"]
            candidates["__target_id"] = candidates[cfg.origin_facility_col].astype(str)
            candidates = candidates.sort_values(
                ["__delta_hours", "__exact_icd", "__target_id"],
                ascending=[True, False, True],
                kind="mergesort",
            )
            best_candidate = candidates.iloc[0]

            source_id = str(anchor[cfg.origin_facility_col])
            target_id = str(best_candidate[cfg.origin_facility_col])
            event_id = _stable_hash(
                [
                    source_id,
                    target_id,
                    str(anchor["__discharge_dt"]),
                    str(best_candidate["__admission_dt"]),
                    str(anchor["__patient"]),
                    cfg.rule_version,
                ]
            )

            events.append(
                {
                    "event_id": event_id,
                    "source_facility_id": source_id,
                    "target_location_id": target_id,
                    "target_location_type": cfg.target_location_type,
                    "source_discharge_datetime": anchor["__discharge_dt"],
                    "target_admission_datetime": best_candidate["__admission_dt"],
                    "delta_hours": float(best_candidate["__delta_hours"]),
                    "match_method": cfg.match_method,
                    "confidence_score": 1.0,
                    "rule_version": cfg.rule_version,
                    "assumption_revision": cfg.assumption_revision,
                }
            )

    event_frame = pd.DataFrame(events, columns=_event_columns())
    return event_frame, dict(rejection_counts)


def aggregate_transfer_edges(events: pd.DataFrame) -> pd.DataFrame:
    """Aggregate event-level transfer matches into edge-level records."""
    if events.empty:
        return pd.DataFrame(columns=_edge_columns())

    grouped = (
        events.groupby(
            [
                "source_facility_id",
                "target_location_id",
                "target_location_type",
                "match_method",
                "rule_version",
                "assumption_revision",
            ],
            dropna=False,
        )
        .agg(
            transfer_count=("event_id", "count"),
            confidence_score=("confidence_score", "mean"),
        )
        .reset_index()
    )

    grouped["distance_km"] = np.nan
    grouped["edge_id"] = grouped.apply(
        lambda row: _stable_hash(
            [
                str(row["source_facility_id"]),
                str(row["target_location_id"]),
                str(row["rule_version"]),
                str(row["assumption_revision"]),
            ]
        ),
        axis=1,
    )

    return grouped[_edge_columns()]


def _event_columns() -> list[str]:
    return [
        "event_id",
        "source_facility_id",
        "target_location_id",
        "target_location_type",
        "source_discharge_datetime",
        "target_admission_datetime",
        "delta_hours",
        "match_method",
        "confidence_score",
        "rule_version",
        "assumption_revision",
    ]


def _edge_columns() -> list[str]:
    return [
        "edge_id",
        "source_facility_id",
        "target_location_id",
        "target_location_type",
        "transfer_count",
        "distance_km",
        "match_method",
        "confidence_score",
        "rule_version",
        "assumption_revision",
    ]