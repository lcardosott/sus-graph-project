#!/usr/bin/env python3
"""Schema gate for transfer matching preconditions.

This tool enforces hard preconditions before transfer inference can run.
The gate intentionally fails when day-level admission/discharge timestamps
are missing, or when transfer codebook mapping is not validated.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path

SIASUS_REQUIRED_FIELDS = {
    "PA_SEXO",
    "PA_IDADE",
    "PA_CIDPRI",
    "PA_CODUNI",
}

SIH_REQUIRED_FIELDS = {
    "SEXO",
    "IDADE",
    "DIAG_PRINC",
}

PATIENT_KEY_CANDIDATES = (
    "PA_CNSMED",
    "PA_CNPJCPF",
    "PA_AUTORIZ",
    "N_AIH",
    "NUM_PROC",
)

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

DESTINATION_CANDIDATES = (
    "PA_MUNPCN",
    "DEST_CODUNI",
    "CNES_DEST",
    "MUNIC_RES",
    "CODMUNRES",
    "CNES",
    "PA_CODUNI",
)

ORIGIN_FACILITY_CANDIDATES = (
    "PA_CODUNI",
    "CNES",
    "CGC_HOSP",
)

TRANSFER_FLAG_CANDIDATES = ("PA_TRANSF",)

TRANSFER_REASON_CANDIDATES = (
    "PA_MOTSAI",
    "MOT_SAIDA",
    "COBRANCA",
)

SEX_FIELD_CANDIDATES = ("PA_SEXO", "SEXO")
AGE_FIELD_CANDIDATES = ("PA_IDADE", "IDADE")
DIAGNOSIS_FIELD_CANDIDATES = ("PA_CIDPRI", "DIAG_PRINC")

DATETIME_PAIR_CANDIDATES = (
    ("DT_INTER", "DT_SAIDA"),
    ("DT_INTER", "DT_ALTA"),
    ("DT_ADMISSAO", "DT_ALTA"),
    ("DATA_ENTRADA", "DATA_SAIDA"),
    ("PA_DT_INTER", "PA_DT_ALTA"),
)

TRUE_VALUES = {"1", "true", "t", "yes", "y", "sim", "s"}


@dataclass
class SchemaGateResult:
    source_file: str
    total_columns: int
    source_hint: str
    profile: str
    missing_required_fields: list[str]
    patient_key_field: str | None
    destination_field: str | None
    origin_field: str | None
    transfer_flag_field: str | None
    transfer_reason_field: str | None
    sex_field: str | None
    age_field: str | None
    diagnosis_field: str | None
    datetime_pair: list[str] | None
    probabilistic_birthdate_field: str | None
    probabilistic_residence_field: str | None
    probabilistic_linkage_supported: bool
    blockers: list[str]
    warnings: list[str]
    passed: bool


def _to_bool(raw_value: str | None) -> bool:
    if raw_value is None:
        return False
    return raw_value.strip().lower() in TRUE_VALUES


def read_header(file_path: Path, delimiter: str) -> list[str]:
    with file_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.reader(csv_file, delimiter=delimiter)
        for row in reader:
            if row:
                return [column.strip() for column in row if column is not None]
    raise ValueError(f"Could not read a header row from {file_path}")


def detect_source_hint(header_set: set[str]) -> str:
    if {"PA_CODUNI", "PA_PROC_ID", "PA_CMP"}.issubset(header_set):
        return "SIASUS-like (PA_* namespace)"
    if {"MUNIC_RES", "DIAG_PRINC", "DT_INTER", "DT_SAIDA"}.issubset(header_set):
        return "SIH-like"
    return "unknown"


def select_profile_required_fields(source_hint: str) -> tuple[str, set[str]]:
    if source_hint == "SIH-like":
        return "sih", SIH_REQUIRED_FIELDS
    if source_hint == "SIASUS-like (PA_* namespace)":
        return "siasus", SIASUS_REQUIRED_FIELDS
    return "unknown", SIASUS_REQUIRED_FIELDS


def find_first_available(header_set: set[str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in header_set:
            return candidate
    return None


def find_datetime_pair(header_set: set[str]) -> list[str] | None:
    for candidate_pair in DATETIME_PAIR_CANDIDATES:
        if set(candidate_pair).issubset(header_set):
            return list(candidate_pair)
    return None


def validate_motsai_codebook(codebook_path: Path) -> tuple[bool, str]:
    if not codebook_path.exists():
        return False, f"Transfer codebook file not found: {codebook_path}"

    valid_rows = 0
    with codebook_path.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        expected_columns = {"code", "is_transfer", "validated"}
        if not expected_columns.issubset(set(reader.fieldnames or [])):
            return False, "Transfer codebook must include columns: code,is_transfer,validated"

        for row in reader:
            code = (row.get("code") or "").strip()
            is_transfer = _to_bool(row.get("is_transfer"))
            validated = _to_bool(row.get("validated"))
            if code and is_transfer and validated:
                valid_rows += 1

    if valid_rows == 0:
        return False, "Transfer codebook has zero validated transfer-related codes"

    return True, f"Validated transfer-related PA_MOTSAI codes: {valid_rows}"


def evaluate_schema(
    header: list[str],
    source_file: Path,
    codebook_path: Path | None,
) -> SchemaGateResult:
    header_set = set(header)
    profile, required_fields = select_profile_required_fields(detect_source_hint(header_set))
    missing_required_fields = sorted(required_fields - header_set)

    patient_key_field = find_first_available(header_set, PATIENT_KEY_CANDIDATES)
    destination_field = find_first_available(header_set, DESTINATION_CANDIDATES)
    origin_field = find_first_available(header_set, ORIGIN_FACILITY_CANDIDATES)
    transfer_flag_field = find_first_available(header_set, TRANSFER_FLAG_CANDIDATES)
    transfer_reason_field = find_first_available(header_set, TRANSFER_REASON_CANDIDATES)
    sex_field = find_first_available(header_set, SEX_FIELD_CANDIDATES)
    age_field = find_first_available(header_set, AGE_FIELD_CANDIDATES)
    diagnosis_field = find_first_available(header_set, DIAGNOSIS_FIELD_CANDIDATES)
    datetime_pair = find_datetime_pair(header_set)
    probabilistic_birthdate_field = find_first_available(
        header_set,
        PROBABILISTIC_BIRTHDATE_CANDIDATES,
    )
    probabilistic_residence_field = find_first_available(
        header_set,
        PROBABILISTIC_RESIDENCE_CANDIDATES,
    )
    probabilistic_linkage_supported = bool(
        probabilistic_birthdate_field
        and probabilistic_residence_field
        and sex_field
        and age_field
    )

    blockers: list[str] = []
    warnings: list[str] = []

    if missing_required_fields:
        blockers.append(
            "Missing required profile fields: "
            + ", ".join(missing_required_fields)
        )

    if not patient_key_field and not probabilistic_linkage_supported:
        blockers.append(
            "No patient linkage strategy available. Expected deterministic key ("
            + ", ".join(PATIENT_KEY_CANDIDATES)
            + ") or probabilistic fields ("
            + ", ".join(PROBABILISTIC_BIRTHDATE_CANDIDATES)
            + ") + ("
            + ", ".join(PROBABILISTIC_RESIDENCE_CANDIDATES)
            + ") + sex + age"
        )
    elif not patient_key_field and probabilistic_linkage_supported:
        warnings.append(
            "No deterministic patient key found. Probabilistic linkage is supported by mapped fields."
        )

    if patient_key_field in {"N_AIH", "NUM_PROC"} and probabilistic_linkage_supported:
        warnings.append(
            "Episode identifiers (N_AIH/NUM_PROC) are available but probabilistic linkage can avoid episode keys."
        )

    if not destination_field:
        blockers.append(
            "No destination field found. Expected one of: "
            + ", ".join(DESTINATION_CANDIDATES)
        )

    if not origin_field:
        blockers.append(
            "No origin facility field found. Expected one of: "
            + ", ".join(ORIGIN_FACILITY_CANDIDATES)
        )

    if not transfer_flag_field and not transfer_reason_field:
        blockers.append(
            "No transfer trigger field found. Expected at least one of: "
            + ", ".join(TRANSFER_FLAG_CANDIDATES + TRANSFER_REASON_CANDIDATES)
        )

    if not sex_field or not age_field or not diagnosis_field:
        blockers.append(
            "Could not map required demographic/clinical fields (sex, age, diagnosis) from header"
        )

    if not datetime_pair:
        blockers.append(
            "No day-level admission/discharge datetime pair found. "
            "24-48h transfer matching is blocked."
        )
        if {"PA_CMP", "PA_MVM"}.issubset(header_set):
            warnings.append(
                "Found month-level fields (PA_CMP, PA_MVM). "
                "Month-only granularity is not accepted for transfer matching."
            )

    if codebook_path is None:
        warnings.append("Transfer codebook validation was skipped.")
    elif transfer_reason_field is None:
        warnings.append("Transfer reason field not found; codebook validation was skipped.")
    else:
        codebook_ok, codebook_message = validate_motsai_codebook(codebook_path)
        if not codebook_ok:
            blockers.append(codebook_message)
        else:
            warnings.append(codebook_message)

    passed = len(blockers) == 0
    return SchemaGateResult(
        source_file=str(source_file),
        total_columns=len(header),
        source_hint=detect_source_hint(header_set),
        profile=profile,
        missing_required_fields=missing_required_fields,
        patient_key_field=patient_key_field,
        destination_field=destination_field,
        origin_field=origin_field,
        transfer_flag_field=transfer_flag_field,
        transfer_reason_field=transfer_reason_field,
        sex_field=sex_field,
        age_field=age_field,
        diagnosis_field=diagnosis_field,
        datetime_pair=datetime_pair,
        probabilistic_birthdate_field=probabilistic_birthdate_field,
        probabilistic_residence_field=probabilistic_residence_field,
        probabilistic_linkage_supported=probabilistic_linkage_supported,
        blockers=blockers,
        warnings=warnings,
        passed=passed,
    )


def format_result(result: SchemaGateResult) -> str:
    status = "PASS" if result.passed else "FAIL"
    lines = [
        f"Schema Gate: {status}",
        f"Source file: {result.source_file}",
        f"Detected source hint: {result.source_hint}",
        f"Profile: {result.profile}",
        f"Columns: {result.total_columns}",
        f"Patient key field: {result.patient_key_field}",
        f"Destination field: {result.destination_field}",
        f"Origin field: {result.origin_field}",
        f"Transfer flag field: {result.transfer_flag_field}",
        f"Transfer reason field: {result.transfer_reason_field}",
        f"Sex field: {result.sex_field}",
        f"Age field: {result.age_field}",
        f"Diagnosis field: {result.diagnosis_field}",
        f"Datetime pair: {result.datetime_pair}",
        f"Probabilistic birthdate field: {result.probabilistic_birthdate_field}",
        f"Probabilistic residence field: {result.probabilistic_residence_field}",
        f"Probabilistic linkage supported: {result.probabilistic_linkage_supported}",
    ]

    if result.blockers:
        lines.append("Blockers:")
        for blocker in result.blockers:
            lines.append(f"  - {blocker}")

    if result.warnings:
        lines.append("Notes:")
        for warning in result.warnings:
            lines.append(f"  - {warning}")

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    default_codebook_path = Path(__file__).resolve().parent / "reference" / "motsai_transfer_codes.csv"

    parser = argparse.ArgumentParser(description="Run transfer matching schema gate checks.")
    parser.add_argument(
        "--input",
        required=True,
        help="Input CSV file path used for schema inspection.",
    )
    parser.add_argument(
        "--delimiter",
        default=";",
        help="CSV delimiter used by the source file.",
    )
    parser.add_argument(
        "--motsai-codebook",
        default=str(default_codebook_path),
        help="CSV path with PA_MOTSAI transfer code mapping.",
    )
    parser.add_argument(
        "--skip-codebook-check",
        action="store_true",
        help="Skip PA_MOTSAI codebook validation.",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Always return exit code 0, even if blockers are found.",
    )
    parser.add_argument(
        "--json-output",
        help="Optional path to write full JSON gate report.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_file = Path(args.input).resolve()
    header = read_header(source_file, args.delimiter)

    codebook_path = None
    if not args.skip_codebook_check:
        codebook_path = Path(args.motsai_codebook).resolve()

    result = evaluate_schema(header, source_file, codebook_path)
    print(format_result(result))

    if args.json_output:
        output_path = Path(args.json_output).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as json_file:
            json.dump(asdict(result), json_file, indent=2, ensure_ascii=True)

    if result.passed or args.report_only:
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())