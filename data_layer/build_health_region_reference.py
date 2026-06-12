#!/usr/bin/env python3
"""Build municipality and facility health-region references from CNES ST files."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

from dbfread import DBF


UF_PREFIX_TO_CODE = {
    "11": "RO",
    "12": "AC",
    "13": "AM",
    "14": "RR",
    "15": "PA",
    "16": "AP",
    "17": "TO",
    "21": "MA",
    "22": "PI",
    "23": "CE",
    "24": "RN",
    "25": "PB",
    "26": "PE",
    "27": "AL",
    "28": "SE",
    "29": "BA",
    "31": "MG",
    "32": "ES",
    "33": "RJ",
    "35": "SP",
    "41": "PR",
    "42": "SC",
    "43": "RS",
    "50": "MS",
    "51": "MT",
    "52": "GO",
    "53": "DF",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build CNES health-region reference catalogs.")
    parser.add_argument("--st-dir", default="data_layer/raw/cnes_st/2101")
    parser.add_argument("--out-dir", default="data_layer/reference/catalog")
    parser.add_argument("--competence", default="2101")
    parser.add_argument("--delimiter", default=";")
    return parser.parse_args()


def normalize_digits(value: object, width: int | None = None) -> str:
    text = "" if value is None else str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if width and digits:
        return digits[-width:].zfill(width)
    return digits


def normalize_region(value: object) -> str:
    digits = normalize_digits(value)
    if not digits:
        return ""
    return digits[-4:].zfill(4)


def normalize_cnes(value: object) -> str:
    return normalize_digits(value, 7)


def normalize_municipality(value: object) -> str:
    return normalize_digits(value)[:6]


def uf_from_municipality(code: str) -> str:
    return UF_PREFIX_TO_CODE.get(code[:2], "")


def most_common_region(counter: Counter[str]) -> tuple[str, int]:
    valid = Counter({key: value for key, value in counter.items() if key})
    if not valid:
        return "", 0
    return valid.most_common(1)[0]


def build_references(st_dir: Path) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    municipality_counts: dict[str, Counter[str]] = defaultdict(Counter)
    facility_rows: list[dict[str, object]] = []
    files = sorted(st_dir.glob("ST??2101.dbf"))
    rows_read = 0

    for path in files:
        for row in DBF(str(path), encoding="latin1", load=False):
            rows_read += 1
            municipality_code = normalize_municipality(row.get("CODUFMUN"))
            region_code = normalize_region(row.get("REGSAUDE"))
            cnes = normalize_cnes(row.get("CNES"))
            if not municipality_code:
                continue
            uf = uf_from_municipality(municipality_code)
            region_id = f"{uf}-{region_code}" if uf and region_code else ""
            municipality_counts[municipality_code][region_code] += 1
            if cnes:
                facility_rows.append(
                    {
                        "cnes": cnes,
                        "municipality_code": municipality_code,
                        "uf": uf,
                        "health_region_code": region_code,
                        "health_region_id": region_id,
                        "source_file": path.name,
                    }
                )

    municipality_rows: list[dict[str, object]] = []
    conflict_count = 0
    missing_region_count = 0
    for municipality_code, counter in sorted(municipality_counts.items()):
        region_code, count = most_common_region(counter)
        distinct_regions = sorted(key for key in counter if key)
        if len(distinct_regions) > 1:
            conflict_count += 1
        if not region_code:
            missing_region_count += 1
        uf = uf_from_municipality(municipality_code)
        municipality_rows.append(
            {
                "municipality_code": municipality_code,
                "uf": uf,
                "health_region_code": region_code,
                "health_region_id": f"{uf}-{region_code}" if uf and region_code else "",
                "establishment_rows": sum(counter.values()),
                "selected_region_rows": count,
                "distinct_region_codes": "|".join(distinct_regions),
                "region_conflict": len(distinct_regions) > 1,
                "source": "CNES_ST_REGSAUDE_MODE",
            }
        )

    summary = {
        "st_dir": str(st_dir),
        "st_files": len(files),
        "rows_read": rows_read,
        "facility_rows": len(facility_rows),
        "municipality_rows": len(municipality_rows),
        "municipalities_with_region_conflict": conflict_count,
        "municipalities_without_region": missing_region_count,
    }
    return municipality_rows, facility_rows, summary


def write_csv(path: Path, rows: list[dict[str, object]], delimiter: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        if not rows:
            return
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()), delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    municipality_rows, facility_rows, summary = build_references(Path(args.st_dir))

    outputs = {
        "municipality_regions": out_dir / f"municipality_health_regions_cnes_{args.competence}.csv",
        "facility_regions": out_dir / f"facility_health_regions_cnes_{args.competence}.csv",
        "summary": out_dir / f"health_regions_cnes_{args.competence}_summary.json",
    }
    write_csv(outputs["municipality_regions"], municipality_rows, args.delimiter)
    write_csv(outputs["facility_regions"], facility_rows, args.delimiter)
    summary["outputs"] = {key: str(value) for key, value in outputs.items()}
    outputs["summary"].write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
