#!/usr/bin/env python3
"""Select and optionally download SIASUS files by year/month/part.

This utility provides a simple way to choose multiple years for analysis,
generate a manifest, and download only the selected files.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from ftplib import FTP
from pathlib import Path


DEFAULT_SERVER = "ftp.datasus.gov.br"
DEFAULT_REMOTE_DIR = "dissemin/publicos/SIASUS/200801_/Dados"


@dataclass(frozen=True)
class SiasusEntry:
    file_name: str
    year: int
    month: int
    part: str
    size_bytes: int | None


def parse_numeric_ranges(raw_value: str, min_value: int, max_value: int, label: str) -> set[int]:
    """Parse values like "2021", "2020-2022", "1,3,5-7"."""
    selected: set[int] = set()
    tokens = [token.strip() for token in raw_value.split(",") if token.strip()]
    if not tokens:
        raise ValueError(f"{label} is empty")

    for token in tokens:
        if "-" in token:
            start_str, end_str = token.split("-", maxsplit=1)
            start = int(start_str)
            end = int(end_str)
            if start > end:
                raise ValueError(f"Invalid range in {label}: {token}")
            for value in range(start, end + 1):
                if value < min_value or value > max_value:
                    raise ValueError(f"{label} value out of bounds: {value}")
                selected.add(value)
        else:
            value = int(token)
            if value < min_value or value > max_value:
                raise ValueError(f"{label} value out of bounds: {value}")
            selected.add(value)

    return selected


def parse_years(raw_value: str) -> set[int]:
    return parse_numeric_ranges(raw_value, 2008, 2099, "years")


def parse_months(raw_value: str) -> set[int]:
    return parse_numeric_ranges(raw_value, 1, 12, "months")


def filename_pattern(state_prefix: str) -> re.Pattern[str]:
    escaped = re.escape(state_prefix)
    return re.compile(rf"^{escaped}(?P<yy>\d{{2}})(?P<mm>\d{{2}})(?P<part>[a-z])\.dbc$")


def list_filtered_entries(
    ftp: FTP,
    state_prefix: str,
    years: set[int],
    months: set[int],
    parts: set[str],
) -> list[SiasusEntry]:
    pattern = filename_pattern(state_prefix)
    raw_list = ftp.nlst()
    selected: list[SiasusEntry] = []

    for file_name in raw_list:
        match = pattern.match(file_name)
        if not match:
            continue

        year = 2000 + int(match.group("yy"))
        month = int(match.group("mm"))
        part = match.group("part")

        if year not in years or month not in months or part not in parts:
            continue

        file_size: int | None = None
        try:
            raw_size = ftp.size(file_name)
            if raw_size is not None:
                file_size = int(raw_size)
        except Exception:
            file_size = None

        selected.append(
            SiasusEntry(
                file_name=file_name,
                year=year,
                month=month,
                part=part,
                size_bytes=file_size,
            )
        )

    selected.sort(key=lambda item: (item.year, item.month, item.part))
    return selected


def build_manifest(
    state_prefix: str,
    years: set[int],
    months: set[int],
    parts: set[str],
    remote_dir: str,
    entries: list[SiasusEntry],
) -> dict[str, object]:
    totals_by_year: dict[int, dict[str, object]] = {}

    for year in sorted(years):
        totals_by_year[year] = {
            "year": year,
            "months": [],
            "total_files": 0,
            "total_size_bytes": 0,
        }

    entries_by_year_month: dict[tuple[int, int], list[SiasusEntry]] = {}
    for entry in entries:
        key = (entry.year, entry.month)
        entries_by_year_month.setdefault(key, []).append(entry)

    for year in sorted(years):
        for month in sorted(months):
            bucket = entries_by_year_month.get((year, month), [])
            month_size = sum(item.size_bytes or 0 for item in bucket)
            month_payload = {
                "month": month,
                "num_files": len(bucket),
                "parts": [item.part for item in bucket],
                "files": [item.file_name for item in bucket],
                "size_bytes": month_size,
            }
            totals_by_year[year]["months"].append(month_payload)
            totals_by_year[year]["total_files"] += len(bucket)
            totals_by_year[year]["total_size_bytes"] += month_size

    total_files = sum(year_data["total_files"] for year_data in totals_by_year.values())
    total_size = sum(year_data["total_size_bytes"] for year_data in totals_by_year.values())

    return {
        "dataset_path": remote_dir,
        "state_prefix": state_prefix,
        "years": sorted(years),
        "months": sorted(months),
        "parts": sorted(parts),
        "total_files": total_files,
        "total_size_bytes": total_size,
        "years_detail": [totals_by_year[year] for year in sorted(totals_by_year)],
        "selected_entries": [asdict(entry) for entry in entries],
    }


def download_entries(ftp: FTP, entries: list[SiasusEntry], output_dir: Path) -> tuple[int, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    skipped = 0

    for entry in entries:
        target_dir = output_dir / str(entry.year) / f"{entry.month:02d}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / entry.file_name

        if target_file.exists():
            skipped += 1
            continue

        with target_file.open("wb") as file_handle:
            ftp.retrbinary(f"RETR {entry.file_name}", file_handle.write)
        downloaded += 1

    return downloaded, skipped


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Select SIASUS files by year/month/part and optionally download.")
    parser.add_argument("--server", default=DEFAULT_SERVER, help="FTP server host.")
    parser.add_argument("--remote-dir", default=DEFAULT_REMOTE_DIR, help="FTP directory containing SIASUS files.")
    parser.add_argument("--state-prefix", default="PASP", help="File prefix for state selection, for example PASP.")
    parser.add_argument("--years", required=True, help="Years expression, for example 2021 or 2020-2022.")
    parser.add_argument("--months", default="1-12", help="Months expression, for example 1-12 or 3,4,5.")
    parser.add_argument("--parts", default="abc", help="Allowed partition letters, for example abc or a.")
    parser.add_argument("--max-files", type=int, help="Optional limit on selected files (after sorting).")
    parser.add_argument(
        "--manifest-output",
        default="data_layer/reports/siasus_manifest.json",
        help="Manifest JSON output path.",
    )
    parser.add_argument("--download", action="store_true", help="Download selected files to output directory.")
    parser.add_argument(
        "--download-dir",
        default="data_layer/raw/siasus",
        help="Base directory for downloaded DBC files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    years = parse_years(args.years)
    months = parse_months(args.months)
    parts = {ch for ch in args.parts.lower() if ch.isalpha()}
    if not parts:
        raise ValueError("parts must contain at least one letter")

    ftp = FTP(args.server, timeout=120)
    ftp.login()
    ftp.cwd(args.remote_dir)

    entries = list_filtered_entries(
        ftp=ftp,
        state_prefix=args.state_prefix,
        years=years,
        months=months,
        parts=parts,
    )
    if args.max_files is not None:
        entries = entries[: args.max_files]

    manifest = build_manifest(
        state_prefix=args.state_prefix,
        years=years,
        months=months,
        parts=parts,
        remote_dir=args.remote_dir,
        entries=entries,
    )

    manifest_path = Path(args.manifest_output).resolve()
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8")

    downloaded = 0
    skipped = 0
    if args.download:
        download_dir = Path(args.download_dir).resolve()
        downloaded, skipped = download_entries(ftp, entries, download_dir)

    ftp.quit()

    print(f"Manifest written: {manifest_path}")
    print(f"Selected files: {len(entries)}")
    print(f"Total size (GB): {round(float(manifest['total_size_bytes']) / (1024**3), 2)}")
    if args.download:
        print(f"Downloaded files: {downloaded}")
        print(f"Skipped existing: {skipped}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())