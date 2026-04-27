#!/usr/bin/env python3
"""Select and optionally download SIH files by year/month/UF.

This selector is designed for large nationwide runs with retryable,
size-verified downloads and resumable behavior.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import asdict, dataclass
from ftplib import FTP
from pathlib import Path


DEFAULT_SERVER = "ftp.datasus.gov.br"
DEFAULT_REMOTE_DIR = "dissemin/publicos/SIHSUS/200801_/Dados"
BRAZIL_UFS = {
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT",
    "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
}


@dataclass(frozen=True)
class SihEntry:
    file_name: str
    uf: str
    year: int
    month: int
    size_bytes: int | None


def parse_numeric_ranges(raw_value: str, min_value: int, max_value: int, label: str) -> set[int]:
    selected: set[int] = set()
    tokens = [token.strip() for token in raw_value.split(",") if token.strip()]
    if not tokens:
        raise ValueError(f"{label} is empty")

    for token in tokens:
        if "-" in token:
            start_raw, end_raw = token.split("-", maxsplit=1)
            start = int(start_raw)
            end = int(end_raw)
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


def parse_ufs(raw_value: str) -> set[str]:
    normalized = raw_value.strip().upper()
    if normalized in {"ALL", "*"}:
        return set(BRAZIL_UFS)

    selected = {token.strip().upper() for token in raw_value.split(",") if token.strip()}
    if not selected:
        raise ValueError("ufs is empty")

    invalid = sorted(selected - BRAZIL_UFS)
    if invalid:
        raise ValueError("Invalid UF in --ufs: " + ", ".join(invalid))

    return selected


def filename_pattern() -> re.Pattern[str]:
    return re.compile(r"^RD(?P<uf>[A-Z]{2})(?P<yy>\d{2})(?P<mm>\d{2})\.dbc$")


def connect_ftp(server: str, remote_dir: str, timeout_seconds: int) -> FTP:
    ftp = FTP(server, timeout=timeout_seconds)
    ftp.login()
    ftp.cwd(remote_dir)
    return ftp


def list_filtered_entries(
    ftp: FTP,
    years: set[int],
    months: set[int],
    ufs: set[str],
) -> list[SihEntry]:
    pattern = filename_pattern()
    selected: list[SihEntry] = []

    for file_name in ftp.nlst():
        match = pattern.match(file_name)
        if not match:
            continue

        uf = match.group("uf")
        year = 2000 + int(match.group("yy"))
        month = int(match.group("mm"))
        if uf not in ufs or year not in years or month not in months:
            continue

        file_size: int | None = None
        try:
            raw_size = ftp.size(file_name)
            if raw_size is not None:
                file_size = int(raw_size)
        except Exception:
            file_size = None

        selected.append(
            SihEntry(
                file_name=file_name,
                uf=uf,
                year=year,
                month=month,
                size_bytes=file_size,
            )
        )

    selected.sort(key=lambda item: (item.year, item.month, item.uf))
    return selected


def build_manifest(
    remote_dir: str,
    years: set[int],
    months: set[int],
    ufs: set[str],
    entries: list[SihEntry],
) -> dict[str, object]:
    total_files = len(entries)
    total_size_bytes = int(sum(entry.size_bytes or 0 for entry in entries))

    yearly: dict[int, dict[str, object]] = {}
    for year in sorted(years):
        yearly[year] = {
            "year": year,
            "months": [],
            "total_files": 0,
            "total_size_bytes": 0,
        }

    entry_buckets: dict[tuple[int, int], list[SihEntry]] = {}
    for entry in entries:
        entry_buckets.setdefault((entry.year, entry.month), []).append(entry)

    for year in sorted(years):
        for month in sorted(months):
            bucket = entry_buckets.get((year, month), [])
            month_payload = {
                "month": month,
                "num_files": len(bucket),
                "ufs": sorted({entry.uf for entry in bucket}),
                "files": [entry.file_name for entry in bucket],
                "size_bytes": int(sum(entry.size_bytes or 0 for entry in bucket)),
            }
            yearly[year]["months"].append(month_payload)
            yearly[year]["total_files"] += month_payload["num_files"]
            yearly[year]["total_size_bytes"] += month_payload["size_bytes"]

    by_uf: dict[str, dict[str, object]] = {}
    for uf in sorted(ufs):
        uf_entries = [entry for entry in entries if entry.uf == uf]
        by_uf[uf] = {
            "uf": uf,
            "total_files": len(uf_entries),
            "total_size_bytes": int(sum(entry.size_bytes or 0 for entry in uf_entries)),
        }

    return {
        "dataset_path": remote_dir,
        "dataset": "SIH",
        "years": sorted(years),
        "months": sorted(months),
        "ufs": sorted(ufs),
        "total_files": total_files,
        "total_size_bytes": total_size_bytes,
        "years_detail": [yearly[year] for year in sorted(yearly)],
        "ufs_detail": [by_uf[uf] for uf in sorted(by_uf)],
        "selected_entries": [asdict(entry) for entry in entries],
    }


def download_entries(
    server: str,
    remote_dir: str,
    entries: list[SihEntry],
    output_dir: Path,
    timeout_seconds: int,
    max_retries: int,
    retry_wait_seconds: float,
    verify_size: bool,
) -> tuple[int, int, list[dict[str, str]]]:
    output_dir.mkdir(parents=True, exist_ok=True)

    ftp: FTP | None = connect_ftp(server, remote_dir, timeout_seconds)
    downloaded = 0
    skipped = 0
    failures: list[dict[str, str]] = []

    try:
        for entry in entries:
            target_dir = output_dir / f"year={entry.year}" / f"month={entry.month:02d}" / f"uf={entry.uf}"
            target_dir.mkdir(parents=True, exist_ok=True)
            target_file = target_dir / entry.file_name

            remote_size = entry.size_bytes
            if target_file.exists() and target_file.stat().st_size > 0:
                if (not verify_size) or remote_size is None or target_file.stat().st_size == remote_size:
                    skipped += 1
                    continue
                target_file.unlink()

            temp_file = target_file.with_suffix(target_file.suffix + ".part")
            success = False
            last_error = ""

            for attempt in range(1, max(1, int(max_retries)) + 1):
                try:
                    if ftp is None:
                        ftp = connect_ftp(server, remote_dir, timeout_seconds)

                    with temp_file.open("wb") as handle:
                        ftp.retrbinary(f"RETR {entry.file_name}", handle.write)

                    if verify_size and remote_size is not None:
                        local_size = temp_file.stat().st_size
                        if local_size != remote_size:
                            raise RuntimeError(
                                f"size mismatch for {entry.file_name}: local={local_size} remote={remote_size}"
                            )

                    temp_file.replace(target_file)
                    downloaded += 1
                    success = True
                    break
                except Exception as exc:  # pylint: disable=broad-except
                    last_error = f"{type(exc).__name__}: {exc}"
                    if temp_file.exists():
                        temp_file.unlink(missing_ok=True)
                    if ftp is not None:
                        try:
                            ftp.quit()
                        except Exception:  # pylint: disable=broad-except
                            pass
                        ftp = None
                    if attempt < max(1, int(max_retries)):
                        time.sleep(max(0.0, float(retry_wait_seconds)))

            if not success:
                failures.append(
                    {
                        "file_name": entry.file_name,
                        "uf": entry.uf,
                        "year": str(entry.year),
                        "month": f"{entry.month:02d}",
                        "error": last_error,
                    }
                )
    finally:
        if ftp is not None:
            ftp.quit()

    return downloaded, skipped, failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Select SIH files by year/month/UF and optionally download.")
    parser.add_argument("--server", default=DEFAULT_SERVER, help="FTP server host.")
    parser.add_argument("--remote-dir", default=DEFAULT_REMOTE_DIR, help="FTP directory with SIH files.")
    parser.add_argument("--years", required=True, help="Years expression, for example 2021 or 2020-2022.")
    parser.add_argument("--months", default="1-12", help="Months expression, for example 1-12 or 3,4,5.")
    parser.add_argument("--ufs", default="ALL", help="UF expression, for example SP or SP,RJ or ALL.")
    parser.add_argument("--max-files", type=int, help="Optional limit on selected files after sorting.")
    parser.add_argument(
        "--manifest-output",
        default="data_layer/reports/sih_manifest.json",
        help="Manifest JSON output path.",
    )
    parser.add_argument("--download", action="store_true", help="Download selected files.")
    parser.add_argument(
        "--download-dir",
        default="data_layer/raw/sih",
        help="Base directory for downloaded SIH files.",
    )
    parser.add_argument("--ftp-timeout", type=int, default=120, help="FTP timeout in seconds.")
    parser.add_argument("--max-retries", type=int, default=4, help="Retry attempts per file download.")
    parser.add_argument(
        "--retry-wait-seconds",
        type=float,
        default=1.0,
        help="Wait interval between retries.",
    )
    parser.add_argument(
        "--skip-size-check",
        action="store_true",
        help="Skip downloaded file size verification against FTP metadata.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    years = parse_years(args.years)
    months = parse_months(args.months)
    ufs = parse_ufs(args.ufs)

    ftp = connect_ftp(args.server, args.remote_dir, max(30, int(args.ftp_timeout)))
    try:
        entries = list_filtered_entries(
            ftp=ftp,
            years=years,
            months=months,
            ufs=ufs,
        )
    finally:
        ftp.quit()

    if args.max_files is not None:
        entries = entries[: args.max_files]

    manifest = build_manifest(
        remote_dir=args.remote_dir,
        years=years,
        months=months,
        ufs=ufs,
        entries=entries,
    )

    downloaded = 0
    skipped = 0
    failures: list[dict[str, str]] = []
    if args.download:
        downloaded, skipped, failures = download_entries(
            server=args.server,
            remote_dir=args.remote_dir,
            entries=entries,
            output_dir=Path(args.download_dir).resolve(),
            timeout_seconds=max(30, int(args.ftp_timeout)),
            max_retries=max(1, int(args.max_retries)),
            retry_wait_seconds=max(0.0, float(args.retry_wait_seconds)),
            verify_size=not args.skip_size_check,
        )

    manifest["download"] = {
        "enabled": bool(args.download),
        "downloaded": int(downloaded),
        "skipped": int(skipped),
        "failed": int(len(failures)),
        "verify_size": bool(not args.skip_size_check),
        "max_retries": int(args.max_retries),
        "retry_wait_seconds": float(args.retry_wait_seconds),
        "failures": failures,
    }

    manifest_path = Path(args.manifest_output).resolve()
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8")

    print(f"Manifest written: {manifest_path}")
    print(f"Selected files: {len(entries)}")
    print(f"Total size (GB): {round(float(manifest['total_size_bytes']) / (1024 ** 3), 2)}")
    if args.download:
        print(f"Downloaded files: {downloaded}")
        print(f"Skipped existing: {skipped}")
        print(f"Failed downloads: {len(failures)}")

    if failures:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
