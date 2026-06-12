#!/usr/bin/env python3
"""Create timestamped backups for derived analysis artifacts."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path


DEFAULT_FILES = [
    "data_layer/reports/batches/sih_br_2021_nodes.csv",
    "data_layer/reports/batches/sih_br_2021_edges.csv",
    "data_layer/reports/batches/sih_br_2021_contract_validation.json",
    "data_layer/reports/batches/ui/sih_br_2021_nodes.jsonl",
    "data_layer/reports/batches/ui/sih_br_2021_edges.jsonl",
    "data_layer/reports/batches/ui/sih_br_2021_meta.json",
    "model_layer/reports/graph_sih_br_2021.gexf",
    "model_layer/reports/graph_sih_br_2021_summary.json",
    "viz_layer/reports/graph_sih_br_2021_metrics_summary.json",
    "viz_layer/reports/graph_sih_br_2021_degree_distribution.png",
    "viz_layer/reports/graph_sih_br_2021_component_size_distribution.png",
    "algorithm_layer/reports/resilience_sih_br_2021_summary.json",
    "algorithm_layer/reports/resilience_sih_br_2021_bands.csv",
    "algorithm_layer/reports/resilience_sih_br_2021_centrality.csv",
    "algorithm_layer/reports/resilience_sih_br_2021_stress.csv",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backup current annual analysis artifacts.")
    parser.add_argument("--timestamp", help="Optional timestamp label. Defaults to UTC YYYYmmddTHHMMSSZ.")
    parser.add_argument("--manifest-output", help="Optional manifest JSON output path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path.cwd()
    timestamp = args.timestamp or datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    data_backup_dir = root / "data_layer" / "reports" / "backups" / timestamp
    algorithm_backup_dir = root / "algorithm_layer" / "reports" / "backups" / timestamp
    model_backup_dir = root / "model_layer" / "reports" / "backups" / timestamp
    viz_backup_dir = root / "viz_layer" / "reports" / "backups" / timestamp

    backup_roots = {
        "data_layer/": data_backup_dir,
        "algorithm_layer/": algorithm_backup_dir,
        "model_layer/": model_backup_dir,
        "viz_layer/": viz_backup_dir,
    }

    manifest: dict[str, object] = {"timestamp": timestamp, "copied": [], "missing": []}
    for rel_path in DEFAULT_FILES:
        source = root / rel_path
        if not source.exists():
            manifest["missing"].append(rel_path)  # type: ignore[union-attr]
            continue

        backup_root = None
        for prefix, candidate in backup_roots.items():
            if rel_path.startswith(prefix):
                backup_root = candidate
                suffix = rel_path[len(prefix) :]
                break
        if backup_root is None:
            backup_root = data_backup_dir
            suffix = rel_path

        destination = backup_root / suffix
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        manifest["copied"].append({"source": rel_path, "backup": str(destination.relative_to(root))})  # type: ignore[union-attr]

    manifest_output = Path(args.manifest_output) if args.manifest_output else data_backup_dir / "backup_manifest.json"
    manifest_output.parent.mkdir(parents=True, exist_ok=True)
    manifest_output.write_text(json.dumps(manifest, indent=2, ensure_ascii=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
