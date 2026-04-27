# Data Artifact Naming and Organization

Last updated: 2026-04-24

## Goal

Keep data artifacts discoverable and comparable across pilot, monthly, and national-scale runs.

## Canonical Output Layout

1. Batch outputs
- path: `data_layer/reports/batches/<batch_tag>/`
- files:
  - `nodes_<batch_tag>.csv`
  - `transfer_edges_<batch_tag>.csv`
  - `facility_lookup_<batch_tag>.csv`
  - `municipality_lookup_<batch_tag>.csv`
  - `contract_validation_<batch_tag>.json`
  - `summary_<batch_tag>.json`

2. National reference outputs
- path: `data_layer/reference/catalog/`
- files:
  - `municipalities_br.csv`
  - `cnes_br_<competence>.csv`
  - `reference_br_<competence>_summary.json`

3. Raw SIH downloads
- path: `data_layer/raw/sih/year=YYYY/month=MM/uf=UF/*.dbc`
- manifest: `data_layer/reports/sih_manifest.json`

## Naming Rules

1. Always include time scope in file names (`<yyyymm>` in batch tag).
2. Avoid generic names like `output.csv`, `result.csv`, or `final.csv`.
3. Keep one summary JSON per run with counts and filters.
4. Keep QA evidence close to the run artifacts (`contract_validation_<batch_tag>.json`).

## Legacy Cleanup Policy

1. Keep historical pilot evidence only when referenced in requirement checkpoints.
2. Remove lock/temp files (`.~lock*`, `*.part`, `__pycache__/`).
3. Prevent generated raw/report artifacts from polluting Git status via `.gitignore`.
