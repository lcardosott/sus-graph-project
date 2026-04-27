# Data Layer

This layer builds nodes and edges from SIH raw data, plus yearly aggregation and UI JSONL exports.

## Core scripts
- [run_sih_year_pipeline.py](run_sih_year_pipeline.py): end-to-end yearly pipeline
- [sih_selector.py](sih_selector.py): SIH manifest + download
- [build_national_reference.py](build_national_reference.py): CNES reference catalog

## Key inputs
- SIH raw DBC files (not in repo)
- Transfer codebook: [reference/motsai_transfer_codes.csv](reference/motsai_transfer_codes.csv)
- CNES reference: [reference/catalog/cnes_br_2101_public_hospitals.csv](reference/catalog/cnes_br_2101_public_hospitals.csv)

## Typical workflow (2021)

1) Download SIH data (not tracked in git):

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python data_layer/sih_selector.py \
  --years 2021 \
  --months 1-12 \
  --ufs ALL \
  --download \
  --download-dir data_layer/raw/sih \
  --manifest-output data_layer/reports/sih_manifest_2021_national.json
```

2) Run yearly pipeline (projection + transfers + residence + aggregation):

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python data_layer/run_sih_year_pipeline.py \
  --year 2021 \
  --months 1-12 \
  --ufs ALL
```

3) Rebuild UI JSONL only (no reprocessing):

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python data_layer/run_sih_year_pipeline.py \
  --year 2021 \
  --months 1-12 \
  --ufs ALL \
  --graph-only
```

## Output artifacts (kept)
- UI JSONL: [reports/batches/ui](reports/batches/ui)
- Yearly nodes/edges CSV are not tracked by git (regenerate with graph-only)

## Residence edges
Residence edges are municipio -> hospital and are built for all admissions.
Field priority: MUNIC_RES, then CODMUNRES, then PA_MUNPCN.
