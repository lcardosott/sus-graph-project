# Modular Batch Architecture

This document defines the modular batch structure for scalable SUS flow processing.

## Goals

- Reusable country-wide reference catalogs (municipalities and CNES).
- Filterable batch execution by month, UF, ICD prefixes, and transfer threshold.
- Intuitive artifact naming for long-term maintenance.
- Performance-aware outputs (CSV + optional Parquet).

## Reference Catalog Layer

1. National municipalities reference:
- source: IBGE municipalities API + municipality malha centroids.
- output: `data_layer/reference/catalog/municipalities_br.csv`.

2. National CNES reference (by competence):
- source: DATASUS CNES ST files (`ST<UF><competence>.dbc`) + optional CNES API cache enrichment.
- output: `data_layer/reference/catalog/cnes_br_<competence>.csv`.
- default scope: public SUS hospitals (`VINC_SUS=1` and public administration and hospital care/beds).

3. Builder command:
```bash
python data_layer/build_national_reference.py \
  --competence 2101 \
  --ufs ALL \
  --cnes-scope public_hospitals
```

4. Scope override (only when explicitly required):
```bash
python data_layer/build_national_reference.py \
  --competence 2101 \
  --ufs ALL \
  --cnes-scope all
```

## Batch Execution Layer

Use `data_layer/run_modular_batch.py`.

Main filters:
- `--include-ufs`: state-level filter.
- `--icd-prefixes`: case filter (prefix match on `DIAG_PRINC`).
- `--min-transfer-count`: transfer threshold.

Main enrichment controls:
- `--enrich-municipalities`
- `--include-all-ibge-municipalities`
- `--enrich-centroids`
- `--centroids-for-all-ibge-municipalities`
- `--enrich-facilities-cnes-api`
- `--national-cnes-reference`
- `--national-cnes-append-scope` (`public_hospitals` or `all`)

Contract gate controls:
- enabled by default in `run_modular_batch.py`
- `--skip-contract-validation` (explicit bypass)
- `--max-missing-node-ratio`
- `--min-node-name-coverage`
- `--contract-report-output`

Map generation:
- `--render-map`

## Naming Convention

Batch tag pattern:
- `<batch_prefix>_<yyyymm>[_ufs-<uf-list>][_icd-<prefix-list>][_tmin-<value>]`

Generated artifacts live in:
- `data_layer/reports/batches/<batch_tag>/`

Default file names:
- `nodes_<batch_tag>.csv`
- `transfer_edges_<batch_tag>.csv`
- `facility_lookup_<batch_tag>.csv`
- `municipality_lookup_<batch_tag>.csv`
- `contract_validation_<batch_tag>.json` (when validation is enabled)
- `geo_map_<batch_tag>.html` (when rendering is enabled)
- `summary_<batch_tag>.json`

Large national SIH download layout (selector):
- script: `data_layer/sih_selector.py`
- output hierarchy: `data_layer/raw/sih/year=YYYY/month=MM/uf=UF/*.dbc`
- manifest: `data_layer/reports/sih_manifest.json`

## Performance Notes

- Keep reusable reference catalogs outside month/state-specific outputs.
- Use Parquet (`--write-parquet`) for large filtered outputs.
- Prefer running national reference builders once, then reuse in multiple batches.
- When plotting large maps, apply UF and transfer filters before rendering.
