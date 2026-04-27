# Runbook (Phase 1)

Last updated: 2026-04-25

## Assumptions (task log)

- 2026-04-26: Year aggregation may read monthly edge CSVs with numeric fields (e.g., `confidence_score`) stored as strings; coerce to numeric before aggregation so `mean` works. Rationale: pandas reads CSVs as `dtype=str` in the yearly step and raises on mean over object values like "1.0".
- 2026-04-26: `--graph-only` can skip monthly processing and reuse existing monthly outputs for year aggregation. Rationale: avoids re-running expensive monthly steps when outputs already exist and the failure is in the aggregation stage.
- 2026-04-26: Skip node attributes with missing values (pd.NA/NaN) before GEXF export to avoid NetworkX GEXF type errors. Rationale: GEXF writer rejects pandas missing-value types.
- 2026-04-26: Graph metrics use total degree for directed graphs (in+out) and strongly connected components for component counts/size distribution when directed. Rationale: aligns with standard NetworkX degree() and SCC definitions for directed graphs.
- 2026-04-26: Residence edges are built as municipality-of-residence -> facility for all admissions, using MUNIC_RES then CODMUNRES then PA_MUNPCN, and tagged with edge_type="residence". Rationale: keep full coverage and allow filtering alongside transfer edges.
- 2026-04-26: UI JSONL export is generated during yearly aggregation (unless --skip-ui-json), with default rendering limits set to 0 (no limit) in meta JSON. Rationale: allow full rendering when explicitly requested.

This is a minimal, repeatable sequence to rebuild the data layer and (optionally) the phase-2 graph.

## 0) Environment

```bash
source /home/lulutoratora/Documents/comp/mc859/.venv/bin/activate
```

## 1) National references (public hospitals scope)

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python data_layer/build_national_reference.py \
  --competence 2101 \
  --ufs ALL \
  --cnes-scope public_hospitals \
  --cnes-output data_layer/reference/catalog/cnes_br_2101_public_hospitals.csv \
  --summary-output data_layer/reference/catalog/reference_br_2101_public_hospitals_summary.json
```

## 2) SIH nationwide manifest + download (2021)

Manifest:
```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python data_layer/sih_selector.py \
  --years 2021 \
  --months 1-12 \
  --ufs ALL \
  --manifest-output data_layer/reports/sih_manifest_2021_national.json
```

Download:
```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python data_layer/sih_selector.py \
  --years 2021 \
  --months 1-12 \
  --ufs ALL \
  --download \
  --download-dir data_layer/raw/sih \
  --manifest-output data_layer/reports/sih_manifest_2021_national.json
```

## 3) Monthly pilot batch (scoped hospitals + contract gate)

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python data_layer/run_modular_batch.py \
  --batch-prefix rdsp_pubh \
  --year 2021 \
  --month 1 \
  --records-input data_layer/reports/rdsp2101_nodes_input.csv \
  --edges-input data_layer/reports/transfer_edges_RDSP2101_prob_full.csv \
  --enrich-municipalities \
  --include-all-ibge-municipalities \
  --enrich-centroids \
  --enrich-facilities-cnes-api \
  --national-cnes-reference data_layer/reference/catalog/cnes_br_2101_public_hospitals.csv \
  --render-map \
  --write-parquet
```

## 4) Contract validation (standalone, optional)

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python data_layer/validate_data_contracts.py \
  --nodes-input data_layer/reports/batches/rdsp_pubh_202101/nodes_rdsp_pubh_202101.csv \
  --edges-input data_layer/reports/batches/rdsp_pubh_202101/transfer_edges_rdsp_pubh_202101.csv
```

## 5) Phase 2 graph build (optional, only if model is green)

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python model_layer/build_graph.py \
  --nodes-input data_layer/reports/batches/rdsp_pubh_202101/nodes_rdsp_pubh_202101.csv \
  --edges-input data_layer/reports/batches/rdsp_pubh_202101/transfer_edges_rdsp_pubh_202101.csv \
  --graph-output model_layer/reports/graph_rdsp_pubh_202101.gexf \
  --summary-output model_layer/reports/graph_rdsp_pubh_202101_summary.json
```
