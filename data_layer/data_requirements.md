# Data Layer Requirements (Phase 1)

Last updated: 2026-04-25

## 1. Objective

Build an auditable ETL contract to generate `nodes.csv` and `edges.csv` for graph construction.

This phase is ETL-only. No graph object is instantiated in this layer.

## 2. Scope and Decision Locks

1. Transfer matching implementation is blocked until day/hour admission and discharge fields are confirmed in the selected SUS source.
2. Transfer anchor rule is `PA_TRANSF = 1` OR transfer reason code in validated `MOT_SAIDA`/`PA_MOTSAI` codebook.
3. Every assumption must be logged in this file with date and rationale.

## 2.1 Current Gate Checkpoint

Checkpoint date: 2026-04-08

Low-scale gate run on `data_layer/samples/sample_low_scale_siasus.csv`:

- status: FAIL (expected)
- blocker 1: no day-level admission/discharge datetime pair found
- blocker 2: transfer `PA_MOTSAI` codebook has zero validated transfer-related codes
- note: month-level fields `PA_CMP` and `PA_MVM` exist but are not accepted for 24-48h matching

Implication:

- transfer edge inference remains blocked,
- next implementation target is source/schema alignment and official transfer code mapping validation.

## 2.2 Implementation Checkpoint (Matcher Engine)

Checkpoint date: 2026-04-08

Implemented artifacts:

- `data_layer/transfer_matching.py` with deterministic 4-pillar matching and tie-break rules,
- `data_layer/run_transfer_matching.py` CLI runner guarded by schema gate,
- unit tests for window, demographics, clinical continuity, and tie-break behavior.

Status:

- algorithm implementation is available,
- production execution remains blocked until schema gate is green on the selected source.

## 2.3 Real Data Partitioning Checkpoint (2021)

Checkpoint date: 2026-04-08

Real inventory from DATASUS SIASUS path `dissemin/publicos/SIASUS/200801_/Dados`:

- filename pattern observed: `PASPYYMMx.dbc`
- state prefix: `PASP` (Sao Paulo)
- `YY`: two-digit year
- `MM`: month
- `x`: monthly partition segment (`a`, `b`, `c` in 2021)

2021 inventory summary:

- total files: 36
- month partitioning: 12 months x 3 parts (`a`,`b`,`c`)
- total size: ~2.69 GB

Evidence artifacts:

- `data_layer/reports/siasus_sp_2021_manifest.json`
- `data_layer/reports/preview_PASP2101a.csv`
- `data_layer/reports/schema_gate_2021_preview.json`

Observed limitation in real 2021 SIASUS preview:

- no day-level admission/discharge datetime fields were found,
- schema gate remains blocked for strict 24-48h transfer matching.

## 2.4 SIH Probe Checkpoint (2021)

Checkpoint date: 2026-04-08

Probe source:

- `dissemin/publicos/SIHSUS/200801_/Dados/RDSP2101.dbc`

Observed in real schema probe:

- total columns: 113
- day-level fields present: `DT_INTER` and `DT_SAIDA`
- additional clinical fields present: `DIAG_PRINC`, `SEXO`, `IDADE`, `MUNIC_RES`
- deterministic episode identifiers are present (`N_AIH`, `NUM_PROC`), but they are not adopted as patient continuity keys in the selected strategy

Evidence artifact:

- `data_layer/reports/sih_rdsp2101_probe.json`

Implication:

- SIH appears compatible with the 24-48h temporal requirement,
- current gate/matcher still need a source adapter because field names differ from SIASUS `PA_*` namespace,
- patient linkage strategy must remain explicit and auditable for SIH-based transfer chaining.

## 2.5 SIH Gate Unlock Checkpoint (2021)

Checkpoint date: 2026-04-08

Validated with real SIH preview (`RDSP2101`):

- schema gate status: PASS
- deterministic episode key available: `N_AIH` (kept only as metadata, not as patient linkage key)
- mapped origin: `CNES`
- mapped destination: `MUNIC_RES`
- transfer reason field: `COBRANCA`
- datetime pair: `DT_INTER` + `DT_SAIDA`
- validated transfer codebook entries available: transfer codes `31` and `32`

Evidence artifacts:

- `data_layer/reports/preview_RDSP2101.csv`
- `data_layer/reports/schema_gate_rdsp2101_preview.json`
- `data_layer/reports/transfer_events_RDSP2101_preview.csv`
- `data_layer/reports/transfer_edges_RDSP2101_preview.csv`
- `data_layer/reports/transfer_rejections_RDSP2101_preview.json`

Execution note:

- end-to-end runner executed without schema blockers,
- zero transfer events in 10-row preview (expected for tiny probe sample).

## 2.6 SIH Monthly Probabilistic Pilot (RDSP2101)

Checkpoint date: 2026-04-08

Execution scope:

- full monthly SIH file `RDSP2101` (195,978 rows),
- probabilistic linkage strategy only,
- patient continuity key built from `NASC` + `SEXO` + normalized `IDADE` + `MUNIC_RES`,
- `N_AIH` and `NUM_PROC` explicitly excluded as continuity keys.

Pilot outputs:

- transfer events: `179`
- transfer edges: `141`
- rejection counts:
	- `no_candidate_in_time_window`: `9,233`
	- `clinical_discontinuity`: `140`

Probabilistic key QA:

- key completeness: `195,978/195,978` (`1.0`)
- unique valid keys: `139,828`
- colliding key groups: `23,478`
- rows in collision: `56,150`
- collision ratio among valid keys: `0.286512`

Evidence artifacts:

- `data_layer/reports/rdsp2101_prob_input.csv`
- `data_layer/reports/transfer_events_RDSP2101_prob_full.csv`
- `data_layer/reports/transfer_edges_RDSP2101_prob_full.csv`
- `data_layer/reports/transfer_rejections_RDSP2101_prob_full.json`
- `data_layer/reports/rdsp2101_prob_linkage_qa.json`

## 2.7 Node Mapping Checkpoint (RDSP2101)

Checkpoint date: 2026-04-08

Objective:

- materialize reusable node catalogs for facilities and municipalities,
- map transfer edges to canonical `node_id` values for graph ingestion.

Execution details:

- node source built from SIH month `RDSP2101` using `CNES`, `MUNIC_MOV`, `MUNIC_RES`,
- facility-to-municipality mapping based on `MUNIC_MOV` (municipality of care),
- edge mapping applied on `transfer_edges_RDSP2101_prob_full.csv`.

Output counts:

- total nodes: `1,928`
- facility nodes: `635`
- municipality nodes: `1,293`
- mapped edges: `141`
- edge-node coverage: source `1.0`, target `1.0`

Evidence artifacts:

- `data_layer/reports/rdsp2101_nodes_input.csv`
- `data_layer/reports/nodes_RDSP2101_prob.csv`
- `data_layer/reports/facility_lookup_RDSP2101_prob.csv`
- `data_layer/reports/municipality_lookup_RDSP2101_prob.csv`
- `data_layer/reports/transfer_edges_RDSP2101_prob_full_mapped.csv`
- `data_layer/reports/nodes_RDSP2101_prob_summary.json`

## 2.8 Node Enrichment Checkpoint (RDSP2101)

Checkpoint date: 2026-04-08

Objective:

- enrich node labels to support exploratory analysis before full-year execution,
- keep deterministic node ids and edge-node coverage unchanged.

Execution details:

- enrichment source: IBGE municipality catalog (`localidades/municipios`) with local cache,
- facility labels generated as `CNES <id> (Municipio - UF)`,
- municipality labels generated as `Municipio - UF`,
- facility municipality attribution kept from `MUNIC_MOV`.

Output quality:

- nodes total: `1,928`
- nodes with non-empty `name`: `1,928`
- missing municipality metadata: `0`
- edge-node coverage after enrichment: source `1.0`, target `1.0`

Evidence artifacts:

- `data_layer/reports/nodes_RDSP2101_enriched.csv`
- `data_layer/reports/facility_lookup_RDSP2101_enriched.csv`
- `data_layer/reports/municipality_lookup_RDSP2101_enriched.csv`
- `data_layer/reports/transfer_edges_RDSP2101_prob_full_mapped_enriched.csv`
- `data_layer/reports/nodes_RDSP2101_enriched_summary.json`
- `data_layer/reference/cache/ibge_municipios.json`

## 2.9 National CNES Scope Guardrail Checkpoint

Checkpoint date: 2026-04-24

Objective:

- avoid nationwide node inflation caused by appending all CNES establishments,
- preserve location coverage while restricting appended facilities to public SUS hospital scope.

Implementation details:

- `data_layer/build_national_reference.py` now emits CNES classification flags from official ST fields,
- default CNES scope is `public_hospitals`, defined as:
	- `VINC_SUS = 1`,
	- public administration (`NIV_DEP = 1` or public `NAT_JUR` family),
	- hospital activity (`ATENDHOS = 1` or `LEITHOSP = 1`),
- `data_layer/run_modular_batch.py` now applies `--national-cnes-append-scope public_hospitals` by default and blocks unsafe append when classifier columns are unavailable.

Observed local evidence (SP competence 2101):

- CNES rows parsed: `74,005`
- SUS-linked facilities: `13,118`
- hospital care/bed facilities: `1,206`
- public hospital scope: `709`

Implication:

- scope lock prevents appending hundreds of thousands of non-target facilities,
- municipality location coverage remains available independently via IBGE enrichment.

## 2.10 Contract Validation Gate Checkpoint

Checkpoint date: 2026-04-24

Objective:

- block progression to model/viz steps when node/edge contracts are structurally invalid,
- add non-visual QA evidence in the data layer.

Implemented artifacts:

- `data_layer/contract_validation.py` (shared validation logic),
- `data_layer/validate_data_contracts.py` (CLI validator),
- `run_modular_batch.py` default validation gate with JSON report output,
- `model_layer/build_graph.py` pre-graph contract gate.

Gate behavior:

- default threshold: `max_missing_node_ratio = 0.0`,
- warnings for low node-name coverage,
- non-zero exit when hard contract checks fail.

## 2.11 Scoped Monthly Batch Checkpoint (RDSP Public Hospitals)

Checkpoint date: 2026-04-25

Objective:

- run the monthly pilot batch with public-hospital CNES scope,
- confirm contract validation passes with reduced node volume.

Execution details:

- batch tag: `rdsp_pubh_202101`
- national CNES scope: `public_hospitals`
- nodes total: `11,512` (facility `5,941`, municipality `5,571`)
- edges total: `141`
- contract gate: PASS

Evidence artifacts:

- `data_layer/reports/batches/rdsp_pubh_202101/summary_rdsp_pubh_202101.json`
- `data_layer/reports/batches/rdsp_pubh_202101/contract_validation_rdsp_pubh_202101.json`
- `data_layer/reports/batches/rdsp_pubh_202101/nodes_rdsp_pubh_202101.csv`
- `data_layer/reports/batches/rdsp_pubh_202101/transfer_edges_rdsp_pubh_202101.csv`
- `data_layer/reports/batches/rdsp_pubh_202101/geo_map_rdsp_pubh_202101.html`
- `data_layer/reports/batches/rdsp_pubh_202101/geo_map_rdsp_pubh_202101_summary.json`

## 2.12 National SIH Download Checkpoint (2021)

Checkpoint date: 2026-04-25

Objective:

- materialize the full-year nationwide SIH dataset using the resilient selector.

Execution details:

- manifest: `324` files
- total size: `0.81 GB`
- downloaded: `324`
- failed: `0`

Evidence artifacts:

- `data_layer/reports/sih_manifest_2021_national.json`

## 3. Source and Schema Gate (Mandatory)

The transfer matching algorithm can run only if all conditions below are satisfied.

### 3.1 Required clinical and demographic fields

At least one supported profile must be satisfied.

SIASUS profile:

- `PA_SEXO`
- `PA_IDADE`
- `PA_CIDPRI`
- `PA_CODUNI`

SIH profile:

- `SEXO`
- `IDADE`
- `DIAG_PRINC`
- origin facility field (`CNES` or `CGC_HOSP`)

Transfer trigger availability (mandatory in both profiles):

- `PA_TRANSF`, or
- transfer reason field (`PA_MOTSAI`, `MOT_SAIDA`, or `COBRANCA`).

### 3.2 Required linkage fields

Supported linkage strategies:

1. Probabilistic (default and approved for SIH):
	- required fields: birthdate (`NASC`/`DT_NASC`/`DT_NASCIMENTO`), `SEXO`, normalized `IDADE`, and residence (`MUNIC_RES`/`CODMUNRES`/`PA_MUNPCN`)
	- explicitly does not use `N_AIH` or `NUM_PROC` as patient continuity keys.
2. Deterministic (optional fallback, explicit opt-in):
	- one of `PA_CNSMED`, `PA_CNPJCPF`, or `PA_AUTORIZ`
	- `N_AIH`/`NUM_PROC` are episode identifiers and are not part of the default continuity strategy.

### 3.3 Required destination field

At least one destination location field must be available:

- `PA_MUNPCN` (municipality destination), or
- `MUNIC_RES`/`CODMUNRES`, or
- an explicit destination facility field in the selected source

### 3.4 Required datetime fields (hard blocker)

To enforce the 24-48h sliding window, the dataset must include day-level (or finer) timestamps for both admission and discharge.

Accepted examples (source dependent):

- `DT_INTER` + `DT_SAIDA`
- `DT_ADMISSAO` + `DT_ALTA`
- `DATA_ENTRADA` + `DATA_SAIDA`

If only month-level fields exist (for example `PA_CMP` and `PA_MVM` in `YYYYMM` format), transfer matching is blocked.

## 4. Transfer Matching Algorithm (Four Pillars)

### 4.1 Pillar 1: Anchor (trigger)

Candidate discharge records are selected when:

- `PA_TRANSF = 1`, or
- transfer reason (`PA_MOTSAI`/`MOT_SAIDA`/`COBRANCA`) belongs to the validated transfer code set.

### 4.2 Pillar 2: Sliding time window (Delta T)

For each anchor event, search destination admissions where:

- `admission_datetime - discharge_datetime` is between 24 and 48 hours.

Tie-break order when multiple candidates are valid:

1. smallest non-negative time delta,
2. exact ICD chapter match,
3. lowest lexical destination identifier (deterministic fallback).

### 4.3 Pillar 3: Demographic matching

Mandatory exact match:

- `PA_SEXO`
- normalized age

Age normalization rule:

- normalize to years before comparison,
- reject invalid normalized ages outside [0, 130].

### 4.4 Pillar 4: Clinical continuity

`PA_CIDPRI` at destination must remain in the same ICD-10 chapter as origin.

Rule detail:

- chapter extracted from ICD first character and official chapter boundaries,
- missing or invalid ICD at origin or destination invalidates transfer edge creation for that pair.

### 4.5 Official Transfer Reason Codebook (Applied)

Validated domain currently applied in `data_layer/reference/motsai_transfer_codes.csv`:

- Discharge family: `11`, `12`, `14`, `15`, `16`, `18`
- Permanence family: `21`
- Transfer family (primary target): `31`, `32`
- Death family: `41`, `42`, `43`

Rule used by matcher:

- primary transfer trigger by reason code: `31` or `32`.

## 5. Output Contracts

## 5.1 edges.csv

Minimum contract (v1):

- `edge_id` (string, non-null, deterministic hash)
- `source_facility_id` (string, non-null)
- `target_location_id` (string, non-null)
- `target_location_type` (string, non-null, values: `municipality` or `facility`)
- `transfer_count` (integer, non-null, >= 1)
- `distance_km` (float, nullable until geolocation merge)
- `match_method` (string, non-null, example: `4pillars_strict`)
- `confidence_score` (float, non-null, range [0, 1])
- `rule_version` (string, non-null)
- `assumption_revision` (string, non-null)

## 5.2 nodes.csv

Minimum contract (v1):

- `node_id` (string, non-null)
- `node_type` (string, non-null, values: `facility` or `municipality`)
- `name` (string, nullable)
- `municipality_code` (string, nullable)
- `latitude` (float, nullable)
- `longitude` (float, nullable)
- `capacity_beds` (integer, nullable)
- `habilitation_level` (string, nullable)

## 5.3 Storage Format Strategy (Nodes and Edges)

Decision:

- canonical storage for scale and analytics: Parquet,
- interoperability and manual inspection export: CSV,
- TDMS is not recommended for this project context.

Rationale:

1. Parquet supports columnar compression and much faster scans for large multi-year datasets.
2. Parquet preserves schema and types better than CSV in iterative pipelines.
3. CSV remains useful for audit/debug and spreadsheet sharing.
4. TDMS is optimized for measurement/lab instrumentation workflows and is not a natural fit for SUS tabular ETL.

Recommended output policy:

1. Keep curated bronze/silver tables in Parquet partitioned by `year` and `month`.
2. Generate CSV only for selected outputs (`nodes.csv`, `edges.csv`, QA snapshots).
3. Version outputs by rule version and processing date to ensure reproducibility.

Suggested directory layout:

- `data_layer/curated/parquet/nodes/year=YYYY/month=MM/*.parquet`
- `data_layer/curated/parquet/edges/year=YYYY/month=MM/*.parquet`
- `data_layer/exports/csv/nodes_YYYYMM.csv`
- `data_layer/exports/csv/edges_YYYYMM.csv`

## 5.4 Multi-Year Selection Workflow

Implemented selector CLI:

- `data_layer/siasus_selector.py`

Main capabilities:

1. Choose years by expression (`2021` or `2020-2022`).
2. Choose months (`1-12`, `3,4,5`) and parts (`abc` or `a`).
3. Generate manifest JSON with file counts and sizes.
4. Optional download with cache/skip behavior.

Example usage:

1. Manifest only for 2021:
	- `python data_layer/siasus_selector.py --years 2021 --months 1-12 --parts abc --manifest-output data_layer/reports/siasus_manifest_2021.json`
2. Download selected set:
	- `python data_layer/siasus_selector.py --years 2021 --months 1-12 --parts abc --download --download-dir data_layer/raw/siasus --manifest-output data_layer/reports/siasus_manifest_2021.json`

## 5.5 Strict Projection Policy (RAM Optimization)

When loading DBC/Parquet files for processing, keep strictly this canonical analytics set:

- `CODMUNRES`
- `CNES`
- `DIAG_PRINC`
- `NASC`
- `IDADE`
- `SEXO`
- `DT_INTER`
- `DT_SAIDA`
- `MOT_SAIDA`
- `PA_TRANSF`
- `DIAS_PERM`
- `MARCA_UTI`
- `PROC_REA`
- `VAL_TOT`
- `RACA_COR`

Optional linkage metadata columns (only when explicitly requested):

- `N_AIH`
- `NUM_PROC`
- `PA_CNSMED`
- `PA_CNPJCPF`

Rule:

- `N_AIH` and `NUM_PROC` are never used by default as patient linkage keys in transfer chaining.

Birthdate inclusion note:

- `NASC` is included in the strict projection because probabilistic linkage requires a birthdate field.

Implementation artifact:

- `data_layer/column_projection.py`
- `data_layer/project_dataset.py`

## 5.6 National SIH Selector (Resilient Download Workflow)

Implemented selector CLI:

- `data_layer/sih_selector.py`

Main capabilities:

1. Select by years, months, and UFs (including `ALL`).
2. Generate manifest with total files, sizes, and per-UF/per-month breakdown.
3. Retryable FTP download with reconnect behavior and partial-file cleanup.
4. Size verification against FTP metadata (optional bypass via `--skip-size-check`).
5. Partitioned local layout for easier navigation:
	- `data_layer/raw/sih/year=YYYY/month=MM/uf=UF/*.dbc`

## 6. Low-Scale Validation Protocol

1. Run schema gate on the low-scale sample first.
2. Only if gate passes, run transfer matching on low-scale data.
3. Compare inferred edges against manual audit sample.
4. Freeze `rule_version` after reproducibility check.

Acceptance criteria:

- no silent failures,
- deterministic rerun counts,
- all rejected records classified with explicit reason.

## 7. Open Items

1. Confirm whether destination must be facility-level or municipality-level in this phase.
2. Run full-year 2021 SIH profiling to validate linkage quality and transfer coverage.
3. Define monthly QA thresholds for probabilistic key completeness and collision ratio (not only completeness).
4. Evaluate collision mitigation options (for example adding `RACA_COR` or constrained temporal blocking) before full-year production output.
5. Define if `NIV_DEP` and `NAT_JUR` rules should be expanded to include contracted private SUS hospitals in an alternate scope profile.

## 8. Assumptions Log (Mandatory)

| Date | Assumption | Rationale | Impact | Validation Status | Owner |
| --- | --- | --- | --- | --- | --- |
| 2026-04-08 | Transfer matching requires day/hour admission and discharge fields. Month-level fields are insufficient. | The chosen sliding window is 24-48h and needs temporal ordering at sub-month granularity. | Blocks transfer inference until source gate is green. | Approved | Project team |
| 2026-04-08 | Anchor is `PA_TRANSF = 1` OR transfer-related `PA_MOTSAI` code. | Improves recall while preserving explicit discharge trigger logic. | Requires validated codebook maintenance. | Approved | Project team |
| 2026-04-08 | Every assumption must be documented in the related layer markdown. | Ensures auditability and reproducibility across phases. | Adds governance overhead but prevents hidden decisions. | Approved | Project team |
| 2026-04-08 | Current transfer matcher targets destination facility as the `PA_CODUNI` of the next matched admission event. | The 4-pillar method links a discharge event to a subsequent admission event; destination facility is represented by the receiving admission record. | If a source only exposes municipality destination, this mapping must be adapted by configuration before production runs. | Pending validation on final source | Project team |
| 2026-04-08 | ICD continuity in the matcher is evaluated by ICD-10 chapter compatibility (`PA_CIDPRI`) using chapter boundaries in code. | Implements deterministic chapter-level continuity without external lookup service at runtime. | Rare malformed ICD codes are rejected from strict transfer inference. | Pending domain review | Project team |
| 2026-04-08 | Age matching is strict after normalization to integer years in range [0,130]. | Enforces deterministic demographic equality for the third pillar. | Records with non-integer or out-of-range age are excluded from strict matching. | Pending domain review | Project team |
| 2026-04-08 | The analysis focus year is 2021 (pandemic period), using full-year monthly partitions from Sao Paulo SIASUS (`PASP2101a..PASP2112c`). | User-selected year for stressed-system analysis and comparability across months. | Adds ~2.69 GB raw ingestion target and requires batch orchestration by month/partition. | Approved | Project team |
| 2026-04-08 | Canonical storage format is Parquet; CSV is export/audit format; TDMS is out of scope. | Balances scale performance, schema stability, and interoperability for health ETL. | Requires Parquet dependency in environment and partition governance. | Approved | Project team |
| 2026-04-08 | SIH (`RDSP`) is the primary candidate source to unblock 24-48h matching because it contains `DT_INTER` and `DT_SAIDA` in the 2021 probe. | Temporal requirement cannot be satisfied with monthly-only SIASUS fields. | Requires mapping adapter from SIH field names to matcher canonical contract. | Observed in probe, pending full-year validation | Project team |
| 2026-04-08 | SIH transfer chaining uses probabilistic patient linkage with `NASC` + `SEXO` + normalized `IDADE` + `MUNIC_RES`/`CODMUNRES`; `N_AIH` and `NUM_PROC` are excluded as continuity keys. | User decision to avoid episode identifiers as patient keys and keep continuity based on demographic-temporal identity. | Requires monitoring for key collisions and completeness before production scale. | Approved | Project team |
| 2026-04-08 | Canonical node id format is `facility:<CNES>` and `municipality:<IBGE_code>`; facility municipality attribution is derived from `MUNIC_MOV` when available. | Prevents id collision across node types and preserves care-location semantics for unit mapping. | Enables deterministic edge-to-node mapping and full coverage checks before graph load. | Approved in RDSP2101 checkpoint | Project team |
| 2026-04-08 | Node label enrichment uses IBGE municipality catalog cache and CNES-based synthetic facility labels (`CNES <id> (Municipio - UF)`) until full CNES master metadata is merged. | Enables immediate graph readability without blocking on heavier CNES enrichment ETL. | Good for pilot visualization; not sufficient yet for final facility geolocation/capacity analytics. | Approved for pilot | Project team |
| 2026-04-08 | Official transfer reason codebook is applied using `MOT_SAIDA`/`PA_MOTSAI` families with transfer trigger on codes `31` and `32`. | User-provided SIH/SUS coding structure for transfer/discharge/permanence/death categories. | Enables deterministic trigger logic in SIH and SIASUS adapters. | Applied in local codebook, pending external manual cross-check | Project team |
| 2026-04-24 | National CNES append scope defaults to public SUS hospitals (`VINC_SUS=1` + public administration + hospital care/beds). | Previous nationwide append included non-target facilities and inflated node counts dramatically. | Keeps node scale aligned with transfer-network purpose while preserving municipality coverage. | Approved | Project team |
| 2026-04-24 | Data-layer and model-layer execution are blocked by contract validation when edge-node references are incomplete. | Visualization is not the primary trust mechanism for structural integrity checks. | Adds deterministic QA gate before next phases and reduces propagation of schema/coverage errors. | Approved | Project team |
| 2026-04-24 | Nationwide SIH download must use retryable/reconnect FTP workflow with size verification and partitioned storage (`year/month/uf`). | Full-year country-wide pulls are long-running and sensitive to transient network/FTP failures. | Improves resumability and operational reliability for annual national ingestion. | Approved | Project team |
| 2026-04-25 | Strict projection includes `NASC` (birthdate) for SIH probabilistic linkage. | Probabilistic linkage requires a birthdate field; prior strict projection omitted it. | Enables full-year transfer matching without ad-hoc data reloads. | Approved | Project team |
