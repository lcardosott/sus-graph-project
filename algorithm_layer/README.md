# Algorithm Layer

Resilience analysis over validated graph artifacts.

## Current Runner

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python algorithm_layer/resilience_analysis.py \
  --graph-input model_layer/reports/graph_sih_br_2021.gexf \
  --out-dir algorithm_layer/reports \
  --prefix resilience_sih_br_2021 \
  --distance-bands-km 50,100,200 \
  --min-residence-count 5 \
  --min-transfer-count 2 \
  --centrality-node-type facility
```

The runner applies filters in this order:

1. Recurrence threshold by edge type.
2. Minimum geographic distance band.
3. Undirected weighted projection for resilience algorithms.

Outputs:

- `*_summary.json`: parameters and band-level summary.
- `*_bands.csv`: component/service-basin scale per distance band.
- `*_centrality.csv`: top betweenness facilities per band by default.
- `*_stress.csv`: facility suppression stress-test table.

## Current Assumption

Betweenness and suppression are computed on an undirected projection of the filtered flow graph. This preserves the care-network connectivity question while avoiding the path sparsity of the directed municipio -> hospital topology.

## Public-Hospital Final Analysis

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python algorithm_layer/public_hospital_analysis.py \
  --node-analysis data_layer/reports/analysis/node_analysis_2021_public_hospitals.csv \
  --edge-analysis data_layer/reports/analysis/edge_analysis_2021_public_hospitals.csv \
  --facility-panel data_layer/reports/analysis/facility_panel_2021_public_hospitals.csv \
  --out-dir algorithm_layer/reports \
  --prefix final_2021_public_hospitals
```

Outputs include sensitivity, Louvain communities, community-region overlap by UF prefix, public-hospital centrality, dynamic suppression, and K-path redundancy.

The max-flow/min-cut capacity proxy is now opt-in with `--include-capacity-proxy`. It should be treated as exploratory/future work because annual SIH totals are not simultaneous occupancy or staffed capacity.

## Regional Access Analysis

Build the official health-region reference from local CNES ST files:

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python data_layer/build_health_region_reference.py
```

Then calculate regional mismatch and municipal dependency for 25 km and 50 km:

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python algorithm_layer/regional_flow_analysis.py
```

Outputs:

- `regional_2021_public_hospitals_overall.csv`
- `regional_2021_public_hospitals_regional_mismatch.csv`
- `regional_2021_public_hospitals_municipality_dependency.csv`
- `regional_2021_public_hospitals_summary.json`
