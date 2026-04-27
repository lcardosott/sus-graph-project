# SUS Graph Project

This repository builds a national patient flow graph from SIH data, plus a local visualization UI.

## What is included
- Data processing scripts (projection, transfer matching, node/edge building)
- Graph build utilities
- Local visualization (map + simple graph)
- UI JSONL artifacts used by the local map

## Data sources
- SIH (DATASUS SIH) raw DBC files
- CNES (DATASUS CNES ST files and optional CNES API)
- IBGE municipalities catalog + centroid coordinates
- Transfer reason codebook (MOTSAI)

## Output artifacts kept in repo
- UI JSONL and meta for the map UI in [data_layer/reports/batches/ui](data_layer/reports/batches/ui)
- Yearly graph GEXF in [model_layer/reports/graph_sih_br_2021.gexf](model_layer/reports/graph_sih_br_2021.gexf)
- Graph summary JSON in [model_layer/reports/graph_sih_br_2021_summary.json](model_layer/reports/graph_sih_br_2021_summary.json)
- Metrics summary and plots in [viz_layer/reports](viz_layer/reports)

Raw data and large intermediate files are ignored by git.

## Metrics (2021)
From [viz_layer/reports/graph_sih_br_2021_metrics_summary.json](viz_layer/reports/graph_sih_br_2021_metrics_summary.json):
- Nodes: 11823
- Edges: 203184
- Average degree: 34.370972
- Strongly connected components: 11488

## Transfer detection rule (summary)
Transfers are inferred with a 4-pillar heuristic:
- Use transfer flags and validated discharge reason codes as anchors
- Match candidate admissions within a time window (24 to 48 hours)
- Require demographic continuity (sex and age)
- Require clinical continuity (ICD chapter)

Residence edges are built as municipio de residencia -> hospital for all admissions.

## Local visualization

### 1) Map UI (geographic)
Serve locally:

```bash
python -m http.server 8000
```

Open:

```
http://localhost:8000/viz_layer/graph_map_ui.html
```

### 2) Simple graph HTML (non-geographic)

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python viz_layer/render_graph_html.py \
  --graph-input model_layer/reports/graph_sih_br_2021.gexf \
  --html-output viz_layer/reports/graph_sih_br_2021_simple.html \
  --max-nodes 1500 \
  --max-edges 4000
```

Open:

```
http://localhost:8000/viz_layer/reports/graph_sih_br_2021_simple.html
```

## Reproducibility (2021)
Raw SIH files are not stored in git. To regenerate full outputs:

1) Download SIH DBC files with [data_layer/sih_selector.py](data_layer/sih_selector.py)
2) Run the yearly pipeline with [data_layer/run_sih_year_pipeline.py](data_layer/run_sih_year_pipeline.py)

See the detailed run steps in [data_layer/README.md](data_layer/README.md).
