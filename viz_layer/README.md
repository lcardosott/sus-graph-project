# Visualization Layer

Two local viewers are available.

## Assumptions (2026-04-27)

- The full-year nodes/edges are already validated and stored, so browser-safe assets can be derived without rerunning the heavy pipeline; limits are set to keep map and HTML rendering responsive on typical laptops.

## 1) Map UI (geographic)
Start a local server:

```bash
python -m http.server 8000
```

Open:

```
http://localhost:8000/viz_layer/graph_map_ui.html
```

The final UI uses the lightweight guided layer
[viz_layer/reports/final_map_layers.json](reports/final_map_layers.json), generated from the public-hospital algorithm outputs.

To generate browser-safe JSONL/meta from the full nodes/edges CSV:

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python viz_layer/prepare_browser_safe_assets.py \
  --nodes-input data_layer/reports/batches/sih_br_2021_nodes.csv \
  --edges-input data_layer/reports/batches/sih_br_2021_edges.csv \
  --out-dir data_layer/reports/batches/ui \
  --prefix sih_br_2021_safe \
  --max-nodes 20000 \
  --max-edges 40000
```

The older browser-safe JSONL mode was kept only as a reproducibility fallback. The final presentation path is:

```
http://localhost:8000/viz_layer/graph_map_ui.html
```

## 2) Simple graph HTML (non-geographic)

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python viz_layer/render_graph_html.py \
  --graph-input model_layer/reports/graph_sih_br_2021.gexf \
  --html-output viz_layer/reports/graph_sih_br_2021_simple_safe.html \
  --max-nodes 1200 \
  --max-edges 3000
```

Open:

```
http://localhost:8000/viz_layer/reports/graph_sih_br_2021_simple_safe.html
```

## Metrics report

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python viz_layer/graph_metrics.py \
  --graph-input model_layer/reports/graph_sih_br_2021.gexf \
  --out-dir viz_layer/reports \
  --prefix graph_sih_br_2021 \
  --bins 40
```

Outputs:
- [viz_layer/reports/graph_sih_br_2021_metrics_summary.json](reports/graph_sih_br_2021_metrics_summary.json)
- [viz_layer/reports/graph_sih_br_2021_degree_distribution.png](reports/graph_sih_br_2021_degree_distribution.png)
- [viz_layer/reports/graph_sih_br_2021_component_size_distribution.png](reports/graph_sih_br_2021_component_size_distribution.png)

## Final public-hospital showcase

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python viz_layer/build_final_showcase.py
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python viz_layer/build_final_map_layers.py
```

Open:

```
http://localhost:8000/viz_layer/graph_map_ui.html
```

Public-hospital map assets:

```
viz_layer/reports/final_map_layers.json
```
