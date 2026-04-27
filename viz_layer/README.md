# Visualization Layer

Two local viewers are available.

## 1) Map UI (geographic)
Start a local server:

```bash
python -m http.server 8000
```

Open:

```
http://localhost:8000/viz_layer/graph_map_ui.html
```

The UI uses JSONL in [data_layer/reports/batches/ui](../data_layer/reports/batches/ui).

## 2) Simple graph HTML (non-geographic)

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
