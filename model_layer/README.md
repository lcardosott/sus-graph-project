# Model Layer

This layer builds the directed graph from yearly nodes and edges.

## Build graph

```bash
/home/lulutoratora/Documents/comp/mc859/.venv/bin/python model_layer/build_graph.py \
  --nodes-input data_layer/reports/batches/sih_br_2021_nodes.csv \
  --edges-input data_layer/reports/batches/sih_br_2021_edges.csv \
  --graph-output model_layer/reports/graph_sih_br_2021.gexf \
  --summary-output model_layer/reports/graph_sih_br_2021_summary.json
```

## Outputs kept in repo
- [model_layer/reports/graph_sih_br_2021.gexf](reports/graph_sih_br_2021.gexf)
- [model_layer/reports/graph_sih_br_2021_summary.json](reports/graph_sih_br_2021_summary.json)
