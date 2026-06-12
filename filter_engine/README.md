# Filter Engine

Pure NetworkX filters for creating analysis views from the validated base graph.

## Current filters

- `remove_short_edges`: keeps only long-distance edges, useful for regional evasion analysis.
- `filter_edges_by_distance`: keeps edges inside a configurable distance interval.
- `filter_edges_by_min_count`: removes low-occurrence edges with separate thresholds per edge type.
- `filter_nodes_by_capacity`: induced node subgraph by numeric capacity.
- `filter_nodes_by_typology`: induced node subgraph by categorical facility attributes.

Distance is read from `distance_km` when present. If the edge has no distance attribute, it is computed from endpoint `latitude` and `longitude`.

## Contract

All functions return a copied graph/subgraph and do not mutate the base graph.

Example:

```python
import networkx as nx
from filter_engine import remove_short_edges

graph = nx.read_gexf("model_layer/reports/graph_sih_br_2021.gexf")
recurring = filter_edges_by_min_count(
    graph,
    min_counts_by_type={"residence": 5, "transfer": 2},
    include_all_nodes=False,
)
long_edges = remove_short_edges(recurring, min_distance_km=50)
```
