"""Edge attribute filters for graph views."""

from __future__ import annotations

from collections.abc import Mapping

import networkx as nx


def _as_float(value: object) -> float | None:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if result != result:
        return None
    return result


def filter_edges_by_min_count(
    graph: nx.Graph,
    *,
    min_counts_by_type: Mapping[str, float] | None = None,
    default_min_count: float = 1.0,
    count_attr: str = "transfer_count",
    edge_type_attr: str = "edge_type",
    include_all_nodes: bool = True,
    keep_missing_count: bool = False,
) -> nx.Graph:
    """Return a graph copy with edges meeting per-type occurrence thresholds."""
    if default_min_count < 0:
        raise ValueError("default_min_count must be non-negative")

    thresholds = {
        str(edge_type).strip().lower(): float(min_count)
        for edge_type, min_count in (min_counts_by_type or {}).items()
    }
    for edge_type, min_count in thresholds.items():
        if min_count < 0:
            raise ValueError(f"min count for {edge_type} must be non-negative")

    filtered = graph.__class__()
    if include_all_nodes:
        filtered.add_nodes_from((node_id, attrs.copy()) for node_id, attrs in graph.nodes(data=True))

    edge_iter = graph.edges(keys=True, data=True) if graph.is_multigraph() else graph.edges(data=True)
    for edge in edge_iter:
        if graph.is_multigraph():
            source, target, key, attrs = edge
        else:
            source, target, attrs = edge
            key = None

        attrs_copy = attrs.copy()
        edge_type = str(attrs_copy.get(edge_type_attr, "")).strip().lower()
        min_count = thresholds.get(edge_type, default_min_count)
        count = _as_float(attrs_copy.get(count_attr))
        if count is None:
            if not keep_missing_count:
                continue
        elif count < min_count:
            continue

        if not include_all_nodes:
            filtered.add_node(source, **graph.nodes[source].copy())
            filtered.add_node(target, **graph.nodes[target].copy())
        if graph.is_multigraph():
            filtered.add_edge(source, target, key=key, **attrs_copy)
        else:
            filtered.add_edge(source, target, **attrs_copy)

    filtered.graph.update(graph.graph.copy())
    filtered.graph["filter"] = {
        "name": "min_count",
        "min_counts_by_type": dict(sorted(thresholds.items())),
        "default_min_count": default_min_count,
        "count_attr": count_attr,
        "include_all_nodes": include_all_nodes,
    }
    return filtered
