"""Node attribute filters for graph views."""

from __future__ import annotations

from collections.abc import Iterable

import networkx as nx


def _as_float(value: object) -> float | None:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if result != result:
        return None
    return result


def _copy_induced_subgraph(graph: nx.Graph, node_ids: Iterable[object], filter_metadata: dict[str, object]) -> nx.Graph:
    filtered = graph.subgraph(list(node_ids)).copy()
    filtered.graph.update(graph.graph.copy())
    filtered.graph["filter"] = filter_metadata
    return filtered


def filter_nodes_by_capacity(
    graph: nx.Graph,
    min_capacity_beds: float | None = None,
    max_capacity_beds: float | None = None,
    *,
    capacity_attr: str = "capacity_beds",
    keep_missing: bool = False,
    node_types: Iterable[str] | None = ("facility",),
) -> nx.Graph:
    """Return an induced subgraph filtered by numeric capacity attributes."""
    if min_capacity_beds is None and max_capacity_beds is None:
        return graph.copy()
    if min_capacity_beds is not None and min_capacity_beds < 0:
        raise ValueError("min_capacity_beds must be non-negative")
    if max_capacity_beds is not None and max_capacity_beds < 0:
        raise ValueError("max_capacity_beds must be non-negative")
    if min_capacity_beds is not None and max_capacity_beds is not None and min_capacity_beds > max_capacity_beds:
        raise ValueError("min_capacity_beds cannot be greater than max_capacity_beds")

    filtered_node_types = {str(node_type).strip().lower() for node_type in node_types} if node_types is not None else None
    selected: list[object] = []
    for node_id, attrs in graph.nodes(data=True):
        node_type = str(attrs.get("node_type", "")).strip().lower()
        if filtered_node_types is not None and node_type not in filtered_node_types:
            selected.append(node_id)
            continue

        capacity = _as_float(attrs.get(capacity_attr))
        if capacity is None:
            if keep_missing:
                selected.append(node_id)
            continue
        if min_capacity_beds is not None and capacity < min_capacity_beds:
            continue
        if max_capacity_beds is not None and capacity > max_capacity_beds:
            continue
        selected.append(node_id)

    return _copy_induced_subgraph(
        graph,
        selected,
        {
            "name": "capacity",
            "min_capacity_beds": min_capacity_beds,
            "max_capacity_beds": max_capacity_beds,
            "capacity_attr": capacity_attr,
            "keep_missing": keep_missing,
            "node_types": sorted(filtered_node_types) if filtered_node_types is not None else None,
        },
    )


def filter_nodes_by_typology(
    graph: nx.Graph,
    allowed_values: Iterable[str],
    *,
    typology_attr: str = "habilitation_level",
    keep_missing: bool = False,
    node_types: Iterable[str] | None = ("facility",),
) -> nx.Graph:
    """Return an induced subgraph filtered by categorical typology attributes."""
    allowed = {str(value).strip().lower() for value in allowed_values}
    if not allowed:
        raise ValueError("allowed_values must contain at least one value")

    filtered_node_types = {str(node_type).strip().lower() for node_type in node_types} if node_types is not None else None
    selected: list[object] = []
    for node_id, attrs in graph.nodes(data=True):
        node_type = str(attrs.get("node_type", "")).strip().lower()
        if filtered_node_types is not None and node_type not in filtered_node_types:
            selected.append(node_id)
            continue

        value = attrs.get(typology_attr)
        if value is None or str(value).strip() == "":
            if keep_missing:
                selected.append(node_id)
            continue
        if str(value).strip().lower() in allowed:
            selected.append(node_id)

    return _copy_induced_subgraph(
        graph,
        selected,
        {
            "name": "typology",
            "allowed_values": sorted(allowed),
            "typology_attr": typology_attr,
            "keep_missing": keep_missing,
            "node_types": sorted(filtered_node_types) if filtered_node_types is not None else None,
        },
    )
