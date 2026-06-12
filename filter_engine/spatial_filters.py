"""Spatial filters for graph views.

The functions in this module are intentionally pure: they return a copied
subgraph and never mutate the input graph.
"""

from __future__ import annotations

from math import asin, cos, radians, sin, sqrt
from typing import Iterable

import networkx as nx


EARTH_RADIUS_KM = 6371.0088


def haversine_km(
    lat_a: float,
    lon_a: float,
    lat_b: float,
    lon_b: float,
) -> float:
    """Return great-circle distance in kilometers between two coordinates."""
    phi_a = radians(float(lat_a))
    phi_b = radians(float(lat_b))
    delta_phi = radians(float(lat_b) - float(lat_a))
    delta_lambda = radians(float(lon_b) - float(lon_a))

    hav = sin(delta_phi / 2.0) ** 2 + cos(phi_a) * cos(phi_b) * sin(delta_lambda / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_KM * asin(sqrt(hav))


def _as_float(value: object) -> float | None:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if result != result:
        return None
    return result


def _edge_distance_km(
    graph: nx.Graph,
    source: object,
    target: object,
    attrs: dict[str, object],
    distance_attr: str,
) -> float | None:
    stored_distance = _as_float(attrs.get(distance_attr))
    if stored_distance is not None:
        return stored_distance

    source_attrs = graph.nodes[source]
    target_attrs = graph.nodes[target]
    source_lat = _as_float(source_attrs.get("latitude"))
    source_lon = _as_float(source_attrs.get("longitude"))
    target_lat = _as_float(target_attrs.get("latitude"))
    target_lon = _as_float(target_attrs.get("longitude"))
    if None in (source_lat, source_lon, target_lat, target_lon):
        return None

    return haversine_km(source_lat, source_lon, target_lat, target_lon)


def _edge_type_allowed(attrs: dict[str, object], edge_types: set[str] | None) -> bool:
    if edge_types is None:
        return True
    return str(attrs.get("edge_type", "")).strip().lower() in edge_types


def filter_edges_by_distance(
    graph: nx.Graph,
    min_distance_km: float | None = None,
    max_distance_km: float | None = None,
    *,
    edge_types: Iterable[str] | None = None,
    distance_attr: str = "distance_km",
    include_all_nodes: bool = True,
    keep_edges_with_missing_distance: bool = False,
    annotate_distance: bool = True,
) -> nx.Graph:
    """Return a graph copy containing only edges within the distance interval.

    Distances are read from ``distance_attr`` when present; otherwise they are
    computed from endpoint ``latitude``/``longitude`` node attributes.
    """
    if min_distance_km is None and max_distance_km is None:
        return graph.copy()
    if min_distance_km is not None and min_distance_km < 0:
        raise ValueError("min_distance_km must be non-negative")
    if max_distance_km is not None and max_distance_km < 0:
        raise ValueError("max_distance_km must be non-negative")
    if min_distance_km is not None and max_distance_km is not None and min_distance_km > max_distance_km:
        raise ValueError("min_distance_km cannot be greater than max_distance_km")

    allowed_types = {str(edge_type).strip().lower() for edge_type in edge_types} if edge_types is not None else None
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
        if not _edge_type_allowed(attrs_copy, allowed_types):
            continue

        distance = _edge_distance_km(graph, source, target, attrs_copy, distance_attr)
        if distance is None:
            if not keep_edges_with_missing_distance:
                continue
        else:
            if min_distance_km is not None and distance < min_distance_km:
                continue
            if max_distance_km is not None and distance > max_distance_km:
                continue
            if annotate_distance:
                attrs_copy[distance_attr] = distance

        if not include_all_nodes:
            filtered.add_node(source, **graph.nodes[source].copy())
            filtered.add_node(target, **graph.nodes[target].copy())
        if graph.is_multigraph():
            filtered.add_edge(source, target, key=key, **attrs_copy)
        else:
            filtered.add_edge(source, target, **attrs_copy)

    filtered.graph.update(graph.graph.copy())
    filtered.graph["filter"] = {
        "name": "distance",
        "min_distance_km": min_distance_km,
        "max_distance_km": max_distance_km,
        "edge_types": sorted(allowed_types) if allowed_types is not None else None,
        "include_all_nodes": include_all_nodes,
    }
    return filtered


def remove_short_edges(
    graph: nx.Graph,
    min_distance_km: float = 50.0,
    **kwargs: object,
) -> nx.Graph:
    """Return a graph copy without edges shorter than ``min_distance_km``."""
    return filter_edges_by_distance(graph, min_distance_km=min_distance_km, **kwargs)
