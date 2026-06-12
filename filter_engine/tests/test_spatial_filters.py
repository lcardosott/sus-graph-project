import unittest

import networkx as nx

from filter_engine.spatial_filters import filter_edges_by_distance, haversine_km, remove_short_edges


class SpatialFilterTests(unittest.TestCase):
    def build_graph(self) -> nx.DiGraph:
        graph = nx.DiGraph()
        graph.add_node("municipality:a", node_type="municipality", latitude=0.0, longitude=0.0)
        graph.add_node("facility:near", node_type="facility", latitude=0.0, longitude=0.1)
        graph.add_node("facility:far", node_type="facility", latitude=0.0, longitude=1.0)
        graph.add_edge("municipality:a", "facility:near", edge_type="residence", transfer_count=10)
        graph.add_edge("municipality:a", "facility:far", edge_type="residence", transfer_count=2)
        graph.add_edge("facility:near", "facility:far", edge_type="transfer", distance_km=99.0)
        return graph

    def test_haversine_returns_expected_scale(self) -> None:
        distance = haversine_km(0.0, 0.0, 0.0, 1.0)
        self.assertAlmostEqual(111.2, distance, delta=0.5)

    def test_remove_short_edges_keeps_long_edges_without_mutating_base(self) -> None:
        graph = self.build_graph()
        filtered = remove_short_edges(graph, min_distance_km=50.0)

        self.assertEqual(3, graph.number_of_edges())
        self.assertEqual(2, filtered.number_of_edges())
        self.assertTrue(filtered.has_edge("municipality:a", "facility:far"))
        self.assertTrue(filtered.has_edge("facility:near", "facility:far"))
        self.assertFalse(graph["municipality:a"]["facility:far"].get("distance_km"))
        self.assertIn("distance_km", filtered["municipality:a"]["facility:far"])

    def test_filter_can_target_edge_types(self) -> None:
        graph = self.build_graph()
        filtered = filter_edges_by_distance(graph, min_distance_km=50.0, edge_types=["residence"])

        self.assertEqual(1, filtered.number_of_edges())
        self.assertTrue(filtered.has_edge("municipality:a", "facility:far"))

    def test_can_drop_isolated_nodes(self) -> None:
        graph = self.build_graph()
        filtered = remove_short_edges(graph, min_distance_km=50.0, include_all_nodes=False, edge_types=["residence"])

        self.assertEqual({"municipality:a", "facility:far"}, set(filtered.nodes))


if __name__ == "__main__":
    unittest.main()
