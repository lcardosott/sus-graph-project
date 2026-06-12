import unittest

import networkx as nx

from filter_engine.node_filters import filter_nodes_by_capacity, filter_nodes_by_typology


class NodeFilterTests(unittest.TestCase):
    def build_graph(self) -> nx.DiGraph:
        graph = nx.DiGraph()
        graph.add_node("municipality:1", node_type="municipality")
        graph.add_node("facility:small", node_type="facility", capacity_beds=15, habilitation_level="basic")
        graph.add_node("facility:large", node_type="facility", capacity_beds="120", habilitation_level="high")
        graph.add_node("facility:unknown", node_type="facility")
        graph.add_edge("municipality:1", "facility:small", transfer_count=10)
        graph.add_edge("municipality:1", "facility:large", transfer_count=20)
        return graph

    def test_filter_nodes_by_capacity_keeps_context_nodes(self) -> None:
        graph = self.build_graph()
        filtered = filter_nodes_by_capacity(graph, min_capacity_beds=100)

        self.assertIn("municipality:1", filtered.nodes)
        self.assertIn("facility:large", filtered.nodes)
        self.assertNotIn("facility:small", filtered.nodes)
        self.assertNotIn("facility:unknown", filtered.nodes)
        self.assertTrue(filtered.has_edge("municipality:1", "facility:large"))

    def test_filter_nodes_by_typology(self) -> None:
        graph = self.build_graph()
        filtered = filter_nodes_by_typology(graph, ["high"])

        self.assertIn("facility:large", filtered.nodes)
        self.assertNotIn("facility:small", filtered.nodes)
        self.assertTrue(filtered.has_edge("municipality:1", "facility:large"))

    def test_filters_do_not_mutate_base_graph(self) -> None:
        graph = self.build_graph()
        filtered = filter_nodes_by_capacity(graph, min_capacity_beds=100)
        filtered.remove_node("facility:large")

        self.assertIn("facility:large", graph.nodes)


if __name__ == "__main__":
    unittest.main()
