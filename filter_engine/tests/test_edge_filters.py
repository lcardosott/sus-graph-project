import unittest

import networkx as nx

from filter_engine.edge_filters import filter_edges_by_min_count


class EdgeFilterTests(unittest.TestCase):
    def test_filter_edges_by_type_specific_min_count(self) -> None:
        graph = nx.DiGraph()
        graph.add_node("municipality:a", node_type="municipality")
        graph.add_node("facility:one", node_type="facility")
        graph.add_node("facility:two", node_type="facility")
        graph.add_edge("municipality:a", "facility:one", edge_type="residence", transfer_count=4)
        graph.add_edge("municipality:a", "facility:two", edge_type="residence", transfer_count=8)
        graph.add_edge("facility:one", "facility:two", edge_type="transfer", transfer_count=1)
        graph.add_edge("facility:two", "facility:one", edge_type="transfer", transfer_count=2)

        filtered = filter_edges_by_min_count(
            graph,
            min_counts_by_type={"residence": 5, "transfer": 2},
            include_all_nodes=False,
        )

        self.assertEqual(2, filtered.number_of_edges())
        self.assertTrue(filtered.has_edge("municipality:a", "facility:two"))
        self.assertTrue(filtered.has_edge("facility:two", "facility:one"))
        self.assertEqual(4, graph.number_of_edges())

    def test_missing_count_is_dropped_by_default(self) -> None:
        graph = nx.DiGraph()
        graph.add_edge("a", "b", edge_type="residence")

        filtered = filter_edges_by_min_count(graph)

        self.assertEqual(0, filtered.number_of_edges())


if __name__ == "__main__":
    unittest.main()
