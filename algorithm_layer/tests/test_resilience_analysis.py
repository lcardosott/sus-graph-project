import tempfile
import unittest
from argparse import Namespace
from pathlib import Path

import networkx as nx

from algorithm_layer.resilience_analysis import analyze, parse_distance_bands, weighted_undirected_projection


class ResilienceAnalysisTests(unittest.TestCase):
    def test_parse_distance_bands(self) -> None:
        self.assertEqual([50.0, 100.0], parse_distance_bands("100,50,50"))

    def test_weighted_projection_sums_count_and_keeps_shortest_distance(self) -> None:
        graph = nx.DiGraph()
        graph.add_edge("a", "b", distance_km=10, transfer_count=2)
        graph.add_edge("b", "a", distance_km=8, transfer_count=3)

        projected = weighted_undirected_projection(graph)

        self.assertEqual(1, projected.number_of_edges())
        self.assertEqual(5, projected["a"]["b"]["transfer_count"])
        self.assertEqual(8, projected["a"]["b"]["distance_km"])

    def test_analyze_writes_reports(self) -> None:
        graph = nx.DiGraph()
        graph.add_node("municipality:1", node_type="municipality", latitude=0.0, longitude=0.0)
        graph.add_node("facility:a", node_type="facility", name="A", latitude=0.0, longitude=1.0)
        graph.add_node("facility:b", node_type="facility", name="B", latitude=0.0, longitude=2.0)
        graph.add_edge("municipality:1", "facility:a", edge_type="residence", transfer_count=6)
        graph.add_edge("facility:a", "facility:b", edge_type="transfer", transfer_count=2)

        with tempfile.TemporaryDirectory() as tmpdir:
            graph_path = Path(tmpdir) / "graph.gexf"
            nx.write_gexf(graph, graph_path)
            args = Namespace(
                graph_input=str(graph_path),
                out_dir=tmpdir,
                prefix="test",
                distance_bands_km="50",
                min_residence_count=5,
                min_transfer_count=2,
                centrality_top_n=5,
                centrality_node_type="facility",
                centrality_sample_k=0,
                stress_steps=2,
                path_sample_size=3,
                random_seed=1,
            )

            result = analyze(args)

            self.assertEqual(3, result["base_nodes"])
            self.assertEqual(2, result["recurring_edges"])
            self.assertTrue(Path(result["summary"]).exists())
            self.assertTrue(Path(result["centrality"]).exists())
            self.assertTrue(Path(result["stress"]).exists())


if __name__ == "__main__":
    unittest.main()
