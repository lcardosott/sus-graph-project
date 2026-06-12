import unittest

import pandas as pd

from algorithm_layer.public_hospital_analysis import (
    build_directed_graph,
    community_reports,
    filter_edges,
    k_path_redundancy,
    max_flow_min_cut_proxy,
    sensitivity_matrix,
    top_facility_centrality,
    weighted_projection,
)


class PublicHospitalAlgorithmTests(unittest.TestCase):
    def sample_nodes(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"node_id": "municipality:a", "node_type": "municipality", "municipality_code": "350000"},
                {"node_id": "facility:x", "node_type": "facility", "municipality_code": "350001", "name": "X"},
                {"node_id": "facility:y", "node_type": "facility", "municipality_code": "350002", "name": "Y"},
            ]
        )

    def sample_edges(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"source_node_id": "municipality:a", "target_node_id": "facility:x", "edge_type": "residence", "transfer_count": 8, "distance_km": 80},
                {"source_node_id": "municipality:a", "target_node_id": "facility:y", "edge_type": "residence", "transfer_count": 2, "distance_km": 120},
                {"source_node_id": "facility:x", "target_node_id": "facility:y", "edge_type": "transfer", "transfer_count": 2, "distance_km": 50},
            ]
        )

    def test_filter_edges_uses_type_specific_thresholds(self) -> None:
        filtered = filter_edges(self.sample_edges(), 5, 2, 50)
        self.assertEqual(2, len(filtered))

    def test_sensitivity_matrix_outputs_rows(self) -> None:
        rows = sensitivity_matrix(self.sample_nodes(), self.sample_edges(), [5], [2], [50, 100])
        self.assertEqual(2, len(rows))

    def test_community_and_k_paths(self) -> None:
        graph = weighted_projection(build_directed_graph(self.sample_nodes(), self.sample_edges()))
        communities, overlap = community_reports(graph, seed=1)
        centrality = top_facility_centrality(graph, top_n=2, sample_k=0, seed=1)
        paths = k_path_redundancy(graph, self.sample_edges().head(1), top_pairs=1, k_paths=2)
        self.assertGreaterEqual(len(communities), 1)
        self.assertGreaterEqual(len(overlap), 1)
        self.assertGreaterEqual(len(centrality), 1)
        self.assertEqual(1, len(paths))

    def test_max_flow_proxy(self) -> None:
        facilities = pd.DataFrame([{"facility_node_id": "facility:x", "admissions": 10}, {"facility_node_id": "facility:y", "admissions": 10}])
        summary, cut = max_flow_min_cut_proxy(self.sample_edges(), facilities, top_municipalities=1)
        self.assertEqual(1, len(summary))
        self.assertIsInstance(cut, list)


if __name__ == "__main__":
    unittest.main()
