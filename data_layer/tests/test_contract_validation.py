import unittest

import pandas as pd

from data_layer.contract_validation import ValidationThresholds, validate_nodes_edges_contract


class ContractValidationTests(unittest.TestCase):
    def test_passes_with_valid_nodes_and_edges(self) -> None:
        nodes = pd.DataFrame(
            [
                {"node_id": "facility:1", "node_type": "facility", "municipality_code": "355030", "name": "Hosp 1"},
                {"node_id": "municipality:355030", "node_type": "municipality", "municipality_code": "355030", "name": "Sao Paulo"},
            ]
        )
        edges = pd.DataFrame(
            [
                {"source_node_id": "facility:1", "target_node_id": "municipality:355030", "transfer_count": "2"},
            ]
        )

        report = validate_nodes_edges_contract(nodes, edges)
        self.assertTrue(report["passed"])
        self.assertEqual(0, len(report["errors"]))

    def test_fails_when_edge_points_to_missing_node(self) -> None:
        nodes = pd.DataFrame(
            [
                {"node_id": "facility:1", "node_type": "facility", "municipality_code": "355030", "name": "Hosp 1"},
            ]
        )
        edges = pd.DataFrame(
            [
                {"source_node_id": "facility:1", "target_node_id": "municipality:355030", "transfer_count": "1"},
            ]
        )

        report = validate_nodes_edges_contract(
            nodes,
            edges,
            thresholds=ValidationThresholds(max_missing_node_ratio=0.0),
        )
        self.assertFalse(report["passed"])
        self.assertIn("Missing edge-node references ratio", " ".join(report["errors"]))

    def test_warns_when_name_coverage_is_low(self) -> None:
        nodes = pd.DataFrame(
            [
                {"node_id": "facility:1", "node_type": "facility", "municipality_code": "355030", "name": ""},
                {"node_id": "municipality:355030", "node_type": "municipality", "municipality_code": "355030", "name": ""},
            ]
        )
        edges = pd.DataFrame(
            [
                {"source_node_id": "facility:1", "target_node_id": "municipality:355030", "transfer_count": "1"},
            ]
        )

        report = validate_nodes_edges_contract(
            nodes,
            edges,
            thresholds=ValidationThresholds(min_node_name_coverage=0.9),
        )
        self.assertTrue(report["passed"])
        self.assertGreater(len(report["warnings"]), 0)


if __name__ == "__main__":
    unittest.main()
