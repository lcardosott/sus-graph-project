import unittest

import pandas as pd

from algorithm_layer.regional_flow_analysis import (
    apply_region_join,
    build_municipality_dependency_rows,
    build_overall_rows,
    build_region_mismatch_rows,
)


class RegionalFlowAnalysisTests(unittest.TestCase):
    def sample_edges(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "source_node_id": "municipality:350001",
                    "target_node_id": "facility:a",
                    "edge_type": "residence",
                    "transfer_count": 10,
                    "distance_km": 30,
                    "source_municipality_code": "350001",
                    "target_municipality_code": "350002",
                    "source_uf": "35",
                    "target_uf": "35",
                },
                {
                    "source_node_id": "facility:a",
                    "target_node_id": "facility:b",
                    "edge_type": "transfer",
                    "transfer_count": 2,
                    "distance_km": 60,
                    "source_municipality_code": "350002",
                    "target_municipality_code": "350003",
                    "source_uf": "35",
                    "target_uf": "35",
                },
            ]
        )

    def sample_regions(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"municipality_code": "350001", "health_region_code": "0201", "health_region_id": "SP-0201", "region_conflict": False},
                {"municipality_code": "350002", "health_region_code": "0202", "health_region_id": "SP-0202", "region_conflict": False},
                {"municipality_code": "350003", "health_region_code": "0202", "health_region_id": "SP-0202", "region_conflict": False},
            ]
        )

    def test_region_join_and_summaries(self) -> None:
        edges = apply_region_join(self.sample_edges(), self.sample_regions())
        overall = build_overall_rows(edges, [25], 5, 2)
        mismatch = build_region_mismatch_rows(edges, [25], 5, 2)
        nodes = pd.DataFrame(
            [
                {"node_id": "municipality:350001", "name": "Origem"},
                {"node_id": "facility:a", "name": "Hospital A"},
            ]
        )
        dependency = build_municipality_dependency_rows(edges, nodes, [25], 5)

        self.assertEqual(2, len(overall))
        self.assertEqual(1, len(mismatch))
        self.assertEqual(1, len(dependency))
        self.assertEqual(1.0, dependency[0]["cross_health_region_share"])


if __name__ == "__main__":
    unittest.main()
