import unittest

import pandas as pd

from data_layer.node_mapping import (
    append_missing_municipality_nodes,
    build_nodes_from_records,
    map_edges_to_node_ids,
    summarize_edge_node_coverage,
)


class NodeMappingTests(unittest.TestCase):
    def test_build_nodes_from_sih_records(self) -> None:
        records = pd.DataFrame(
            [
                {"CNES": "1001", "MUNIC_MOV": "355030", "MUNIC_RES": "355030"},
                {"CNES": "1001", "MUNIC_MOV": "355030", "MUNIC_RES": "350950"},
                {"CNES": "1002", "MUNIC_MOV": "350950", "MUNIC_RES": "355030"},
                {"CNES": "1002", "MUNIC_MOV": "350950", "MUNIC_RES": "351060"},
            ]
        )

        nodes, facility_lookup, municipality_lookup, metadata = build_nodes_from_records(records)

        self.assertEqual("CNES", metadata.facility_id_field)
        self.assertEqual("MUNIC_MOV", metadata.facility_municipality_field)
        self.assertEqual(2, int((nodes["node_type"] == "facility").sum()))
        self.assertEqual(3, int((nodes["node_type"] == "municipality").sum()))

        facility_1001 = facility_lookup[facility_lookup["location_id"] == "1001"].iloc[0]
        self.assertEqual("facility:1001", facility_1001["node_id"])
        self.assertEqual("355030", facility_1001["municipality_code"])

        municipality_ids = set(municipality_lookup["node_id"].tolist())
        self.assertIn("municipality:350950", municipality_ids)
        self.assertIn("municipality:355030", municipality_ids)
        self.assertIn("municipality:351060", municipality_ids)

    def test_map_edges_to_node_ids_and_coverage(self) -> None:
        nodes = pd.DataFrame(
            [
                {"node_id": "facility:1001"},
                {"node_id": "facility:1002"},
                {"node_id": "municipality:355030"},
            ]
        )

        edges = pd.DataFrame(
            [
                {
                    "source_facility_id": "1001",
                    "target_location_id": "1002",
                    "target_location_type": "facility",
                },
                {
                    "source_facility_id": "1001",
                    "target_location_id": "355030",
                    "target_location_type": "municipality",
                },
                {
                    "source_facility_id": "9999",
                    "target_location_id": "355030",
                    "target_location_type": "municipality",
                },
            ]
        )

        mapped = map_edges_to_node_ids(edges, known_nodes=nodes)

        self.assertEqual("facility:1001", mapped.iloc[0]["source_node_id"])
        self.assertEqual("facility:1002", mapped.iloc[0]["target_node_id"])
        self.assertEqual("municipality:355030", mapped.iloc[1]["target_node_id"])
        self.assertFalse(bool(mapped.iloc[2]["source_node_exists"]))
        self.assertTrue(bool(mapped.iloc[2]["target_node_exists"]))

        summary = summarize_edge_node_coverage(mapped)
        self.assertEqual(3, summary["edge_rows"])
        self.assertEqual(1, summary["source_node_missing"])
        self.assertEqual(0, summary["target_node_missing"])

    def test_append_missing_municipality_nodes(self) -> None:
        nodes = pd.DataFrame(
            [
                {
                    "node_id": "municipality:355030",
                    "node_type": "municipality",
                    "name": "Sao Paulo - SP",
                    "municipality_code": "355030",
                    "latitude": pd.NA,
                    "longitude": pd.NA,
                    "capacity_beds": pd.NA,
                    "habilitation_level": pd.NA,
                },
                {
                    "node_id": "facility:1001",
                    "node_type": "facility",
                    "name": "CNES 1001",
                    "municipality_code": "355030",
                    "latitude": pd.NA,
                    "longitude": pd.NA,
                    "capacity_beds": pd.NA,
                    "habilitation_level": pd.NA,
                },
            ]
        )

        out, added = append_missing_municipality_nodes(nodes, ["355030", "330455", "130260"])
        municipality_ids = set(out[out["node_type"] == "municipality"]["node_id"].tolist())

        self.assertEqual(2, added)
        self.assertIn("municipality:355030", municipality_ids)
        self.assertIn("municipality:330455", municipality_ids)
        self.assertIn("municipality:130260", municipality_ids)


if __name__ == "__main__":
    unittest.main()
