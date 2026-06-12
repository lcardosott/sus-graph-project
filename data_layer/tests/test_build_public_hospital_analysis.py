import unittest

import pandas as pd

from data_layer.build_public_hospital_analysis import (
    build_edge_analysis,
    build_facility_panel,
    build_node_analysis,
    node_id_from_cnes,
)


class PublicHospitalAnalysisTests(unittest.TestCase):
    def test_node_id_from_cnes_normalizes_to_seven_digits(self) -> None:
        self.assertEqual("facility:0000035", node_id_from_cnes("35"))

    def test_build_facility_panel_filters_public_hospitals(self) -> None:
        records = pd.DataFrame(
            [
                {"CNES": "35", "IDADE": "50", "DIAS_PERM": "2", "VAL_TOT": "100", "IS_DEATH": False, "IS_TRANSFER_REGULATED": True, "MARCA_UTI": "", "DIAG_PRINC": "I500", "SEXO": "M", "RACA_COR": "1", "CODMUNRES": "355030", "PROC_REA": "x"},
                {"CNES": "99", "IDADE": "70", "DIAS_PERM": "4", "VAL_TOT": "200", "IS_DEATH": True, "IS_TRANSFER_REGULATED": False, "MARCA_UTI": "1", "DIAG_PRINC": "J180", "SEXO": "F", "RACA_COR": "2", "CODMUNRES": "355030", "PROC_REA": "y"},
            ]
        )

        panel = build_facility_panel(records, {"facility:0000035"})

        self.assertEqual(1, len(panel))
        self.assertEqual("facility:0000035", panel.iloc[0]["facility_node_id"])
        self.assertEqual(1, int(panel.iloc[0]["regulated_transfer_exits"]))

    def test_build_edge_and_node_analysis_scope(self) -> None:
        nodes = pd.DataFrame(
            [
                {"node_id": "municipality:355030", "node_type": "municipality", "name": "Sao Paulo", "municipality_code": "355030", "latitude": "-23.5", "longitude": "-46.6"},
                {"node_id": "facility:0000035", "node_type": "facility", "name": "Hosp", "municipality_code": "355030", "latitude": "-23.6", "longitude": "-46.7"},
                {"node_id": "facility:0000099", "node_type": "facility", "name": "Other", "municipality_code": "355030", "latitude": "-23.7", "longitude": "-46.8"},
            ]
        )
        edges = pd.DataFrame(
            [
                {"source_node_id": "municipality:355030", "target_node_id": "facility:0000035", "edge_type": "residence", "transfer_count": "5"},
                {"source_node_id": "municipality:355030", "target_node_id": "facility:0000099", "edge_type": "residence", "transfer_count": "5"},
            ]
        )
        panel = pd.DataFrame([{"facility_node_id": "facility:0000035", "admissions": 10, "deaths": 1, "death_rate": 0.1, "mean_stay_days": 2, "regulated_transfer_exit_rate": 0.2, "icu_marker_rate": 0.0, "mean_value_per_admission": 100, "dominant_icd_chapter": "I"}])
        public_ref = pd.DataFrame([{"cnes": "0000035", "is_public_hospital": "1"}])

        edge_analysis, residence, transfer = build_edge_analysis(edges, nodes, {"facility:0000035"}, panel)
        node_analysis = build_node_analysis(nodes, public_ref, panel, {"facility:0000035"})

        self.assertEqual(1, len(edge_analysis))
        self.assertEqual(1, len(residence))
        self.assertEqual(0, len(transfer))
        self.assertEqual("excluded_non_public_hospital", node_analysis[node_analysis["node_id"] == "facility:0000099"].iloc[0]["analysis_scope"])


if __name__ == "__main__":
    unittest.main()
