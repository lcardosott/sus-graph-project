import unittest

import pandas as pd

from data_layer.column_projection import (
    add_outcome_features,
    project_dataframe,
    resolve_projection_map,
)


class ColumnProjectionTests(unittest.TestCase):
    def test_resolve_projection_map_from_sih_columns(self) -> None:
        columns = [
            "MUNIC_RES",
            "CGC_HOSP",
            "DIAG_PRINC",
            "NASC",
            "IDADE",
            "SEXO",
            "DT_INTER",
            "DT_SAIDA",
            "COBRANCA",
            "DIAS_PERM",
            "MARCA_UTI",
            "PROC_REA",
            "VAL_TOT",
            "RACA_COR",
            "N_AIH",
        ]

        projection_map, missing = resolve_projection_map(columns, include_linkage=True)

        self.assertEqual("MUNIC_RES", projection_map["CODMUNRES"])
        self.assertEqual("CGC_HOSP", projection_map["CNES"])
        self.assertEqual("COBRANCA", projection_map["MOT_SAIDA"])
        self.assertEqual("NASC", projection_map["NASC"])
        self.assertIn("PA_TRANSF", missing)
        self.assertEqual("N_AIH", projection_map["N_AIH"])

    def test_project_dataframe_keeps_only_selected(self) -> None:
        raw = pd.DataFrame(
            {
                "MUNIC_RES": [355030],
                "CGC_HOSP": [1234567],
                "DIAG_PRINC": ["I500"],
                "NASC": ["1950-01-01"],
                "IDADE": [70],
                "SEXO": ["F"],
                "DT_INTER": ["2021-01-10"],
                "DT_SAIDA": ["2021-01-12"],
                "COBRANCA": ["31"],
                "VAL_TOT": [1000.0],
            }
        )
        projection_map, _ = resolve_projection_map(list(raw.columns))
        projected = project_dataframe(raw, projection_map)

        self.assertIn("CODMUNRES", projected.columns)
        self.assertIn("MOT_SAIDA", projected.columns)
        self.assertIn("NASC", projected.columns)
        self.assertNotIn("MUNIC_RES", projected.columns)
        self.assertEqual("31", str(projected.iloc[0]["MOT_SAIDA"]))

    def test_add_outcome_features(self) -> None:
        frame = pd.DataFrame({"MOT_SAIDA": ["31", "11", "21", "41", "99"]})
        out = add_outcome_features(frame)

        self.assertEqual(True, bool(out.iloc[0]["IS_TRANSFER_REGULATED"]))
        self.assertEqual(True, bool(out.iloc[1]["IS_DISCHARGE"]))
        self.assertEqual(True, bool(out.iloc[2]["IS_PERMANENCE"]))
        self.assertEqual(True, bool(out.iloc[3]["IS_DEATH"]))
        self.assertEqual("other", out.iloc[4]["MOT_SAIDA_GROUP"])


if __name__ == "__main__":
    unittest.main()
