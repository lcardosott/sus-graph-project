import unittest

import pandas as pd

from data_layer.run_modular_batch import _resolve_public_hospital_mask


class RunModularBatchTests(unittest.TestCase):
    def test_public_hospital_mask_from_flag_column(self) -> None:
        frame = pd.DataFrame(
            [
                {"cnes": "1", "is_public_hospital": "1"},
                {"cnes": "2", "is_public_hospital": "0"},
                {"cnes": "3", "is_public_hospital": "true"},
            ]
        )

        mask = _resolve_public_hospital_mask(frame)
        self.assertIsNotNone(mask)
        assert mask is not None
        self.assertEqual([True, False, True], mask.tolist())

    def test_public_hospital_mask_from_raw_cnes_fields(self) -> None:
        frame = pd.DataFrame(
            [
                {"vinc_sus": "1", "atendhos": "1", "leithosp": "0", "niv_dep": "1", "nat_jur": ""},
                {"vinc_sus": "1", "atendhos": "0", "leithosp": "1", "niv_dep": "3", "nat_jur": ""},
                {"vinc_sus": "0", "atendhos": "1", "leithosp": "1", "niv_dep": "1", "nat_jur": ""},
            ]
        )

        mask = _resolve_public_hospital_mask(frame)
        self.assertIsNotNone(mask)
        assert mask is not None
        self.assertEqual([True, False, False], mask.tolist())

    def test_public_hospital_mask_returns_none_for_incomplete_schema(self) -> None:
        frame = pd.DataFrame(
            [
                {"cnes": "1", "vinc_sus": "1"},
            ]
        )
        mask = _resolve_public_hospital_mask(frame)
        self.assertIsNone(mask)


if __name__ == "__main__":
    unittest.main()
