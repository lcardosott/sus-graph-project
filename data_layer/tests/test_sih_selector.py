import unittest

from data_layer.sih_selector import filename_pattern, parse_months, parse_ufs, parse_years


class SihSelectorTests(unittest.TestCase):
    def test_parse_years_single_and_range(self) -> None:
        years = parse_years("2021,2023-2024")
        self.assertEqual({2021, 2023, 2024}, years)

    def test_parse_months_range(self) -> None:
        months = parse_months("1-3,12")
        self.assertEqual({1, 2, 3, 12}, months)

    def test_parse_ufs_all(self) -> None:
        ufs = parse_ufs("ALL")
        self.assertIn("SP", ufs)
        self.assertIn("AM", ufs)

    def test_parse_ufs_invalid_raises(self) -> None:
        with self.assertRaises(ValueError):
            parse_ufs("SP,XX")

    def test_filename_pattern_matches_expected(self) -> None:
        pattern = filename_pattern()
        match = pattern.match("RDSP2107.dbc")
        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual("SP", match.group("uf"))
        self.assertEqual("21", match.group("yy"))
        self.assertEqual("07", match.group("mm"))

    def test_filename_pattern_rejects_non_sih_file(self) -> None:
        pattern = filename_pattern()
        self.assertIsNone(pattern.match("PASP2107a.dbc"))


if __name__ == "__main__":
    unittest.main()
