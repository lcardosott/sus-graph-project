import unittest

from data_layer.siasus_selector import filename_pattern, parse_months, parse_years


class SiasusSelectorTests(unittest.TestCase):
    def test_parse_years_single_and_range(self) -> None:
        years = parse_years("2021,2023-2024")
        self.assertEqual({2021, 2023, 2024}, years)

    def test_parse_months_range(self) -> None:
        months = parse_months("1-3,12")
        self.assertEqual({1, 2, 3, 12}, months)

    def test_parse_months_out_of_bounds_raises(self) -> None:
        with self.assertRaises(ValueError):
            parse_months("0")

    def test_filename_pattern_matches_expected(self) -> None:
        pattern = filename_pattern("PASP")
        match = pattern.match("PASP2107b.dbc")
        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual("21", match.group("yy"))
        self.assertEqual("07", match.group("mm"))
        self.assertEqual("b", match.group("part"))

    def test_filename_pattern_rejects_other_prefix(self) -> None:
        pattern = filename_pattern("PASP")
        self.assertIsNone(pattern.match("PARG2107b.dbc"))


if __name__ == "__main__":
    unittest.main()