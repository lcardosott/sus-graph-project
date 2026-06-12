import unittest
from collections import Counter

from data_layer.build_health_region_reference import most_common_region, normalize_region


class HealthRegionReferenceTests(unittest.TestCase):
    def test_normalize_region_keeps_last_four_digits(self) -> None:
        self.assertEqual("0209", normalize_region("R209"))
        self.assertEqual("0027", normalize_region("027"))

    def test_most_common_region_ignores_empty_codes(self) -> None:
        code, count = most_common_region(Counter({"": 10, "0209": 3, "0210": 1}))
        self.assertEqual("0209", code)
        self.assertEqual(3, count)


if __name__ == "__main__":
    unittest.main()
