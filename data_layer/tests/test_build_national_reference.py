import unittest

from data_layer.build_national_reference import _is_public_administration, _is_truthy


class BuildNationalReferenceTests(unittest.TestCase):
    def test_is_truthy_supports_common_values(self) -> None:
        self.assertTrue(_is_truthy("1"))
        self.assertTrue(_is_truthy("true"))
        self.assertTrue(_is_truthy("SIM"))
        self.assertFalse(_is_truthy("0"))

    def test_is_public_administration_from_niv_dep(self) -> None:
        self.assertTrue(_is_public_administration("1", "4000"))
        self.assertFalse(_is_public_administration("3", "4000"))

    def test_is_public_administration_from_nat_jur_family(self) -> None:
        self.assertTrue(_is_public_administration("", "1244"))
        self.assertFalse(_is_public_administration("", "4000"))


if __name__ == "__main__":
    unittest.main()
