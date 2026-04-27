import tempfile
import unittest
from pathlib import Path

from data_layer.schema_gate import evaluate_schema


def write_codebook(path: Path, rows: list[str]) -> None:
    path.write_text(
        "code,description,is_transfer,validated,validated_on,source\n" + "\n".join(rows) + "\n",
        encoding="utf-8",
    )


class SchemaGateTests(unittest.TestCase):
    def test_fails_when_datetime_pair_is_missing(self) -> None:
        header = [
            "PA_TRANSF",
            "PA_MOTSAI",
            "PA_SEXO",
            "PA_IDADE",
            "PA_CIDPRI",
            "PA_CODUNI",
            "PA_CNSMED",
            "PA_MUNPCN",
            "PA_CMP",
            "PA_MVM",
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            codebook_path = Path(temp_dir) / "motsai_codes.csv"
            write_codebook(
                codebook_path,
                ["01,Validated transfer code,true,true,2026-04-08,DATASUS"],
            )

            result = evaluate_schema(header, Path("sample.csv"), codebook_path)

        self.assertFalse(result.passed)
        self.assertIn("No day-level admission/discharge datetime pair found", " ".join(result.blockers))

    def test_passes_with_datetime_pair_and_valid_codebook(self) -> None:
        header = [
            "PA_TRANSF",
            "PA_MOTSAI",
            "PA_SEXO",
            "PA_IDADE",
            "PA_CIDPRI",
            "PA_CODUNI",
            "PA_CNSMED",
            "PA_MUNPCN",
            "DT_INTER",
            "DT_SAIDA",
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            codebook_path = Path(temp_dir) / "motsai_codes.csv"
            write_codebook(
                codebook_path,
                ["01,Validated transfer code,true,true,2026-04-08,DATASUS"],
            )

            result = evaluate_schema(header, Path("sample.csv"), codebook_path)

        self.assertTrue(result.passed)

    def test_fails_with_unvalidated_codebook(self) -> None:
        header = [
            "PA_TRANSF",
            "PA_MOTSAI",
            "PA_SEXO",
            "PA_IDADE",
            "PA_CIDPRI",
            "PA_CODUNI",
            "PA_CNSMED",
            "PA_MUNPCN",
            "DT_INTER",
            "DT_SAIDA",
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            codebook_path = Path(temp_dir) / "motsai_codes.csv"
            write_codebook(
                codebook_path,
                ["00,Placeholder,false,false,,pending"],
            )

            result = evaluate_schema(header, Path("sample.csv"), codebook_path)

        self.assertFalse(result.passed)
        self.assertIn("zero validated transfer-related codes", " ".join(result.blockers))

    def test_passes_for_sih_profile_with_daily_dates(self) -> None:
        header = [
            "N_AIH",
            "MUNIC_RES",
            "CNES",
            "DIAG_PRINC",
            "IDADE",
            "SEXO",
            "DT_INTER",
            "DT_SAIDA",
            "MOT_SAIDA",
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            codebook_path = Path(temp_dir) / "motsai_codes.csv"
            write_codebook(
                codebook_path,
                ["31,Transferencia para outro estabelecimento,true,true,2026-04-08,Manual SIH"],
            )

            result = evaluate_schema(header, Path("sih_sample.csv"), codebook_path)

        self.assertTrue(result.passed)
        self.assertEqual("sih", result.profile)
        self.assertEqual("N_AIH", result.patient_key_field)
        self.assertEqual(["DT_INTER", "DT_SAIDA"], result.datetime_pair)
        self.assertFalse(result.probabilistic_linkage_supported)

    def test_passes_without_deterministic_key_when_probabilistic_fields_exist(self) -> None:
        header = [
            "MUNIC_RES",
            "CNES",
            "DIAG_PRINC",
            "IDADE",
            "SEXO",
            "NASC",
            "DT_INTER",
            "DT_SAIDA",
            "MOT_SAIDA",
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            codebook_path = Path(temp_dir) / "motsai_codes.csv"
            write_codebook(
                codebook_path,
                ["31,Transferencia para outro estabelecimento,true,true,2026-04-08,Manual SIH"],
            )

            result = evaluate_schema(header, Path("sih_probabilistic_sample.csv"), codebook_path)

        self.assertTrue(result.passed)
        self.assertIsNone(result.patient_key_field)
        self.assertTrue(result.probabilistic_linkage_supported)
        self.assertEqual("NASC", result.probabilistic_birthdate_field)
        self.assertEqual("MUNIC_RES", result.probabilistic_residence_field)


if __name__ == "__main__":
    unittest.main()