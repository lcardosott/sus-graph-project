import unittest
from pathlib import Path
import tempfile
from unittest.mock import patch

import pandas as pd

from data_layer.node_enrichment import (
    MunicipalityCenter,
    extract_catalog_municipality_codes,
    enrich_nodes_with_facility_reference,
    enrich_nodes_with_municipality_metadata,
    load_or_refresh_cnes_facility_reference,
    normalize_municipality_code,
)


class NodeEnrichmentTests(unittest.TestCase):
    def test_normalize_municipality_code(self) -> None:
        self.assertEqual("3550308", normalize_municipality_code("3550308"))
        self.assertEqual("355030", normalize_municipality_code("355030"))
        self.assertEqual("3550308", normalize_municipality_code("3550308.0"))
        self.assertEqual("", normalize_municipality_code(None))

    def test_enrich_nodes_with_municipality_metadata_supports_6_digit_codes(self) -> None:
        nodes = pd.DataFrame(
            [
                {
                    "node_id": "municipality:355030",
                    "node_type": "municipality",
                    "name": pd.NA,
                    "municipality_code": "355030",
                    "latitude": pd.NA,
                    "longitude": pd.NA,
                    "capacity_beds": pd.NA,
                    "habilitation_level": pd.NA,
                },
                {
                    "node_id": "facility:2077485",
                    "node_type": "facility",
                    "name": pd.NA,
                    "municipality_code": "355030",
                    "latitude": pd.NA,
                    "longitude": pd.NA,
                    "capacity_beds": pd.NA,
                    "habilitation_level": pd.NA,
                },
            ]
        )

        catalog = [
            {
                "id": 3550308,
                "nome": "Sao Paulo",
                "microrregiao": {
                    "mesorregiao": {
                        "UF": {
                            "sigla": "SP",
                        }
                    }
                },
            }
        ]

        centers = {
            "3550308": MunicipalityCenter(
                ibge_id="3550308",
                latitude=-23.55052,
                longitude=-46.63331,
            )
        }

        out, stats = enrich_nodes_with_municipality_metadata(
            nodes,
            catalog,
            municipality_centers=centers,
        )

        self.assertEqual("Sao Paulo - SP", str(out.iloc[0]["name"]))
        self.assertIn("Sao Paulo - SP", str(out.iloc[1]["name"]))
        self.assertAlmostEqual(-23.55052, float(out.iloc[0]["latitude"]), places=5)
        self.assertAlmostEqual(-46.63331, float(out.iloc[0]["longitude"]), places=5)
        self.assertAlmostEqual(-23.55052, float(out.iloc[1]["latitude"]), places=5)
        self.assertAlmostEqual(-46.63331, float(out.iloc[1]["longitude"]), places=5)
        self.assertEqual(1, stats.municipality_nodes_enriched)
        self.assertEqual(1, stats.municipality_nodes_with_coordinates)
        self.assertEqual(1, stats.facility_nodes_with_coordinates)
        self.assertEqual(0, stats.missing_municipality_metadata)

    def test_enrich_nodes_with_facility_reference(self) -> None:
        nodes = pd.DataFrame(
            [
                {
                    "node_id": "facility:2077485",
                    "node_type": "facility",
                    "name": "CNES 2077485",
                    "municipality_code": "355030",
                    "latitude": pd.NA,
                    "longitude": pd.NA,
                    "capacity_beds": pd.NA,
                    "habilitation_level": pd.NA,
                }
            ]
        )

        reference = pd.DataFrame(
            [
                {
                    "cnes": "2077485",
                    "name": "Hospital Exemplo",
                    "latitude": "-23.5505",
                    "longitude": "-46.6333",
                    "capacity_beds": "250",
                    "habilitation_level": "alta_complexidade",
                }
            ]
        )

        out, updated_rows = enrich_nodes_with_facility_reference(
            nodes,
            reference,
            facility_id_column="cnes",
        )

        self.assertEqual("Hospital Exemplo", str(out.iloc[0]["name"]))
        self.assertEqual("-23.5505", str(out.iloc[0]["latitude"]))
        self.assertEqual("250", str(out.iloc[0]["capacity_beds"]))
        self.assertGreater(updated_rows, 0)

    def test_extract_catalog_municipality_codes(self) -> None:
        catalog = [
            {"id": 3550308},
            {"id": 3304557},
            {"id": "1302603"},
        ]

        self.assertEqual(
            ["130260", "330455", "355030"],
            extract_catalog_municipality_codes(catalog, output_digits=6),
        )
        self.assertEqual(
            ["1302603", "3304557", "3550308"],
            extract_catalog_municipality_codes(catalog, output_digits=7),
        )

    def test_load_or_refresh_cnes_facility_reference_uses_cache(self) -> None:
        fake_records = {
            "0008028": {
                "cnes": "0008028",
                "name": "Hospital Municipal Antonio Giglio",
                "municipality_code": "353440",
                "latitude": -23.53232235,
                "longitude": -46.77712772,
            },
            "0008036": {
                "cnes": "0008036",
                "name": "Unidade Exemplo",
                "municipality_code": "353440",
                "latitude": -23.5301,
                "longitude": -46.7799,
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = Path(tmpdir) / "cnes_cache.json"

            with patch(
                "data_layer.node_enrichment.fetch_cnes_facility_reference",
                side_effect=lambda cnes_code, **_: fake_records.get(cnes_code),
            ) as mocked_fetch:
                first = load_or_refresh_cnes_facility_reference(
                    cache_path,
                    ["0008028", "0008036"],
                )
                self.assertEqual(2, len(first))
                self.assertEqual(2, mocked_fetch.call_count)

            with patch(
                "data_layer.node_enrichment.fetch_cnes_facility_reference",
                side_effect=AssertionError("cache should satisfy request without new fetch"),
            ):
                second = load_or_refresh_cnes_facility_reference(
                    cache_path,
                    ["0008028", "0008036"],
                )
                self.assertEqual(2, len(second))

            row = first[first["cnes"] == "0008028"].iloc[0]
            self.assertEqual("Hospital Municipal Antonio Giglio", str(row["name"]))
            self.assertEqual("353440", str(row["municipality_code"]))
            self.assertAlmostEqual(-23.53232235, float(row["latitude"]), places=6)


if __name__ == "__main__":
    unittest.main()
