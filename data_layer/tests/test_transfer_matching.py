import unittest

import pandas as pd

from data_layer.transfer_matching import (
    TransferMatchConfig,
    aggregate_transfer_edges,
    build_probabilistic_patient_key,
    infer_transfer_events,
)


def base_config() -> TransferMatchConfig:
    return TransferMatchConfig(
        patient_key_col="patient_id",
        transfer_flag_col="PA_TRANSF",
        discharge_reason_col="PA_MOTSAI",
        sex_col="PA_SEXO",
        age_col="PA_IDADE",
        icd_col="PA_CIDPRI",
        origin_facility_col="PA_CODUNI",
        admission_datetime_col="DT_INTER",
        discharge_datetime_col="DT_SAIDA",
        min_window_hours=24,
        max_window_hours=48,
    )


class TransferMatchingTests(unittest.TestCase):
    def test_build_probabilistic_key_ignores_episode_identifiers(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "NASC": "1980-01-02",
                    "SEXO": "f",
                    "IDADE": "45",
                    "MUNIC_RES": "3550308",
                    "N_AIH": "123",
                    "NUM_PROC": "ABC",
                },
                {
                    "NASC": "1981-03-04",
                    "SEXO": "M",
                    "IDADE": "46",
                    "MUNIC_RES": "3552205",
                    "N_AIH": "456",
                    "NUM_PROC": "DEF",
                },
            ]
        )

        keys, metadata = build_probabilistic_patient_key(df, sex_col="SEXO", age_col="IDADE")

        self.assertEqual("NASC", metadata["birthdate_field"])
        self.assertEqual("MUNIC_RES", metadata["residence_field"])
        self.assertEqual("1980-01-02|F|45|3550308", keys.iloc[0])
        self.assertEqual("1981-03-04|M|46|3552205", keys.iloc[1])

    def test_build_probabilistic_key_requires_birthdate_and_residence(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "SEXO": "F",
                    "IDADE": "30",
                }
            ]
        )

        keys, metadata = build_probabilistic_patient_key(df, sex_col="SEXO", age_col="IDADE")

        self.assertIsNone(metadata["birthdate_field"])
        self.assertIsNone(metadata["residence_field"])
        self.assertEqual("", keys.iloc[0])

    def test_happy_path_transfer_match(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "patient_id": "p1",
                    "PA_TRANSF": "1",
                    "PA_MOTSAI": "00",
                    "PA_SEXO": "F",
                    "PA_IDADE": "55",
                    "PA_CIDPRI": "I500",
                    "PA_CODUNI": "HOSP_A",
                    "DT_INTER": "2026-01-01 00:00:00",
                    "DT_SAIDA": "2026-01-02 06:00:00",
                },
                {
                    "patient_id": "p1",
                    "PA_TRANSF": "0",
                    "PA_MOTSAI": "00",
                    "PA_SEXO": "F",
                    "PA_IDADE": "55",
                    "PA_CIDPRI": "I509",
                    "PA_CODUNI": "HOSP_B",
                    "DT_INTER": "2026-01-03 08:00:00",
                    "DT_SAIDA": "2026-01-04 05:00:00",
                },
            ]
        )

        events, rejections = infer_transfer_events(df, {"01"}, base_config())

        self.assertEqual(1, len(events))
        self.assertEqual("HOSP_A", events.iloc[0]["source_facility_id"])
        self.assertEqual("HOSP_B", events.iloc[0]["target_location_id"])
        self.assertEqual({}, rejections)

        edges = aggregate_transfer_edges(events)
        self.assertEqual(1, len(edges))
        self.assertEqual(1, int(edges.iloc[0]["transfer_count"]))

    def test_rejects_when_time_window_shorter_than_24h(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "patient_id": "p2",
                    "PA_TRANSF": "1",
                    "PA_MOTSAI": "00",
                    "PA_SEXO": "M",
                    "PA_IDADE": "40",
                    "PA_CIDPRI": "J180",
                    "PA_CODUNI": "HOSP_A",
                    "DT_INTER": "2026-01-01 00:00:00",
                    "DT_SAIDA": "2026-01-01 10:00:00",
                },
                {
                    "patient_id": "p2",
                    "PA_TRANSF": "0",
                    "PA_MOTSAI": "00",
                    "PA_SEXO": "M",
                    "PA_IDADE": "40",
                    "PA_CIDPRI": "J181",
                    "PA_CODUNI": "HOSP_B",
                    "DT_INTER": "2026-01-02 06:00:00",
                    "DT_SAIDA": "2026-01-03 05:00:00",
                },
            ]
        )

        events, rejections = infer_transfer_events(df, set(), base_config())

        self.assertEqual(0, len(events))
        self.assertEqual(1, rejections.get("no_candidate_in_time_window", 0))

    def test_rejects_demographic_mismatch(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "patient_id": "p3",
                    "PA_TRANSF": "1",
                    "PA_MOTSAI": "00",
                    "PA_SEXO": "F",
                    "PA_IDADE": "30",
                    "PA_CIDPRI": "N390",
                    "PA_CODUNI": "HOSP_A",
                    "DT_INTER": "2026-01-01 00:00:00",
                    "DT_SAIDA": "2026-01-02 06:00:00",
                },
                {
                    "patient_id": "p3",
                    "PA_TRANSF": "0",
                    "PA_MOTSAI": "00",
                    "PA_SEXO": "M",
                    "PA_IDADE": "30",
                    "PA_CIDPRI": "N391",
                    "PA_CODUNI": "HOSP_B",
                    "DT_INTER": "2026-01-03 08:00:00",
                    "DT_SAIDA": "2026-01-04 05:00:00",
                },
            ]
        )

        events, rejections = infer_transfer_events(df, set(), base_config())

        self.assertEqual(0, len(events))
        self.assertEqual(1, rejections.get("demographic_mismatch", 0))

    def test_rejects_clinical_discontinuity(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "patient_id": "p4",
                    "PA_TRANSF": "1",
                    "PA_MOTSAI": "00",
                    "PA_SEXO": "F",
                    "PA_IDADE": "70",
                    "PA_CIDPRI": "I500",
                    "PA_CODUNI": "HOSP_A",
                    "DT_INTER": "2026-01-01 00:00:00",
                    "DT_SAIDA": "2026-01-02 06:00:00",
                },
                {
                    "patient_id": "p4",
                    "PA_TRANSF": "0",
                    "PA_MOTSAI": "00",
                    "PA_SEXO": "F",
                    "PA_IDADE": "70",
                    "PA_CIDPRI": "O470",
                    "PA_CODUNI": "HOSP_B",
                    "DT_INTER": "2026-01-03 08:00:00",
                    "DT_SAIDA": "2026-01-04 05:00:00",
                },
            ]
        )

        events, rejections = infer_transfer_events(df, set(), base_config())

        self.assertEqual(0, len(events))
        self.assertEqual(1, rejections.get("clinical_discontinuity", 0))

    def test_tie_break_prefers_exact_icd_then_lexical_target(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "patient_id": "p5",
                    "PA_TRANSF": "0",
                    "PA_MOTSAI": "TR",
                    "PA_SEXO": "F",
                    "PA_IDADE": "50",
                    "PA_CIDPRI": "I500",
                    "PA_CODUNI": "HOSP_A",
                    "DT_INTER": "2026-01-01 00:00:00",
                    "DT_SAIDA": "2026-01-02 06:00:00",
                },
                {
                    "patient_id": "p5",
                    "PA_TRANSF": "0",
                    "PA_MOTSAI": "00",
                    "PA_SEXO": "F",
                    "PA_IDADE": "50",
                    "PA_CIDPRI": "I500",
                    "PA_CODUNI": "HOSP_C",
                    "DT_INTER": "2026-01-03 08:00:00",
                    "DT_SAIDA": "2026-01-04 05:00:00",
                },
                {
                    "patient_id": "p5",
                    "PA_TRANSF": "0",
                    "PA_MOTSAI": "00",
                    "PA_SEXO": "F",
                    "PA_IDADE": "50",
                    "PA_CIDPRI": "I509",
                    "PA_CODUNI": "HOSP_B",
                    "DT_INTER": "2026-01-03 08:00:00",
                    "DT_SAIDA": "2026-01-04 05:00:00",
                },
            ]
        )

        events, _ = infer_transfer_events(df, {"TR"}, base_config())

        self.assertEqual(1, len(events))
        self.assertEqual("HOSP_C", events.iloc[0]["target_location_id"])


if __name__ == "__main__":
    unittest.main()