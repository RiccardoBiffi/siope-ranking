from __future__ import annotations

import io
import os
import tempfile
import unittest
import zipfile
from pathlib import Path

import pandas as pd

from siope_ranking.data_pipeline import (
    build_dataset,
    parse_siope_zip_bytes,
    resolve_siope_entities,
    resolve_source,
)


def make_zip_bytes(name: str, content: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, content)
    return buffer.getvalue()


class ParseSiopeZipBytesTests(unittest.TestCase):
    def test_parse_siope_zip_bytes_filters_entities_and_sets_operation(self) -> None:
        zip_bytes = make_zip_bytes(
            "USCITE_2024.csv",
            '"0001","2024","01","1.01.01","100"\n'
            '"0002","2024","02","1.01.02","250"\n',
        )

        frame = parse_siope_zip_bytes(zip_bytes, flow_type="PAGAMENTO", entity_codes={"0002"})

        self.assertEqual(frame["entity_code"].tolist(), ["0002"])
        self.assertEqual(frame["tipo_operazione"].tolist(), ["PAGAMENTO"])
        self.assertEqual(frame["year"].tolist(), [2024])
        self.assertEqual(frame["importo"].tolist(), [2.5])


class ResolveSiopeEntitiesTests(unittest.TestCase):
    def test_resolve_siope_entities_supports_aliases_and_overrides(self) -> None:
        registry = pd.DataFrame(
            [
                {
                    "university": "Università di Bari Aldo Moro",
                    "city": "Bari",
                    "region": "Puglia",
                    "lat": 41.1,
                    "lon": 16.8,
                    "students": 1,
                    "faculty": 1,
                    "siope_code": "080001",
                },
                {
                    "university": "Università di Bologna",
                    "city": "Bologna",
                    "region": "Emilia-Romagna",
                    "lat": 44.4,
                    "lon": 11.3,
                    "students": 1,
                    "faculty": 1,
                    "siope_code": "370001",
                },
                {
                    "university": "Scuola Superiore Sant'Anna",
                    "city": "Pisa",
                    "region": "Toscana",
                    "lat": 43.7,
                    "lon": 10.4,
                    "students": 1,
                    "faculty": 1,
                    "siope_code": "090004",
                },
            ]
        )
        active_entities = pd.DataFrame(
            [
                {"entity_code": "000700261000000", "official_name": "UNIVERSITA' DEGLI STUDI DI BARI ALDO MORO"},
                {"entity_code": "000704380000000", "official_name": "ALMA MATER STUDIORUM UNIVERSITA' DI BOLOGNA"},
                {
                    "entity_code": "016203420000000",
                    "official_name": "SCUOLA SUPERIORE DI STUDI UNIVERSITARI DI PERFEZIONAMENTO S. ANNA",
                },
            ]
        )

        resolved = resolve_siope_entities(registry, active_entities)

        mapping = dict(zip(resolved["university"], resolved["siope_live_code"], strict=True))
        self.assertEqual(mapping["Università di Bari Aldo Moro"], "000700261000000")
        self.assertEqual(mapping["Università di Bologna"], "000704380000000")
        self.assertEqual(mapping["Scuola Superiore Sant'Anna"], "016203420000000")

    def test_resolve_siope_entities_raises_for_unmatched_university(self) -> None:
        registry = pd.DataFrame(
            [
                {
                    "university": "Università Inventata",
                    "city": "N.A.",
                    "region": "N.A.",
                    "lat": 0.0,
                    "lon": 0.0,
                    "students": 1,
                    "faculty": 1,
                    "siope_code": "999999",
                }
            ]
        )
        active_entities = pd.DataFrame(
            [{"entity_code": "000700261000000", "official_name": "UNIVERSITA' DEGLI STUDI DI BARI ALDO MORO"}]
        )

        with self.assertRaisesRegex(RuntimeError, "Università Inventata"):
            resolve_siope_entities(registry, active_entities)


class ResolveSourceTests(unittest.TestCase):
    def test_resolve_source_prefers_workbook_without_years(self) -> None:
        args = type("Args", (), {"source": "auto", "start_year": None, "end_year": None})()
        self.assertEqual(resolve_source(args), "workbook")

    def test_resolve_source_switches_to_siope_when_years_are_provided(self) -> None:
        args = type("Args", (), {"source": "auto", "start_year": 2019, "end_year": 2024})()
        self.assertEqual(resolve_source(args), "siope")


class LiveSiopeIntegrationTests(unittest.TestCase):
    @unittest.skipUnless(os.getenv("SIOPE_LIVE_TESTS") == "1", "Live SIOPE test disabled")
    def test_build_dataset_downloads_real_siope_data(self) -> None:
        registry = pd.DataFrame(
            [
                {
                    "university": "Università di Bari Aldo Moro",
                    "city": "Bari",
                    "region": "Puglia",
                    "lat": 41.1171,
                    "lon": 16.8719,
                    "students": 43000,
                    "faculty": 1900,
                    "siope_code": "080001",
                },
                {
                    "university": "Politecnico di Bari",
                    "city": "Bari",
                    "region": "Puglia",
                    "lat": 41.1171,
                    "lon": 16.8719,
                    "students": 12000,
                    "faculty": 650,
                    "siope_code": "080002",
                },
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            registry_path = Path(temp_dir) / "registry.csv"
            output_path = Path(temp_dir) / "university_kpis.csv"
            registry.to_csv(registry_path, index=False)

            frame = build_dataset(registry_path=registry_path, output_path=output_path, years=[2024])

            self.assertTrue(output_path.exists())
            self.assertFalse(frame.empty)
            self.assertEqual(sorted(frame["year"].unique().tolist()), [2024])
            self.assertIn("composite_score", frame.columns)


if __name__ == "__main__":
    unittest.main()
