from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import requests


@dataclass
class SiopeConfig:
    base_url: str = "https://www.siope.it/SiopeServerWS"
    timeout: int = 30


class SiopeClient:
    """Client minimale per recuperare transazioni annuali da SIOPE.

    Nota: endpoint/parametri possono variare nel tempo; la funzione usa un endpoint CSV
    configurabile via --base-url e ritorna errori espliciti in caso di risposta non valida.
    """

    def __init__(self, config: SiopeConfig | None = None) -> None:
        self.config = config or SiopeConfig()

    def fetch_university_year(self, siope_code: str, year: int) -> pd.DataFrame:
        endpoint = f"{self.config.base_url}/export/transazioni"
        params = {"ente": siope_code, "anno": year, "formato": "csv"}
        response = requests.get(endpoint, params=params, timeout=self.config.timeout)
        response.raise_for_status()

        frame = pd.read_csv(pd.io.common.StringIO(response.text), sep=';')
        required = {"tipo_operazione", "importo"}
        if not required.issubset(frame.columns):
            raise ValueError(
                f"Formato inatteso per ente={siope_code} anno={year}. Colonne: {list(frame.columns)}"
            )
        frame["siope_code"] = siope_code
        frame["year"] = year
        return frame


def compute_kpis(raw: pd.DataFrame, registry: pd.DataFrame) -> pd.DataFrame:
    raw = raw.copy()
    raw["importo"] = (
        raw["importo"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False
        )
    )
    raw["importo"] = pd.to_numeric(raw["importo"], errors="coerce")
    raw = raw.dropna(subset=["importo"])

    grouped = (
        raw.groupby(["siope_code", "year", "tipo_operazione"], as_index=False)["importo"]
        .sum()
        .pivot_table(
            index=["siope_code", "year"],
            columns="tipo_operazione",
            values="importo",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )
    grouped.columns.name = None

    if "INCASSO" not in grouped.columns:
        grouped["INCASSO"] = 0
    if "PAGAMENTO" not in grouped.columns:
        grouped["PAGAMENTO"] = 0

    merged = grouped.merge(registry, on="siope_code", how="left")
    merged["net_balance"] = merged["INCASSO"] - merged["PAGAMENTO"]
    merged["expense_per_student"] = merged["PAGAMENTO"] / merged["students"].replace(0, np.nan)
    merged["expense_per_faculty"] = merged["PAGAMENTO"] / merged["faculty"].replace(0, np.nan)
    merged["income_per_student"] = merged["INCASSO"] / merged["students"].replace(0, np.nan)

    # indice sintetico normalizzato 0-100 per ranking multi-criterio
    for column in ["expense_per_student", "income_per_student", "net_balance"]:
        col_min = merged[column].min()
        col_max = merged[column].max()
        if pd.notna(col_min) and pd.notna(col_max) and col_max != col_min:
            merged[f"{column}_score"] = (merged[column] - col_min) / (col_max - col_min) * 100
        else:
            merged[f"{column}_score"] = 0

    merged["composite_score"] = (
        0.4 * merged["income_per_student_score"]
        + 0.4 * (100 - merged["expense_per_student_score"])
        + 0.2 * merged["net_balance_score"]
    )

    return merged.sort_values(["year", "composite_score"], ascending=[True, False])


def build_dataset(
    registry_path: Path, output_path: Path, years: Iterable[int], base_url: str | None = None
) -> pd.DataFrame:
    registry = pd.read_csv(registry_path)
    registry["siope_code"] = registry["siope_code"].astype(str)

    client = SiopeClient(SiopeConfig(base_url=base_url or SiopeConfig.base_url))
    all_rows: list[pd.DataFrame] = []
    failures: list[str] = []

    for _, row in registry.iterrows():
        for year in years:
            try:
                frame = client.fetch_university_year(str(row["siope_code"]), int(year))
                all_rows.append(frame)
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{row['university']} ({row['siope_code']} - {year}): {exc}")

    if not all_rows:
        raise RuntimeError(
            "Nessun dato scaricato da SIOPE. Verifica endpoint/parametri o usa export locale."
        )

    raw = pd.concat(all_rows, ignore_index=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    kpis = compute_kpis(raw, registry)
    kpis.to_csv(output_path, index=False)

    if failures:
        print("[WARN] Alcuni download sono falliti:")
        for item in failures[:20]:
            print(" -", item)
        if len(failures) > 20:
            print(f" - ... altri {len(failures) - 20} errori")

    return kpis


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build dataset ranking atenei da SIOPE")
    parser.add_argument("--registry", default="config/universities.csv")
    parser.add_argument("--output", default="data/processed/university_kpis.csv")
    parser.add_argument("--start-year", type=int, default=2019)
    parser.add_argument("--end-year", type=int, default=2024)
    parser.add_argument("--base-url", default=SiopeConfig.base_url)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = range(args.start_year, args.end_year + 1)
    build_dataset(
        registry_path=Path(args.registry),
        output_path=Path(args.output),
        years=years,
        base_url=args.base_url,
    )


if __name__ == "__main__":
    main()
