from __future__ import annotations

import argparse
import re
import unicodedata
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import requests

DEFAULT_WORKBOOK_PATH = Path("data/processed/Bilanci, iscritti e docenti - nazionali.xlsx")
DEFAULT_KPI_OUTPUT_PATH = Path("data/processed/university_kpis.csv")
DEFAULT_BREAKDOWN_OUTPUT_PATH = Path("data/processed/university_category_breakdown.csv")


@dataclass
class SiopeConfig:
    base_url: str = "https://www.siope.it/SiopeServerWS"
    timeout: int = 30


class SiopeClient:
    """Client minimale per recuperare transazioni annuali da SIOPE."""

    def __init__(self, config: SiopeConfig | None = None) -> None:
        self.config = config or SiopeConfig()

    def fetch_university_year(self, siope_code: str, year: int) -> pd.DataFrame:
        endpoint = f"{self.config.base_url}/export/transazioni"
        params = {"ente": siope_code, "anno": year, "formato": "csv"}
        response = requests.get(endpoint, params=params, timeout=self.config.timeout)
        response.raise_for_status()

        frame = pd.read_csv(StringIO(response.text), sep=";")
        required = {"tipo_operazione", "importo"}
        if not required.issubset(frame.columns):
            raise ValueError(
                f"Formato inatteso per ente={siope_code} anno={year}. Colonne: {list(frame.columns)}"
            )
        frame["siope_code"] = siope_code
        frame["year"] = year
        return frame


def normalize_name(value: object) -> str:
    if value is None or pd.isna(value):
        return ""

    text = unicodedata.normalize("NFKC", str(value))
    text = text.replace("’", "'").replace("`", "'")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def strip_classification_code(value: object) -> str:
    text = normalize_name(value)
    return re.sub(r"^\d+(?:\.\d+)+\s*", "", text)


def normalize_flow_type(value: object) -> str:
    text = normalize_name(value).upper()
    if text.startswith("INCASS"):
        return "INCASSI"
    if text.startswith("PAGAMENT"):
        return "PAGAMENTI"
    return text


def melt_population_sheet(workbook_path: Path, sheet_name: str, value_name: str) -> pd.DataFrame:
    frame = pd.read_excel(workbook_path, sheet_name=sheet_name)
    frame = frame.rename(columns={frame.columns[0]: "university"})
    melted = frame.melt(id_vars="university", var_name="year", value_name=value_name)
    melted["university_key"] = melted["university"].map(normalize_name)
    melted["year"] = pd.to_numeric(melted["year"], errors="coerce").astype("Int64")
    melted[value_name] = pd.to_numeric(melted[value_name], errors="coerce")
    melted = melted.dropna(subset=["year", value_name])
    melted["year"] = melted["year"].astype(int)
    return melted[["university_key", "year", value_name]]


def load_registry(workbook_path: Path) -> pd.DataFrame:
    registry = pd.read_excel(workbook_path, sheet_name="Atenei").rename(
        columns={
            "Nome università": "university",
            "Regione": "region",
            "Città": "city",
            "Latitudine": "lat",
            "Longitudine": "lon",
        }
    )
    registry["university"] = registry["university"].map(normalize_name)
    registry["university_key"] = registry["university"].map(normalize_name)
    registry["lat"] = pd.to_numeric(registry["lat"], errors="coerce")
    registry["lon"] = pd.to_numeric(registry["lon"], errors="coerce")
    return registry[["university_key", "university", "region", "city", "lat", "lon"]]


def load_workbook_balance(workbook_path: Path) -> pd.DataFrame:
    balance = pd.read_excel(workbook_path, sheet_name="Bilancio atenei").rename(
        columns={
            "Ateneo": "university",
            "Anno": "year",
            "Incassi/Pagamenti": "flow_type",
            "Categoria generale": "general_category",
            "Macrocategoria": "macro_category",
            "Categoria": "category",
            "Codice tipologia": "type_code",
            "Descrizione tipologia": "type_description",
            "Importo nel periodo": "amount",
            "Importo a tutto il periodo": "cumulative_amount",
        }
    )
    balance["university_key"] = balance["university"].map(normalize_name)
    balance["year"] = pd.to_numeric(balance["year"], errors="coerce").astype("Int64")
    balance["flow_type"] = balance["flow_type"].map(normalize_flow_type)
    balance["amount"] = pd.to_numeric(balance["amount"], errors="coerce").fillna(0)
    balance["cumulative_amount"] = pd.to_numeric(balance["cumulative_amount"], errors="coerce").fillna(0)
    for column in ["general_category", "macro_category", "category", "type_description"]:
        balance[column] = balance[column].map(strip_classification_code)
    balance["type_code"] = balance["type_code"].map(normalize_name)
    balance = balance.dropna(subset=["year"])
    balance["year"] = balance["year"].astype(int)
    return balance


def compute_kpis(raw: pd.DataFrame, registry: pd.DataFrame) -> pd.DataFrame:
    raw = raw.copy()
    raw["importo"] = (
        raw["importo"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
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


def merge_metric_bucket(
    frame: pd.DataFrame,
    balance: pd.DataFrame,
    output_column: str,
    flow_type: str,
    source_column: str,
    labels: set[str],
) -> pd.DataFrame:
    bucket = (
        balance.loc[
            (balance["flow_type"] == flow_type) & balance[source_column].isin(labels),
            ["university_key", "year", "amount"],
        ]
        .groupby(["university_key", "year"], as_index=False)["amount"]
        .sum()
        .rename(columns={"amount": output_column})
    )
    return frame.merge(bucket, on=["university_key", "year"], how="left")


def add_financial_scores(frame: pd.DataFrame) -> pd.DataFrame:
    scored = frame.copy()
    metric_weights = {
        "receipts_per_student": 0.4,
        "net_balance_per_student": 0.3,
        "receipts_to_payments_ratio": 0.3,
    }

    for metric in metric_weights:
        scored[f"{metric}_score"] = 0.0
        for year, group in scored.groupby("year"):
            min_value = group[metric].min()
            max_value = group[metric].max()
            if pd.notna(min_value) and pd.notna(max_value) and max_value != min_value:
                year_scores = (group[metric] - min_value) / (max_value - min_value) * 100
                scored.loc[group.index, f"{metric}_score"] = year_scores

    scored["financial_strength_score"] = 0.0
    for metric, weight in metric_weights.items():
        scored["financial_strength_score"] += scored[f"{metric}_score"] * weight
    scored["composite_score"] = scored["financial_strength_score"]
    return scored


def build_workbook_dataset(
    workbook_path: Path,
    output_path: Path = DEFAULT_KPI_OUTPUT_PATH,
    breakdown_output_path: Path = DEFAULT_BREAKDOWN_OUTPUT_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook non trovato: {workbook_path}")

    registry = load_registry(workbook_path)
    students = melt_population_sheet(workbook_path, "Iscritti", "students")
    faculty = melt_population_sheet(workbook_path, "Docenti", "faculty")
    balance = load_workbook_balance(workbook_path)

    totals = (
        balance.groupby(["university_key", "year", "flow_type"], as_index=False)["amount"]
        .sum()
        .pivot_table(
            index=["university_key", "year"],
            columns="flow_type",
            values="amount",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )
    totals.columns.name = None
    totals = totals.rename(columns={"INCASSI": "total_receipts", "PAGAMENTI": "total_payments"})
    for column in ["total_receipts", "total_payments"]:
        if column not in totals.columns:
            totals[column] = 0.0

    kpis = registry[["university_key", "university", "region", "city", "lat", "lon"]]
    kpis = kpis.merge(students, on="university_key", how="left")
    kpis = kpis.merge(faculty, on=["university_key", "year"], how="left")
    kpis = kpis.merge(totals, on=["university_key", "year"], how="left")

    bucket_definitions = [
        ("current_expenses", "PAGAMENTI", "general_category", {"Spese correnti"}),
        ("capital_expenses", "PAGAMENTI", "general_category", {"Spese in conto capitale"}),
        (
            "third_party_payments",
            "PAGAMENTI",
            "general_category",
            {"Uscite per conto terzi e partite di giro"},
        ),
        (
            "current_receipts",
            "INCASSI",
            "general_category",
            {"Entrate extratributarie", "Trasferimenti correnti"},
        ),
        ("capital_receipts", "INCASSI", "general_category", {"Entrate in conto capitale"}),
        (
            "third_party_receipts",
            "INCASSI",
            "general_category",
            {"Entrate per conto terzi e partite di giro"},
        ),
        ("personnel_costs", "PAGAMENTI", "macro_category", {"Redditi da lavoro dipendente"}),
        ("goods_services_costs", "PAGAMENTI", "macro_category", {"Acquisto di beni e servizi"}),
        ("transfer_costs", "PAGAMENTI", "macro_category", {"Trasferimenti correnti"}),
        (
            "service_revenue",
            "INCASSI",
            "macro_category",
            {"Vendita di beni e servizi e proventi derivanti dalla gestione dei beni"},
        ),
        ("current_transfer_revenue", "INCASSI", "macro_category", {"Trasferimenti correnti"}),
        ("investment_revenue", "INCASSI", "macro_category", {"Contributi agli investimenti"}),
        (
            "investment_expense",
            "PAGAMENTI",
            "macro_category",
            {"Investimenti fissi lordi e acquisto di terreni"},
        ),
    ]

    for output_column, flow_type, source_column, labels in bucket_definitions:
        kpis = merge_metric_bucket(kpis, balance, output_column, flow_type, source_column, labels)

    numeric_columns = [
        "students",
        "faculty",
        "total_receipts",
        "total_payments",
        "current_expenses",
        "capital_expenses",
        "third_party_payments",
        "current_receipts",
        "capital_receipts",
        "third_party_receipts",
        "personnel_costs",
        "goods_services_costs",
        "transfer_costs",
        "service_revenue",
        "current_transfer_revenue",
        "investment_revenue",
        "investment_expense",
    ]
    for column in numeric_columns:
        if column in kpis.columns:
            kpis[column] = pd.to_numeric(kpis[column], errors="coerce")

    fill_zero_columns = [column for column in numeric_columns if column not in {"students", "faculty"}]
    kpis[fill_zero_columns] = kpis[fill_zero_columns].fillna(0)

    kpis["net_balance"] = kpis["total_receipts"] - kpis["total_payments"]
    kpis["student_faculty_ratio"] = kpis["students"] / kpis["faculty"].replace(0, np.nan)
    kpis["receipts_to_payments_ratio"] = kpis["total_receipts"] / kpis["total_payments"].replace(0, np.nan)
    kpis["receipts_per_student"] = kpis["total_receipts"] / kpis["students"].replace(0, np.nan)
    kpis["payments_per_student"] = kpis["total_payments"] / kpis["students"].replace(0, np.nan)
    kpis["net_balance_per_student"] = kpis["net_balance"] / kpis["students"].replace(0, np.nan)
    kpis["receipts_per_faculty"] = kpis["total_receipts"] / kpis["faculty"].replace(0, np.nan)
    kpis["payments_per_faculty"] = kpis["total_payments"] / kpis["faculty"].replace(0, np.nan)
    kpis["net_balance_per_faculty"] = kpis["net_balance"] / kpis["faculty"].replace(0, np.nan)
    kpis["current_expenses_share"] = kpis["current_expenses"] / kpis["total_payments"].replace(0, np.nan)
    kpis["capital_expenses_share"] = kpis["capital_expenses"] / kpis["total_payments"].replace(0, np.nan)
    kpis["personnel_cost_share"] = kpis["personnel_costs"] / kpis["total_payments"].replace(0, np.nan)
    kpis["goods_services_share"] = kpis["goods_services_costs"] / kpis["total_payments"].replace(0, np.nan)
    kpis["capital_receipts_share"] = kpis["capital_receipts"] / kpis["total_receipts"].replace(0, np.nan)
    kpis["current_transfer_revenue_share"] = (
        kpis["current_transfer_revenue"] / kpis["total_receipts"].replace(0, np.nan)
    )
    kpis["service_revenue_share"] = kpis["service_revenue"] / kpis["total_receipts"].replace(0, np.nan)

    kpis = add_financial_scores(kpis)
    kpis = kpis.sort_values(["year", "financial_strength_score"], ascending=[True, False])

    ordered_columns = [
        "university",
        "year",
        "region",
        "city",
        "lat",
        "lon",
        "students",
        "faculty",
        "student_faculty_ratio",
        "total_receipts",
        "total_payments",
        "net_balance",
        "receipts_to_payments_ratio",
        "receipts_per_student",
        "payments_per_student",
        "net_balance_per_student",
        "receipts_per_faculty",
        "payments_per_faculty",
        "net_balance_per_faculty",
        "current_receipts",
        "capital_receipts",
        "third_party_receipts",
        "service_revenue",
        "current_transfer_revenue",
        "investment_revenue",
        "current_expenses",
        "capital_expenses",
        "third_party_payments",
        "personnel_costs",
        "goods_services_costs",
        "transfer_costs",
        "investment_expense",
        "current_expenses_share",
        "capital_expenses_share",
        "personnel_cost_share",
        "goods_services_share",
        "capital_receipts_share",
        "current_transfer_revenue_share",
        "service_revenue_share",
        "receipts_per_student_score",
        "net_balance_per_student_score",
        "receipts_to_payments_ratio_score",
        "financial_strength_score",
        "composite_score",
    ]
    kpis = kpis[ordered_columns]

    breakdown_frames: list[pd.DataFrame] = []
    flow_totals = (
        balance.groupby(["university_key", "year", "flow_type"], as_index=False)["amount"]
        .sum()
        .rename(columns={"amount": "flow_total"})
    )
    registry_subset = registry[["university_key", "university", "region", "city"]]
    for taxonomy_level, category_column in [
        ("general_category", "general_category"),
        ("macro_category", "macro_category"),
        ("category", "category"),
    ]:
        grouped = (
            balance.groupby(["university_key", "year", "flow_type", category_column], as_index=False)["amount"]
            .sum()
            .rename(columns={category_column: "category_name"})
        )
        grouped = grouped[grouped["category_name"] != ""]
        grouped["taxonomy_level"] = taxonomy_level
        grouped = grouped.merge(flow_totals, on=["university_key", "year", "flow_type"], how="left")
        grouped = grouped.merge(registry_subset, on="university_key", how="left")
        grouped["share_of_flow"] = grouped["amount"] / grouped["flow_total"].replace(0, np.nan)
        breakdown_frames.append(grouped)

    breakdown = pd.concat(breakdown_frames, ignore_index=True)
    breakdown = breakdown.sort_values(["year", "university", "flow_type", "taxonomy_level", "amount"], ascending=[True, True, True, True, False])
    breakdown = breakdown[
        [
            "university",
            "year",
            "region",
            "city",
            "flow_type",
            "taxonomy_level",
            "category_name",
            "amount",
            "flow_total",
            "share_of_flow",
        ]
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    breakdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    kpis.to_csv(output_path, index=False)
    breakdown.to_csv(breakdown_output_path, index=False)
    return kpis, breakdown


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build dataset ranking atenei")
    parser.add_argument(
        "--source",
        choices=["workbook", "siope"],
        default="workbook",
        help="Origine dati: workbook Excel preprocessato oppure endpoint SIOPE.",
    )
    parser.add_argument("--workbook", default=str(DEFAULT_WORKBOOK_PATH))
    parser.add_argument("--output", default=str(DEFAULT_KPI_OUTPUT_PATH))
    parser.add_argument("--breakdown-output", default=str(DEFAULT_BREAKDOWN_OUTPUT_PATH))
    parser.add_argument("--registry", default="config/universities.csv")
    parser.add_argument("--start-year", type=int, default=2019)
    parser.add_argument("--end-year", type=int, default=2024)
    parser.add_argument("--base-url", default=SiopeConfig.base_url)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.source == "workbook":
        build_workbook_dataset(
            workbook_path=Path(args.workbook),
            output_path=Path(args.output),
            breakdown_output_path=Path(args.breakdown_output),
        )
        return

    years = range(args.start_year, args.end_year + 1)
    build_dataset(
        registry_path=Path(args.registry),
        output_path=Path(args.output),
        years=years,
        base_url=args.base_url,
    )


if __name__ == "__main__":
    main()
