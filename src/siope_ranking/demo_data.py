from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def build_demo_dataset(registry_path: Path, output_path: Path, start_year: int = 2019, end_year: int = 2024) -> None:
    rng = np.random.default_rng(42)
    registry = pd.read_csv(registry_path)
    rows = []

    for _, r in registry.iterrows():
        students = max(int(r["students"]), 1)
        faculty = max(int(r["faculty"]), 1)
        base_expense = students * rng.uniform(6000, 11000)
        for year in range(start_year, end_year + 1):
            yearly_factor = 1 + (year - start_year) * rng.uniform(0.01, 0.03)
            expense = base_expense * yearly_factor * rng.uniform(0.95, 1.1)
            income = expense * rng.uniform(0.98, 1.08)
            rows.append(
                {
                    "siope_code": str(r["siope_code"]),
                    "year": year,
                    "PAGAMENTO": round(expense, 2),
                    "INCASSO": round(income, 2),
                    "university": r["university"],
                    "city": r["city"],
                    "region": r["region"],
                    "lat": r["lat"],
                    "lon": r["lon"],
                    "students": students,
                    "faculty": faculty,
                }
            )

    frame = pd.DataFrame(rows)
    frame["net_balance"] = frame["INCASSO"] - frame["PAGAMENTO"]
    frame["expense_per_student"] = frame["PAGAMENTO"] / frame["students"]
    frame["expense_per_faculty"] = frame["PAGAMENTO"] / frame["faculty"]
    frame["income_per_student"] = frame["INCASSO"] / frame["students"]

    for col in ["expense_per_student", "income_per_student", "net_balance"]:
        frame[f"{col}_score"] = (frame[col] - frame[col].min()) / (frame[col].max() - frame[col].min()) * 100

    frame["composite_score"] = (
        0.4 * frame["income_per_student_score"] + 0.4 * (100 - frame["expense_per_student_score"]) + 0.2 * frame["net_balance_score"]
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)


if __name__ == "__main__":
    build_demo_dataset(Path("config/universities.csv"), Path("data/processed/university_kpis.csv"))
