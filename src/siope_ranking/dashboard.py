from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

DEFAULT_DATASET = Path("data/processed/university_kpis.csv")


def load_data(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame["year"] = frame["year"].astype(int)
    return frame


def ranking_view(frame: pd.DataFrame) -> None:
    st.subheader("1) Ranking interattivo")
    years = sorted(frame["year"].unique())
    selected_year = st.selectbox("Anno", options=years, index=len(years) - 1)

    metrics = {
        "Score composito": "composite_score",
        "Spesa per studente (min meglio)": "expense_per_student",
        "Incasso per studente": "income_per_student",
        "Saldo netto": "net_balance",
    }
    selected_metric_label = st.selectbox("Criterio ranking", options=list(metrics.keys()))
    selected_metric = metrics[selected_metric_label]

    year_data = frame[frame["year"] == selected_year].copy()
    ascending = selected_metric == "expense_per_student"
    year_data = year_data.sort_values(selected_metric, ascending=ascending)
    year_data["rank"] = range(1, len(year_data) + 1)

    top_n = st.slider("Mostra top N", 5, min(30, len(year_data)), 10)
    shown = year_data.head(top_n)

    fig = px.bar(
        shown,
        x="rank",
        y=selected_metric,
        hover_name="university",
        color="region",
        title=f"Ranking {selected_year} - {selected_metric_label}",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        shown[
            [
                "rank",
                "university",
                "region",
                "students",
                "faculty",
                "PAGAMENTO",
                "INCASSO",
                "expense_per_student",
                "income_per_student",
                "composite_score",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )


def trend_view(frame: pd.DataFrame) -> None:
    st.subheader("2) Trend annuale per ateneo")
    university = st.selectbox("Ateneo", options=sorted(frame["university"].dropna().unique()))
    metric = st.selectbox(
        "Misura",
        options=["PAGAMENTO", "INCASSO", "expense_per_student", "income_per_student", "net_balance"],
    )

    uni_data = frame[frame["university"] == university].sort_values("year")
    fig = px.line(
        uni_data,
        x="year",
        y=metric,
        markers=True,
        title=f"{university} - andamento di {metric}",
    )
    st.plotly_chart(fig, use_container_width=True)


def quality_notes() -> None:
    st.subheader("3) Professionalità e normalizzazione")
    st.markdown(
        """
        - I valori economici sono normalizzati su studenti e corpo docente.
        - Sono aggiunti metadati geografici (`regione`, `città`, `lat`, `lon`).
        - È disponibile un indicatore composito per confronti multi-criterio.
        """
    )


def main() -> None:
    st.set_page_config(page_title="SIOPE Ranking Università", layout="wide")
    st.title("SIOPE Ranking - Università Italiane")
    st.caption("Dashboard interattiva per ranking e trend annuali")

    data_path = st.sidebar.text_input("Dataset path", value=str(DEFAULT_DATASET))
    if not Path(data_path).exists():
        st.error(
            "Dataset non trovato. Esegui prima la pipeline: "
            "`python -m siope_ranking.data_pipeline --start-year 2019 --end-year 2024`"
        )
        st.stop()

    frame = load_data(Path(data_path))
    ranking_view(frame)
    trend_view(frame)
    quality_notes()


if __name__ == "__main__":
    main()
