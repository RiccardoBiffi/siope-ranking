from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from siope_ranking.tabular_import import (
    DEFAULT_IMPORTED_DIR,
    dataframe_to_csv_bytes,
    list_excel_sheets,
    load_uploaded_table,
    write_csv,
)

DEFAULT_DATASET = Path("data/processed/university_kpis.csv")
DEFAULT_BREAKDOWN_DATASET = Path("data/processed/university_category_breakdown.csv")

LEGACY_KPI_COLUMNS = {
    "year",
    "university",
    "region",
    "students",
    "faculty",
    "PAGAMENTO",
    "INCASSO",
    "expense_per_student",
    "income_per_student",
    "net_balance",
    "composite_score",
}

WORKBOOK_KPI_COLUMNS = {
    "year",
    "university",
    "region",
    "city",
    "students",
    "faculty",
    "total_receipts",
    "total_payments",
    "net_balance",
    "receipts_to_payments_ratio",
    "receipts_per_student",
    "payments_per_student",
    "net_balance_per_student",
    "receipts_per_faculty",
    "payments_per_faculty",
    "student_faculty_ratio",
    "financial_strength_score",
}


def load_data(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def load_breakdown_data(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
    frame = frame.dropna(subset=["year"])
    frame["year"] = frame["year"].astype(int)
    return frame


def detect_schema(frame: pd.DataFrame) -> str | None:
    if WORKBOOK_KPI_COLUMNS.issubset(frame.columns):
        return "workbook"
    if LEGACY_KPI_COLUMNS.issubset(frame.columns):
        return "legacy"
    return None


def prepare_frame(frame: pd.DataFrame, schema: str) -> pd.DataFrame:
    prepared = frame.copy()
    prepared["year"] = pd.to_numeric(prepared["year"], errors="coerce").astype("Int64")
    prepared = prepared.dropna(subset=["year"])
    prepared["year"] = prepared["year"].astype(int)

    numeric_columns = {
        "legacy": [
            "PAGAMENTO",
            "INCASSO",
            "expense_per_student",
            "income_per_student",
            "net_balance",
            "composite_score",
        ],
        "workbook": [
            "students",
            "faculty",
            "total_receipts",
            "total_payments",
            "net_balance",
            "receipts_to_payments_ratio",
            "receipts_per_student",
            "payments_per_student",
            "net_balance_per_student",
            "receipts_per_faculty",
            "payments_per_faculty",
            "student_faculty_ratio",
            "financial_strength_score",
            "personnel_cost_share",
            "goods_services_share",
            "current_expenses_share",
            "capital_expenses_share",
            "service_revenue_share",
            "current_transfer_revenue_share",
        ],
    }
    for column in numeric_columns.get(schema, []):
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")

    return prepared


def import_view(frame: pd.DataFrame, source_name: str) -> None:
    preview_rows = min(len(frame), 500)
    st.subheader("1) Dati caricati")
    st.caption(f"Sorgente attiva: `{source_name}`")
    st.caption(f"Anteprima prime {preview_rows} righe su {len(frame)}.")
    st.dataframe(frame.head(preview_rows), use_container_width=True, hide_index=True)


def ranking_view(frame: pd.DataFrame, schema: str) -> None:
    st.subheader("2) Ranking interattivo")
    years = sorted(frame["year"].unique())
    selected_year = st.selectbox("Anno", options=years, index=len(years) - 1, key=f"{schema}_rank_year")

    if schema == "workbook":
        metrics = {
            "Score di solidita finanziaria": "financial_strength_score",
            "Incassi totali": "total_receipts",
            "Pagamenti totali": "total_payments",
            "Saldo netto": "net_balance",
            "Incassi per studente": "receipts_per_student",
            "Pagamenti per studente": "payments_per_student",
            "Saldo per studente": "net_balance_per_student",
            "Incassi/Pagamenti": "receipts_to_payments_ratio",
            "Studenti per docente": "student_faculty_ratio",
            "Quota costo del personale": "personnel_cost_share",
            "Quota beni e servizi": "goods_services_share",
        }
        table_columns = [
            "rank",
            "university",
            "region",
            "students",
            "faculty",
            "total_receipts",
            "total_payments",
            "net_balance",
            "receipts_per_student",
            "payments_per_student",
            "receipts_to_payments_ratio",
            "financial_strength_score",
        ]
    else:
        metrics = {
            "Score composito": "composite_score",
            "Spesa per studente": "expense_per_student",
            "Incasso per studente": "income_per_student",
            "Saldo netto": "net_balance",
        }
        table_columns = [
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

    selected_metric_label = st.selectbox(
        "Criterio ranking",
        options=list(metrics.keys()),
        key=f"{schema}_rank_metric",
    )
    selected_metric = metrics[selected_metric_label]

    year_data = frame[frame["year"] == selected_year].copy()
    year_data = year_data.sort_values(selected_metric, ascending=False)
    year_data["rank"] = range(1, len(year_data) + 1)

    top_n = st.slider(
        "Mostra top N",
        min_value=1,
        max_value=min(30, len(year_data)),
        value=min(10, len(year_data)),
        key=f"{schema}_top_n",
    )
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
    st.dataframe(shown[table_columns], use_container_width=True, hide_index=True)


def trend_view(frame: pd.DataFrame, schema: str) -> None:
    st.subheader("3) Trend annuale per ateneo")
    university = st.selectbox(
        "Ateneo",
        options=sorted(frame["university"].dropna().unique()),
        key=f"{schema}_trend_university",
    )

    if schema == "workbook":
        metrics = [
            "financial_strength_score",
            "total_receipts",
            "total_payments",
            "net_balance",
            "receipts_per_student",
            "payments_per_student",
            "net_balance_per_student",
            "receipts_to_payments_ratio",
            "student_faculty_ratio",
        ]
    else:
        metrics = ["PAGAMENTO", "INCASSO", "expense_per_student", "income_per_student", "net_balance"]

    metric = st.selectbox("Misura", options=metrics, key=f"{schema}_trend_metric")
    uni_data = frame[frame["university"] == university].sort_values("year")
    fig = px.line(
        uni_data,
        x="year",
        y=metric,
        markers=True,
        title=f"{university} - andamento di {metric}",
    )
    st.plotly_chart(fig, use_container_width=True)


def composition_view(breakdown_frame: pd.DataFrame) -> None:
    st.subheader("4) Composizione per categoria")
    if breakdown_frame.empty:
        st.info("Dataset breakdown non disponibile. Esegui prima la pipeline workbook per abilitarlo.")
        return

    available_years = sorted(breakdown_frame["year"].unique())
    selected_year = st.selectbox("Anno composizione", options=available_years, key="breakdown_year")
    available_universities = sorted(
        breakdown_frame.loc[breakdown_frame["year"] == selected_year, "university"].dropna().unique()
    )
    selected_university = st.selectbox(
        "Ateneo composizione",
        options=available_universities,
        key="breakdown_university",
    )
    flow_type = st.selectbox("Flusso", options=["INCASSI", "PAGAMENTI"], key="breakdown_flow")
    taxonomy_level = st.selectbox(
        "Livello",
        options=["general_category", "macro_category", "category"],
        format_func=lambda value: {
            "general_category": "Categoria generale",
            "macro_category": "Macrocategoria",
            "category": "Categoria",
        }[value],
        key="breakdown_level",
    )

    filtered = breakdown_frame[
        (breakdown_frame["year"] == selected_year)
        & (breakdown_frame["university"] == selected_university)
        & (breakdown_frame["flow_type"] == flow_type)
        & (breakdown_frame["taxonomy_level"] == taxonomy_level)
    ].copy()
    filtered = filtered.sort_values("amount", ascending=False)

    if filtered.empty:
        st.info("Nessun dato disponibile per la combinazione selezionata.")
        return

    top_n = st.slider(
        "Mostra prime categorie",
        min_value=1,
        max_value=min(20, len(filtered)),
        value=min(10, len(filtered)),
        key="breakdown_top_n",
    )
    shown = filtered.head(top_n)

    fig = px.bar(
        shown,
        x="amount",
        y="category_name",
        orientation="h",
        color="share_of_flow",
        color_continuous_scale="Blues",
        title=f"{selected_university} - {flow_type.lower()} {selected_year}",
        labels={"share_of_flow": "Quota sul flusso"},
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(
        shown[["category_name", "amount", "flow_total", "share_of_flow"]],
        use_container_width=True,
        hide_index=True,
    )


def quality_notes(schema: str) -> None:
    if schema == "workbook":
        st.subheader("5) KPI preprocessati")
        st.markdown(
            """
            - I KPI derivano dal workbook nazionale con iscritti, docenti e bilanci 2019-2023.
            - Gli importi sono aggregati per ateneo/anno e normalizzati su studenti e docenti.
            - La dashboard include un breakdown per categoria generale, macrocategoria e categoria.
            """
        )
        return

    st.subheader("4) KPI preprocessati")
    st.markdown(
        """
        - I valori economici sono normalizzati su studenti e corpo docente.
        - Sono aggiunti metadati geografici (`regione`, `citta`, `lat`, `lon`).
        - E disponibile un indicatore composito per confronti multi-criterio.
        """
    )


def import_controls() -> tuple[pd.DataFrame | None, str | None]:
    st.sidebar.subheader("Importa Excel o CSV")
    uploaded_file = st.sidebar.file_uploader(
        "Carica un file",
        type=["csv", "xls", "xlsx", "xlsm"],
        help="Il file viene pulito e convertito in CSV. Se contiene gia il dataset KPI preprocessato, la dashboard lo usa direttamente.",
    )
    if uploaded_file is None:
        return None, None

    file_bytes = uploaded_file.getvalue()
    sheet_name: str | int = 0

    if Path(uploaded_file.name).suffix.lower() in {".xls", ".xlsx", ".xlsm"}:
        try:
            sheet_names = list_excel_sheets(file_bytes)
        except Exception as exc:  # noqa: BLE001
            st.sidebar.error(f"Impossibile leggere il workbook: {exc}")
            return None, None
        sheet_name = st.sidebar.selectbox("Foglio Excel", options=sheet_names)

    try:
        frame = load_uploaded_table(uploaded_file.name, file_bytes, sheet_name=sheet_name)
    except Exception as exc:  # noqa: BLE001
        st.sidebar.error(f"Import fallito: {exc}")
        return None, None

    csv_name = f"{Path(uploaded_file.name).stem}.csv"
    target_path = DEFAULT_IMPORTED_DIR / csv_name
    st.sidebar.caption(f"Importate {len(frame)} righe e {len(frame.columns)} colonne.")
    st.sidebar.download_button(
        "Scarica CSV convertito",
        data=dataframe_to_csv_bytes(frame),
        file_name=csv_name,
        mime="text/csv",
        use_container_width=True,
    )

    if st.sidebar.button("Salva CSV in data/imported", use_container_width=True):
        saved_path = write_csv(frame, target_path)
        st.sidebar.success(f"File salvato in {saved_path}")

    return frame, uploaded_file.name


def main() -> None:
    st.set_page_config(page_title="SIOPE Ranking Universita", layout="wide")
    st.title("SIOPE Ranking - Universita Italiane")
    st.caption("Dashboard per KPI finanziari, iscritti, docenti e composizione dei bilanci")

    imported_frame, imported_name = import_controls()
    st.sidebar.divider()

    breakdown_frame = pd.DataFrame()
    if imported_frame is not None:
        frame = imported_frame
        source_name = imported_name or "upload"
    else:
        data_path = st.sidebar.text_input("Dataset KPI", value=str(DEFAULT_DATASET))
        breakdown_path = st.sidebar.text_input("Dataset breakdown", value=str(DEFAULT_BREAKDOWN_DATASET))
        if not Path(data_path).exists():
            st.error(
                "Dataset KPI non trovato. Esegui prima la pipeline workbook: "
                "`python -m siope_ranking.data_pipeline --source workbook`"
            )
            st.stop()

        frame = load_data(Path(data_path))
        source_name = data_path
        if Path(breakdown_path).exists():
            breakdown_frame = load_breakdown_data(Path(breakdown_path))

    import_view(frame, source_name)
    schema = detect_schema(frame)
    if schema is None:
        st.info(
            "Il file e stato caricato correttamente, ma non corrisponde a uno schema KPI supportato. "
            "La tabella sopra mostra comunque i dati importati."
        )
        st.stop()

    prepared = prepare_frame(frame, schema)
    ranking_view(prepared, schema)
    trend_view(prepared, schema)
    if schema == "workbook":
        composition_view(breakdown_frame)
    quality_notes(schema)


if __name__ == "__main__":
    main()
