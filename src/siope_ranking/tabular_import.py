from __future__ import annotations

import argparse
import re
from io import BytesIO
from pathlib import Path
from typing import Iterable

import pandas as pd

DEFAULT_IMPORTED_DIR = Path("data/imported")
SUPPORTED_SPREADSHEET_SUFFIXES = {".csv", ".xls", ".xlsx", ".xlsm"}


def _make_unique_column_names(columns: Iterable[object]) -> list[str]:
    cleaned_columns: list[str] = []
    seen: dict[str, int] = {}

    for index, raw_name in enumerate(columns, start=1):
        name = re.sub(r"\s+", " ", str(raw_name).strip().replace("\n", " ").replace("\r", " "))
        name = name.replace(" ", "_")
        name = re.sub(r"[^\w.-]", "_", name)
        name = re.sub(r"_+", "_", name).strip("_.")
        if not name:
            name = f"column_{index}"

        count = seen.get(name, 0)
        seen[name] = count + 1
        cleaned_columns.append(name if count == 0 else f"{name}_{count + 1}")

    return cleaned_columns


def normalize_tabular_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized = normalized.dropna(axis=0, how="all").dropna(axis=1, how="all")
    normalized.columns = _make_unique_column_names(normalized.columns)

    for column in normalized.columns:
        if pd.api.types.is_object_dtype(normalized[column]) or pd.api.types.is_string_dtype(
            normalized[column]
        ):
            normalized[column] = normalized[column].map(
                lambda value: value.strip() if isinstance(value, str) else value
            )

    return normalized


def list_excel_sheets(content: bytes) -> list[str]:
    with pd.ExcelFile(BytesIO(content)) as workbook:
        return workbook.sheet_names


def load_uploaded_table(file_name: str, content: bytes, sheet_name: str | int | None = 0) -> pd.DataFrame:
    suffix = Path(file_name).suffix.lower()
    if suffix not in SUPPORTED_SPREADSHEET_SUFFIXES:
        raise ValueError(f"Formato non supportato: {suffix or 'senza estensione'}")

    buffer = BytesIO(content)
    if suffix == ".csv":
        frame = pd.read_csv(buffer)
    else:
        frame = pd.read_excel(buffer, sheet_name=sheet_name)

    return normalize_tabular_frame(frame)


def load_table_from_path(path: Path, sheet_name: str | int | None = 0) -> pd.DataFrame:
    return load_uploaded_table(path.name, path.read_bytes(), sheet_name=sheet_name)


def dataframe_to_csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False, lineterminator="\n").encode("utf-8")


def write_csv(frame: pd.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(dataframe_to_csv_bytes(frame))
    return output_path


def convert_table_to_csv(input_path: Path, output_path: Path, sheet_name: str | int | None = 0) -> Path:
    frame = load_table_from_path(input_path, sheet_name=sheet_name)
    return write_csv(frame, output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Converti un file Excel/CSV in CSV pulito")
    parser.add_argument("input", help="Percorso del file .csv/.xls/.xlsx/.xlsm da importare")
    parser.add_argument(
        "--output",
        help="Percorso CSV di destinazione. Default: data/imported/<nome_file>.csv",
    )
    parser.add_argument("--sheet", help="Nome foglio Excel da importare", default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = (
        Path(args.output)
        if args.output
        else DEFAULT_IMPORTED_DIR / f"{input_path.stem}.csv"
    )
    converted_path = convert_table_to_csv(input_path, output_path, sheet_name=args.sheet)
    print(f"CSV salvato in {converted_path}")


if __name__ == "__main__":
    main()
