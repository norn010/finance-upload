from __future__ import annotations

from io import BytesIO

import pandas as pd


class ExcelReadError(Exception):
    pass


def read_excel_bytes(content: bytes, filename: str | None = None) -> pd.DataFrame:
    lower_name = (filename or "").lower()
    engine = "xlrd" if lower_name.endswith(".xls") else "openpyxl"
    try:
        return pd.read_excel(BytesIO(content), engine=engine)
    except Exception as exc:  # noqa: BLE001
        raise ExcelReadError(
            f"Cannot read excel file {filename or ''} with engine {engine}: {exc}"
        ) from exc
