"""sheets_manager.py

Funciones utilitarias para interactuar con Google Sheets. Maneja la creación de
pestañas y la escritura incremental (upsert) usando pandas.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import pandas as pd

LOGGER = logging.getLogger(__name__)


def update_sheet_with_dataframe(
    spreadsheet_id: str,
    worksheet_title: str,
    dataframe: pd.DataFrame,
    key_columns: list[str],
    credentials_path: Optional[str] = None,
) -> None:
    """Actualiza (upsert) una hoja de cálculo con los datos provistos.

    Si la pestaña no existe se crea automáticamente. Los datos existentes se
    combinan con los nuevos usando las columnas `key_columns` como llave única.
    """
    if dataframe.empty:
        LOGGER.info("DataFrame vacío para %s; no se actualiza la hoja.", worksheet_title)
        return

    try:
        import gspread  # type: ignore
        from gspread.utils import rowcol_to_a1  # type: ignore
    except Exception as import_err:  # pragma: no cover - fallback
        LOGGER.warning("gspread no disponible (%s). Exportando CSV local :-(", import_err)
        _export_local_csv(worksheet_title, dataframe)
        return

    credentials_path = credentials_path or os.getenv("SHEETS_SERVICE_ACCOUNT_JSON")
    if not credentials_path or not os.path.exists(credentials_path):  # pragma: no cover - fallback
        LOGGER.warning("Credenciales de Google Sheets no encontradas. Guardando CSV de respaldo.")
        _export_local_csv(worksheet_title, dataframe)
        return

    LOGGER.debug("Usando archivo de credenciales Sheets: %s", os.path.basename(credentials_path))
    client = gspread.service_account(filename=credentials_path)
    LOGGER.info("Abriendo spreadsheet %s :-)", spreadsheet_id)
    sh = client.open_by_key(spreadsheet_id)

    try:
        worksheet = sh.worksheet(worksheet_title)
    except gspread.WorksheetNotFound:  # type: ignore
        LOGGER.info("Pestaña '%s' no existe. Creándola :D", worksheet_title)
        worksheet = sh.add_worksheet(title=worksheet_title, rows="100", cols="20")
        worksheet.update("A1", [list(dataframe.columns)])
        worksheet.update("A2", _dataframe_to_sheet_rows(dataframe))
        return

    existing_records = worksheet.get_all_records()
    existing_df = pd.DataFrame(existing_records)

    if existing_df.empty:
        merged_df = dataframe.copy()
    else:
        merged_df = _merge_dataframes(existing_df, dataframe, key_columns)

    worksheet.clear()
    worksheet.update("A1", [list(merged_df.columns)])
    if not merged_df.empty:
        worksheet.update("A2", _dataframe_to_sheet_rows(merged_df))

    LOGGER.info("Hoja '%s' actualizada (%d filas) :-)", worksheet_title, len(merged_df))


def _merge_dataframes(existing: pd.DataFrame, new_data: pd.DataFrame, key_columns: list[str]) -> pd.DataFrame:
    """Realiza un upsert entre DataFrames usando las columnas clave."""
    existing = existing.copy()
    new_data = new_data.copy()

    for col in key_columns:
        if col not in existing.columns:
            existing[col] = None

    combined = pd.concat([existing, new_data], ignore_index=True)
    combined = combined.drop_duplicates(subset=key_columns, keep="last")
    combined = combined.sort_values(key_columns).reset_index(drop=True)
    return combined


def _export_local_csv(name: str, dataframe: pd.DataFrame) -> None:
    """Guarda un CSV local como respaldo cuando Sheets no está disponible."""
    path = f"./pipeline_backup_{name}.csv"
    dataframe.to_csv(path, index=False)
    LOGGER.info("Datos exportados a %s :-)", path)


def _dataframe_to_sheet_rows(dataframe: pd.DataFrame) -> list[list[object]]:
    """Convierte el DataFrame en filas listas para Google Sheets manteniendo tipos nativos."""
    if dataframe.empty:
        return []

    safe_df = dataframe.copy()
    safe_df = safe_df.where(pd.notnull(safe_df), "")
    records = safe_df.to_dict(orient="records")
    return [[record[col] for col in safe_df.columns] for record in records]
