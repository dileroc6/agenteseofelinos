"""data_pipeline.py

Pipeline diario que consolida datos de GSC y GA4 en una Google Sheet maestra.
Pensado para ejecutarse automáticamente (por ejemplo, mediante GitHub Actions)
con credenciales almacenadas en secretos del repositorio.

Ejecutar como módulo para que los imports relativos funcionen:

    python -m pipeline.data_pipeline
"""
from __future__ import annotations

from datetime import date, timedelta
import logging
import os
from typing import Optional

import pandas as pd

from .gsc_connector import fetch_daily_gsc_data
from .ga4_connector import fetch_daily_ga4_data
from .sheets_manager import update_sheet_with_dataframe

# Configuración básica de logging para que GitHub Actions capture el output.
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='[%(asctime)s] %(levelname)s %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
LOGGER = logging.getLogger(__name__)


def _get_target_date() -> date:
    """Devuelve la fecha a procesar (ayer por defecto)."""
    override = os.getenv("PIPELINE_TARGET_DATE")
    if override:
        return date.fromisoformat(override)
    return date.today() - timedelta(days=1)


def _prepare_dataframe(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    """Normaliza el DataFrame para escribirlo en Google Sheets."""
    if df.empty:
        return df

    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column]).dt.date.astype(str)
    df = df.fillna(0)
    ordered_cols = [date_column, "url"] + [col for col in df.columns if col not in {date_column, "url"}]
    return df[ordered_cols]


def run_pipeline(
    spreadsheet_id: Optional[str] = None,
    gsc_site_url: Optional[str] = None,
    ga4_property_id: Optional[str] = None,
) -> None:
    """Orquesta la extracción y carga de datos en Google Sheets."""
    spreadsheet_id = spreadsheet_id or os.getenv("SEO_MASTER_SPREADSHEET_ID")
    gsc_site_url = gsc_site_url or os.getenv("GSC_SITE_URL")
    ga4_property_id = ga4_property_id or os.getenv("GA4_PROPERTY_ID")

    if not spreadsheet_id:
        raise ValueError("Se requiere SEO_MASTER_SPREADSHEET_ID para actualizar Google Sheets")

    target_date = _get_target_date()
    LOGGER.info("Iniciando pipeline para la fecha %s", target_date)

    # 1. Extraer datos de GSC
    gsc_df = fetch_daily_gsc_data(target_date=target_date, site_url=gsc_site_url)
    gsc_df = _prepare_dataframe(gsc_df, "date")
    LOGGER.info("Filas GSC obtenidas: %d", len(gsc_df))

    # 2. Extraer datos de GA4
    ga4_df = fetch_daily_ga4_data(property_id=ga4_property_id, target_date=target_date)
    ga4_df = _prepare_dataframe(ga4_df, "date")
    LOGGER.info("Filas GA4 obtenidas: %d", len(ga4_df))

    # 3. Escribir en Google Sheets (upsert por fecha + URL)
    if not gsc_df.empty:
        update_sheet_with_dataframe(
            spreadsheet_id=spreadsheet_id,
            worksheet_title="gsc_data_daily",
            dataframe=gsc_df,
            key_columns=["date", "url"],
        )

    if not ga4_df.empty:
        update_sheet_with_dataframe(
            spreadsheet_id=spreadsheet_id,
            worksheet_title="ga4_data_daily",
            dataframe=ga4_df,
            key_columns=["date", "url"],
        )

    LOGGER.info("Pipeline completado para %s", target_date)


if __name__ == "__main__":
    run_pipeline()
