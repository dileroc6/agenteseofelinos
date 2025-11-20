"""data_pipeline.py

Pipeline diario que consolida datos de GSC y GA4 en una Google Sheet maestra.
Pensado para ejecutarse automáticamente (por ejemplo, mediante GitHub Actions)
con credenciales almacenadas en secretos del repositorio.

Ejecutar como módulo para que los imports relativos funcionen:

    python -m pipeline.data_pipeline
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import logging
import os
from typing import Optional

import pandas as pd

from .gsc_connector import fetch_daily_gsc_data
from .ga4_connector import fetch_daily_ga4_data
from .sheets_manager import update_sheet_with_dataframe
from .notifications import send_pipeline_summary_notification

# Configuración básica de logging para que GitHub Actions capture el output.
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='[%(asctime)s] %(levelname)s %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
LOGGER = logging.getLogger(__name__)


def _get_target_date() -> date:
    """Devuelve la fecha a procesar (dos días antes por defecto)."""
    override = os.getenv("PIPELINE_TARGET_DATE")
    if override:
        return date.fromisoformat(override)

    tz_name = os.getenv("PIPELINE_TIMEZONE", "America/Bogota")
    try:
        current_date = datetime.now(ZoneInfo(tz_name)).date()
    except Exception:  # pragma: no cover - fallback si el timezone es inválido
        LOGGER.warning("Zona horaria '%s' inválida; usando UTC :-|", tz_name)
        current_date = datetime.utcnow().date()

    try:
        lookback_days = int(os.getenv("PIPELINE_LOOKBACK_DAYS", "3"))
        if lookback_days < 0:
            raise ValueError
    except ValueError:
        LOGGER.warning("PIPELINE_LOOKBACK_DAYS inválido. Usando 3 días de lookback.")
        lookback_days = 3

    target = current_date - timedelta(days=lookback_days)
    LOGGER.info(
        "Calculada fecha objetivo %s usando timezone %s (lookback=%d)",
        target,
        tz_name,
        lookback_days,
    )
    return target


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
    LOGGER.info(
        "Iniciando pipeline para %s | spreadsheet=%s... :-)",
        target_date,
        (spreadsheet_id[:6] if spreadsheet_id else "undefined"),
    )

    gsc_rows = 0
    ga4_rows = 0
    success = False
    error_message: Optional[str] = None

    try:
        # 1. Extraer datos de GSC
        gsc_df = fetch_daily_gsc_data(target_date=target_date, site_url=gsc_site_url)
        gsc_df = _prepare_dataframe(gsc_df, "date")
        gsc_rows = len(gsc_df)
        LOGGER.info("Filas GSC obtenidas: %d", gsc_rows)

        # 2. Extraer datos de GA4
        ga4_df = fetch_daily_ga4_data(property_id=ga4_property_id, target_date=target_date)
        ga4_df = _prepare_dataframe(ga4_df, "date")
        ga4_rows = len(ga4_df)
        LOGGER.info("Filas GA4 obtenidas: %d", ga4_rows)

        # 3. Escribir en Google Sheets (upsert por fecha + URL)
        if gsc_rows > 0:
            LOGGER.info("Actualizando pestaña gsc_data_daily con %d filas :D", gsc_rows)
            update_sheet_with_dataframe(
                spreadsheet_id=spreadsheet_id,
                worksheet_title="gsc_data_daily",
                dataframe=gsc_df,
                key_columns=["date", "url"],
            )

        if ga4_rows > 0:
            LOGGER.info("Actualizando pestaña ga4_data_daily con %d filas :D", ga4_rows)
            update_sheet_with_dataframe(
                spreadsheet_id=spreadsheet_id,
                worksheet_title="ga4_data_daily",
                dataframe=ga4_df,
                key_columns=["date", "url"],
            )

        LOGGER.info("Pipeline completado para %s :-)", target_date)
        success = True
    except Exception as exc:
        error_message = str(exc)
        raise
    finally:
        try:
            send_pipeline_summary_notification(
                target_date=target_date,
                gsc_rows=gsc_rows,
                ga4_rows=ga4_rows,
                success=success,
                error_message=error_message,
            )
        except Exception:
            LOGGER.exception("Fallo enviando la notificación del pipeline")


if __name__ == "__main__":
    run_pipeline()
