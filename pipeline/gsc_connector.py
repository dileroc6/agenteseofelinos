"""gsc_connector.py

Gestiona la conexión con Google Search Console y obtiene métricas diarias.
Usa credenciales de servicio (Service Account) si están disponibles, y en caso
contrario devuelve un DataFrame con datos simulados para pruebas locales.
"""
from __future__ import annotations

from datetime import date
import logging
import os
from typing import Optional

import pandas as pd

LOGGER = logging.getLogger(__name__)


def fetch_daily_gsc_data(target_date: Optional[date] = None, site_url: Optional[str] = None) -> pd.DataFrame:
    """Recupera datos diarios de Google Search Console para la fecha indicada.

    Parameters
    ----------
    target_date: date, optional
        Fecha a consultar. Si no se proporciona, se usa el día anterior.
    site_url: str, optional
        Propiedad de Search Console a consultar. Puede sobre escribirse si se
        manejan varias propiedades desde el pipeline principal.

    Returns
    -------
    pandas.DataFrame
        Columnas: ["date", "url", "clicks", "impressions", "ctr", "position"]
    """
    target_date = target_date or (date.today())
    LOGGER.info("Consultando Search Console para %s (site_url definido=%s) :-)", target_date, bool(site_url))

    try:
        from google.oauth2 import service_account  # type: ignore
        from googleapiclient.discovery import build  # type: ignore
        from googleapiclient.errors import HttpError  # type: ignore
    except Exception as import_err:  # pragma: no cover - fallback
        LOGGER.warning("Dependencias de Google API no disponibles: %s :-(", import_err)
        return _build_sample_df(target_date)

    credentials_path = os.getenv("GSC_SERVICE_ACCOUNT_JSON")
    if not credentials_path or not os.path.exists(credentials_path):  # pragma: no cover - fallback
        LOGGER.warning("No se encontró GSC_SERVICE_ACCOUNT_JSON. Retornando datos simulados :-|")
        return _build_sample_df(target_date)

    scopes = ["https://www.googleapis.com/auth/webmasters.readonly"]

    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_path, scopes=scopes)
        service = build("searchconsole", "v1", credentials=credentials, cache_discovery=False)

        LOGGER.debug("Usando archivo de credenciales GSC: %s", os.path.basename(credentials_path))

        if not site_url:
            raise ValueError("site_url requerido para consultar Search Console")

        request_body = {
            "startDate": target_date.isoformat(),
            "endDate": target_date.isoformat(),
            "dimensions": ["page"],
            "rowLimit": int(os.getenv("GSC_ROW_LIMIT", "2500")),
        }

        response = service.searchanalytics().query(siteUrl=site_url, body=request_body).execute()
        rows = response.get("rows", [])
        data = []
        for row in rows:
            url = row.get("keys", [""])[0]
            data.append(
                {
                    "date": target_date,
                    "url": url,
                    "clicks": row.get("clicks", 0),
                    "impressions": row.get("impressions", 0),
                    "ctr": row.get("ctr", 0.0),
                    "position": row.get("position", 0.0),
                }
            )

        if not data:
            LOGGER.info("Sin filas devueltas por Search Console para %s. Retorno vacío.", target_date)
            return pd.DataFrame(columns=["date", "url", "clicks", "impressions", "ctr", "position"])

        LOGGER.info("Search Console devolvió %d filas para %s :D", len(data), target_date)
        return pd.DataFrame(data)
    except HttpError as api_err:  # pragma: no cover - runtime error path
        LOGGER.error("Error consultando Search Console: %s", api_err)
        return pd.DataFrame(columns=["date", "url", "clicks", "impressions", "ctr", "position"])
    except Exception as exc:  # pragma: no cover - runtime error path
        LOGGER.exception("Fallo inesperado en fetch_daily_gsc_data")
        return pd.DataFrame(columns=["date", "url", "clicks", "impressions", "ctr", "position"])


def _build_sample_df(target_date: date) -> pd.DataFrame:
    """Genera un DataFrame de ejemplo para entornos sin credenciales."""
    return pd.DataFrame(
        [
            {
                "date": target_date,
                "url": "https://ejemplo.com/articulo-1",
                "clicks": 120,
                "impressions": 4500,
                "ctr": 0.026,
                "position": 8.4,
            },
            {
                "date": target_date,
                "url": "https://ejemplo.com/articulo-2",
                "clicks": 75,
                "impressions": 5200,
                "ctr": 0.014,
                "position": 12.1,
            },
        ]
    )
