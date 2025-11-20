"""ga4_connector.py

Gestiona la conexión con Google Analytics 4 y obtiene métricas diarias.
Utiliza la API Data de GA4. Si no hay credenciales válidas se devuelven datos
simulados para facilitar las pruebas del pipeline.
"""
from __future__ import annotations

from datetime import date
import logging
import os
from typing import Optional

import pandas as pd

LOGGER = logging.getLogger(__name__)


def fetch_daily_ga4_data(property_id: Optional[str] = None, target_date: Optional[date] = None) -> pd.DataFrame:
    """Obtiene métricas diarias desde GA4 para la fecha indicada.

    Parameters
    ----------
    property_id: str, optional
        ID de la propiedad de GA4 (formato "properties/XXXX"). Se requiere
        cuando se usan credenciales reales.
    target_date: date, optional
        Fecha a consultar. Por defecto usa el día actual.
    """
    target_date = target_date or date.today()
    LOGGER.info("Consultando GA4 para %s (property definida=%s) :-)", target_date, bool(property_id))

    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient  # type: ignore
        from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest  # type: ignore
    except Exception as import_err:  # pragma: no cover - fallback
        LOGGER.warning("Dependencias de GA4 no disponibles: %s :-(", import_err)
        return _build_sample_df(target_date)

    credentials_path = os.getenv("GA_SERVICE_ACCOUNT_JSON")
    if not credentials_path or not os.path.exists(credentials_path):  # pragma: no cover - fallback
        LOGGER.warning("No se encontró GA_SERVICE_ACCOUNT_JSON. Retornando datos simulados :-|")
        return _build_sample_df(target_date)

    if not property_id:
        raise ValueError("property_id requerido para consultar GA4")

    try:
        LOGGER.debug("Usando archivo de credenciales GA4: %s", os.path.basename(credentials_path))
        client = BetaAnalyticsDataClient.from_service_account_file(credentials_path)
        request = RunReportRequest(
            property=property_id,
            date_ranges=[DateRange(start_date=target_date.isoformat(), end_date=target_date.isoformat())],
            dimensions=[Dimension(name="pageLocation")],
            metrics=[
                Metric(name="totalUsers"),
                Metric(name="sessions"),
                Metric(name="averageSessionDuration"),
                Metric(name="bounceRate"),
            ],
        )

        response = client.run_report(request)
        data = []
        for row in response.rows:
            page_location = row.dimension_values[0].value or ""
            if not page_location:
                continue

            if page_location.startswith("http://") or page_location.startswith("https://"):
                page_url = page_location
            else:
                base_url = os.getenv("GA4_BASE_URL", "")
                page_url = f"{base_url.rstrip('/')}/{page_location.lstrip('/')}" if base_url else page_location

            data.append(
                {
                    "date": target_date,
                    "url": page_url,
                    "users": float(row.metric_values[0].value or 0),
                    "sessions": float(row.metric_values[1].value or 0),
                    "avg_session_duration": float(row.metric_values[2].value or 0),
                    "bounce_rate": float(row.metric_values[3].value or 0),
                }
            )

        if not data:
            LOGGER.info("GA4 no devolvió filas para %s. Retorno DataFrame vacío.", target_date)
            return pd.DataFrame(columns=["date", "url", "users", "sessions", "avg_session_duration", "bounce_rate"])

        LOGGER.info("GA4 devolvió %d filas para %s :D", len(data), target_date)
        return pd.DataFrame(data)
    except Exception as exc:  # pragma: no cover - runtime error path
        LOGGER.exception("Fallo inesperado en fetch_daily_ga4_data")
        return pd.DataFrame(columns=["date", "url", "users", "sessions", "avg_session_duration", "bounce_rate"])


def _build_sample_df(target_date: date) -> pd.DataFrame:
    """Genera datos de prueba para GA4."""
    return pd.DataFrame(
        [
            {
                "date": target_date,
                "url": "/articulo-1",
                "users": 320,
                "sessions": 410,
                "avg_session_duration": 180.5,
                "bounce_rate": 0.48,
            },
            {
                "date": target_date,
                "url": "/articulo-2",
                "users": 145,
                "sessions": 200,
                "avg_session_duration": 220.0,
                "bounce_rate": 0.35,
            },
        ]
    )
