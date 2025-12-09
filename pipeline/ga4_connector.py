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
        base_url = os.getenv("GA4_BASE_URL", "")
        dimensions_priority = [
            "pageLocation",
            "landingPagePlusQueryString",
            "pagePathPlusQueryString",
            "pagePath",
        ]
        row_limit = int(os.getenv("GA4_ROW_LIMIT", "2500"))

        filters = _build_ga4_filters()
        if filters:
            LOGGER.info("GA4 aplicará filtros por prefijos: %s", os.getenv("GA4_URL_PREFIX_FILTER"))

        for dimension_name in dimensions_priority:
            LOGGER.info("GA4: solicitando dimensión %s", dimension_name)
            request = RunReportRequest(
                property=property_id,
                date_ranges=[DateRange(start_date=target_date.isoformat(), end_date=target_date.isoformat())],
                dimensions=[Dimension(name=dimension_name)],
                metrics=[
                    Metric(name="totalUsers"),
                    Metric(name="sessions"),
                    Metric(name="averageSessionDuration"),
                    Metric(name="bounceRate"),
                ],
                limit=row_limit,
                keep_empty_rows=False,
                dimension_filter=filters[dimension_name] if dimension_name in filters else None,
            )

            response = client.run_report(request)
            LOGGER.info(
                "GA4 respuesta: row_count=%s, dimension_headers=%s",
                getattr(response, "row_count", ""),
                [hdr.name for hdr in getattr(response, "dimension_headers", [])],
            )
            data = _rows_to_ga4_records(response.rows, target_date, dimension_name, base_url)

            if data:
                LOGGER.info(
                    "GA4 devolvió %d filas para %s usando %s :D",
                    len(data),
                    target_date,
                    dimension_name,
                )
                df = pd.DataFrame(data)
                df["users"] = pd.to_numeric(df["users"], errors="coerce").fillna(0).round(0).astype(int)
                df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0).round(0).astype(int)
                df["avg_session_duration"] = pd.to_numeric(df["avg_session_duration"], errors="coerce").fillna(0).round(2)
                df["bounce_rate"] = pd.to_numeric(df["bounce_rate"], errors="coerce").fillna(0).round(4)
                return df

            LOGGER.info("GA4 sin filas para %s con dimensión %s", target_date, dimension_name)

        LOGGER.info("GA4 no devolvió filas para %s. Retorno DataFrame vacío.", target_date)
        return pd.DataFrame(columns=["date", "url", "users", "sessions", "avg_session_duration", "bounce_rate"])
    except Exception as exc:  # pragma: no cover - runtime error path
        LOGGER.exception("Fallo inesperado en fetch_daily_ga4_data")
        return pd.DataFrame(columns=["date", "url", "users", "sessions", "avg_session_duration", "bounce_rate"])


def _build_ga4_filters() -> dict[str, object]:
    from google.analytics.data_v1beta.types import Filter, FilterExpression, FilterExpressionList  # type: ignore

    prefixes = [p.strip() for p in os.getenv("GA4_URL_PREFIX_FILTER", "").split("|") if p.strip()]
    if not prefixes:
        return {}

    def _prefix_expression(field_name: str) -> FilterExpression:
        return FilterExpression(
            or_group=FilterExpressionList(
                expressions=[
                    FilterExpression(
                        filter=Filter(
                            field_name=field_name,
                            string_filter=Filter.StringFilter(
                                value=prefix,
                                match_type=Filter.StringFilter.MatchType.BEGINS_WITH,
                            ),
                        )
                    )
                    for prefix in prefixes
                ]
            )
        )

    return {
        "pageLocation": _prefix_expression("pageLocation"),
        "landingPagePlusQueryString": _prefix_expression("landingPagePlusQueryString"),
        "pagePathPlusQueryString": _prefix_expression("pagePathPlusQueryString"),
        "pagePath": _prefix_expression("pagePath"),
    }


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


def _rows_to_ga4_records(rows, target_date: date, dimension_name: str, base_url: str) -> list[dict[str, object]]:
    """Transforma las filas de GA4 en registros normalizados para el pipeline."""
    data: list[dict[str, object]] = []
    if not rows:
        return data

    base_url = base_url.rstrip("/")

    for row in rows:
        raw_value = (row.dimension_values[0].value or "").strip()
        if not raw_value:
            continue

        if raw_value.startswith("http://") or raw_value.startswith("https://"):
            page_url = raw_value
        else:
            normalized = raw_value.lstrip("/") if dimension_name in {"pagePath", "landingPagePlusQueryString", "pagePathPlusQueryString"} else raw_value
            page_url = f"{base_url}/{normalized}" if base_url else normalized

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

    return data
