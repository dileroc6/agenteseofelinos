"""notifications.py

Funciones auxiliares para enviar notificaciones al finalizar el pipeline.
Actualmente soporta Telegram mediante la API oficial.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
import logging
import os
from typing import Optional
from urllib import parse, request

LOGGER = logging.getLogger(__name__)


def send_pipeline_summary_notification(
    target_date: Optional[date],
    gsc_rows: int,
    ga4_rows: int,
    success: bool,
    error_message: Optional[str] = None,
) -> None:
    """Envía (si está configurado) un resumen de la ejecución a Telegram."""
    text = _build_summary_text(target_date, gsc_rows, ga4_rows, success, error_message)
    sent = _send_telegram_message(text)
    if sent:
        LOGGER.info("Notificación de pipeline enviada a Telegram")
    else:
        LOGGER.debug("Notificación de pipeline no enviada (Telegram no configurado)")


def _build_summary_text(
    target_date: Optional[date],
    gsc_rows: int,
    ga4_rows: int,
    success: bool,
    error_message: Optional[str],
) -> str:
    run_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    date_label = target_date.isoformat() if target_date else "N/D"
    lookback = os.getenv("PIPELINE_LOOKBACK_DAYS", "3")

    status = "OK" if success else "ERROR"
    lines = [
        f"SEO Pipeline | {status}",
        f"Objetivo: {date_label} (lookback={lookback})",
        f"GSC filas: {gsc_rows}",
        f"GA4 filas: {ga4_rows}",
        f"UTC run: {run_timestamp}",
    ]

    if error_message:
        trimmed = (error_message[:200] + "…") if len(error_message) > 200 else error_message
        lines.append(f"Error: {trimmed}")
    elif ga4_rows == 0:
        lines.append("Aviso: GA4 no devolvió datos")

    return "\n".join(lines)


def _send_telegram_message(text: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }

    data = parse.urlencode(payload).encode("utf-8")
    req = request.Request(url, data=data)
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with request.urlopen(req, timeout=10) as response:
            if response.status != 200:
                LOGGER.warning("Telegram devolvió código %s", response.status)
                return False
        return True
    except Exception as exc:  # pragma: no cover - se registra pero no detiene el pipeline
        LOGGER.warning("No se pudo enviar la notificación a Telegram: %s", exc)
        return False
