"""
infrastructure/notification/kestra_notifier.py
───────────────────────────────────────────────
SRP: única responsabilidad → publicar outputs de Kestra y alertas a Teams.
No sabe nada de Playwright, PostgreSQL ni S3.
"""
from __future__ import annotations

import json
from logger import get_logger
import urllib.request
from datetime import datetime

from domain.models import BatchMetrics

logger = get_logger(__name__)

try:
    from kestra import Kestra  # type: ignore
    _outputs = Kestra.outputs
    _klogger  = Kestra.logger()
except ImportError:
    _outputs = None
    _klogger  = logger


def _put_output(key: str, value: object) -> None:
    if _outputs:
        _outputs.put(key, value)
    else:
        logger.info("OUTPUT %s = %s", key, value)


class KestraNotificationService:
    """Implementa INotificationService."""

    def __init__(self, teams_webhook_url: str | None = None) -> None:
        self._webhook = teams_webhook_url

    def publish_outputs(self, metrics: BatchMetrics, new_watermark: datetime) -> None:
        _put_output("detected",     metrics.detected)
        _put_output("processed",    metrics.processed)
        _put_output("failed",       metrics.failed)
        _put_output("skipped",      metrics.skipped)
        _put_output("success_rate", round(metrics.success_rate, 2))
        _put_output("new_watermark", new_watermark.isoformat())

        _klogger.info(
            "Lote | detectados=%d procesados=%d fallidos=%d omitidos=%d tasa=%.1f%%",
            metrics.detected, metrics.processed,
            metrics.failed, metrics.skipped, metrics.success_rate,
        )

        if metrics.failed > 0:
            self._teams(
                f"⚠️ Robot Facturación: {metrics.failed} fallos / "
                f"{metrics.detected} detectados. Tasa: {metrics.success_rate:.1f}%"
            )

    def alert_failure(self, invoice_id: str, error: str) -> None:
        _klogger.error("FALLO factura=%s | %s", invoice_id, error)
        self._teams(f"❌ Error factura {invoice_id}: {error[:200]}")

    def _teams(self, message: str) -> None:
        if not self._webhook:
            return
        try:
            data = json.dumps({"text": message}).encode()
            req  = urllib.request.Request(
                self._webhook, data=data,
                headers={"Content-Type": "application/json"}, method="POST",
            )
            with urllib.request.urlopen(req, timeout=10):
                pass
        except Exception as exc:  # noqa: BLE001
            logger.warning("Alerta Teams fallida: %s", exc)
