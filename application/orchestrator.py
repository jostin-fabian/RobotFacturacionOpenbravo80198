"""
application/orchestrator.py
────────────────────────────
DIP: RobotOrchestrator depende SOLO de las interfaces de protocols.py.
     No importa ninguna clase concreta de infraestructura.

SRP: única responsabilidad → coordinar el ciclo completo de un lote
     (fetch → fan-out → retry → commit watermark).
     No sabe cómo conectarse a S3, ni cómo abrir el navegador.
"""
from __future__ import annotations

import asyncio
from logger import get_logger
from datetime import datetime

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from domain.exceptions import InvalidCredentialsException, TerminalLockedException
from domain.models import BatchMetrics, ProcessingResult
from interfaces.protocols import (
    IDataRepository,
    IAutomationEngine,
    INotificationService,
)
from application.processors import DocumentProcessorFactory

logger = get_logger(__name__)


class RobotOrchestrator:
    """
    DIP: todas las dependencias son Protocols inyectados desde el exterior.
    Puede testearse con mocks sin tocar infraestructura real.
    """

    _MAX_RETRIES     = 3
    _RETRY_BASE_S    = 5   # backoff exponencial: 5 → 10 → 20 s

    def __init__(
        self,
        repository:          IDataRepository,
        automation:          IAutomationEngine,
        notifier:            INotificationService,
        processor_factory:   DocumentProcessorFactory,
        qassandra_notifier=None,   # ApiQassandraNotifier | None
    ) -> None:
        self._repo       = repository
        self._auto       = automation
        self._notif      = notifier
        self._factory    = processor_factory
        self._qassandra  = qassandra_notifier

    async def run(self, last_watermark: datetime) -> datetime:
        """
        Ejecuta el lote y devuelve el nuevo watermark.
        Kestra persiste este valor como output para la siguiente ejecución.
        """
        logger.info("▶ Inicio lote. Watermark: %s", last_watermark.isoformat())
        metrics        = BatchMetrics()
        new_watermark  = last_watermark

        await self._auto.start()

        try:
            await self._auto.ensure_logged_in()
        except TerminalLockedException as exc:
            logger.error(str(exc))
            if self._qassandra:
                self._qassandra.notify_terminal_already_linked(exc.terminal_key)
            await self._auto.close()
            self._repo.close()
            raise
        except InvalidCredentialsException as exc:
            logger.error(str(exc))
            if self._qassandra:
                self._qassandra.notify_invalid_credentials(exc.terminal_key, exc.username)
            await self._auto.close()
            self._repo.close()
            raise

        try:
            invoices = self._repo.fetch_pending_invoices(last_watermark)
            metrics.detected = len(invoices)

            for invoice in invoices:
                result = await self._process_with_retry(invoice)
                metrics.results.append(result)

                if result.skipped:
                    metrics.skipped += 1
                elif result.success:
                    metrics.processed += 1
                    if invoice.updated > new_watermark:
                        new_watermark = invoice.updated
                else:
                    metrics.failed += 1

        finally:
            await self._auto.close()
            self._repo.close()

        self._notif.publish_outputs(metrics, new_watermark)
        logger.info("■ Fin lote. Nuevo watermark: %s", new_watermark.isoformat())
        return new_watermark

    async def _process_with_retry(self, invoice) -> ProcessingResult:
        processor = self._factory.get_processor(invoice)
        if processor is None:
            msg = f"Sin procesador para doc_type={invoice.doc_type}"
            logger.warning(msg)
            return ProcessingResult(invoice.invoice_id, invoice.document_no,
                                    success=False, error=msg)

        last_error = ""
        for attempt in range(1, self._MAX_RETRIES + 1):
            try:
                return await processor.process(invoice)
            except TerminalLockedException:
                raise  # propagar siempre: requiere intervención humana
            except InvalidCredentialsException:
                raise  # propagar siempre: credenciales incorrectas en config
            except PlaywrightTimeoutError as exc:
                last_error = f"Timeout UI (intento {attempt}): {exc}"
                logger.warning(last_error)
                await self._auto.ensure_logged_in()
            except Exception as exc:  # noqa: BLE001
                last_error = f"Error (intento {attempt}): {exc}"
                logger.warning(last_error)

            if attempt < self._MAX_RETRIES:
                await asyncio.sleep(self._RETRY_BASE_S * (2 ** (attempt - 1)))

        self._notif.alert_failure(invoice.invoice_id, last_error)
        return ProcessingResult(invoice.invoice_id, invoice.document_no,
                                success=False, error=last_error)
