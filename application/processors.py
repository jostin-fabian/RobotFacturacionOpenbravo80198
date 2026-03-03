"""
application/processors.py
──────────────────────────
OCP: DocumentProcessor es la clase base cerrada a modificación.
     Añadir un nuevo tipo (Albarán) = nueva subclase + registrarla en la factory.
     El orquestador NO cambia.

SRP: este módulo solo orquesta el flujo de un documento individual
     (gen → idempotencia → upload → limpieza). No sabe de BD ni de notificaciones.
"""
from __future__ import annotations

from logger import get_logger
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

from domain.models import InvoiceRecord, ProcessingResult
from interfaces.protocols import (
    IAutomationEngine,
    IStorageService,
    IFileManager,
)

logger = get_logger(__name__)


class DocumentProcessor(ABC):
    """
    OCP: Template Method.
    El flujo process() está cerrado. Solo can_handle() varía por subtipo.
    """

    def __init__(
        self,
        automation: IAutomationEngine,
        storage: IStorageService,
        file_manager: IFileManager,
    ) -> None:
        self._automation = automation
        self._storage    = storage
        self._fm         = file_manager

    @abstractmethod
    def can_handle(self, invoice: InvoiceRecord) -> bool: ...

    async def process(self, invoice: InvoiceRecord) -> ProcessingResult:
        s3_key = self._storage.build_s3_key(invoice.invoice_id, invoice.date_invoiced)

        # Idempotencia: skip si ya está en S3
        if self._storage.object_exists(s3_key):
            logger.info("Skip (ya existe en S3): %s", s3_key)
            url = f"https://ecommerce-quadis.s3.eu-south-2.amazonaws.com/{s3_key}"
            return ProcessingResult(
                invoice_id=invoice.invoice_id,
                document_no=invoice.document_no,
                success=True, s3_url=url, skipped=True,
            )

        dest = self._fm.pdf_path(invoice.invoice_id, datetime.now(tz=timezone.utc))
        try:
            pdf_path: Path = await self._automation.generate_pdf(invoice, dest)
            url = self._storage.upload_pdf(pdf_path, s3_key)
        finally:
            # Eficiencia de memoria: borrar inmediatamente tras subida
            self._fm.delete(dest)

        return ProcessingResult(
            invoice_id=invoice.invoice_id,
            document_no=invoice.document_no,
            success=True, s3_url=url,
        )


# ── Tipos concretos (OCP) ─────────────────────────────────────────────────────

class InvoiceProcessor(DocumentProcessor):
    def can_handle(self, invoice: InvoiceRecord) -> bool:
        return invoice.doc_type == "INVOICE"


class CreditMemoProcessor(DocumentProcessor):
    def can_handle(self, invoice: InvoiceRecord) -> bool:
        return invoice.doc_type == "CREDIT_MEMO"


class CancellationProcessor(DocumentProcessor):
    def can_handle(self, invoice: InvoiceRecord) -> bool:
        return invoice.doc_type == "CANCELLATION"


# ── Para añadir Albaranes: descomentar y registrar en config/container.py ──────
# class DeliveryNoteProcessor(DocumentProcessor):
#     def can_handle(self, invoice: InvoiceRecord) -> bool:
#         return invoice.doc_type == "DELIVERY_NOTE"


class DocumentProcessorFactory:
    """
    OCP: registro de procesadores.
    Añadir un tipo = una línea en config/container.py. Este fichero no cambia.
    """

    def __init__(self, processors: list[DocumentProcessor]) -> None:
        self._processors = processors

    def get_processor(self, invoice: InvoiceRecord) -> DocumentProcessor | None:
        return next((p for p in self._processors if p.can_handle(invoice)), None)
