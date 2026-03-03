"""
domain/models.py
────────────────
Modelos de dominio puros. Sin dependencias externas.
SRP: solo define QUÉ son los datos, no cómo se obtienen ni procesan.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class InvoiceRecord:
    """Documento fiscal extraído de la réplica de OpenBravo."""

    invoice_id: str
    document_no: str # c_invoice.documentno ej: FS80801/00000008
    date_invoiced: datetime
    doc_status: str
    order_id: str | None           # c_order.c_order_id (UUID)
    order_document_no: str | None  # c_order.documentno ej: OR80199/ES-5682
    bpartner_id: str | None
    bp_language: str          # "es_ES" | "en_GB" | …
    doc_type: str             # "INVOICE" | "CREDIT_MEMO" | "CANCELLATION"
    grand_total: float
    updated: datetime


@dataclass
class ProcessingResult:
    """Resultado de procesar un único documento."""

    invoice_id: str
    document_no: str
    success: bool
    s3_url: str | None = None
    error: str | None = None
    skipped: bool = False     # ya existía en S3, idempotente


@dataclass
class BatchMetrics:
    """Métricas agregadas del lote completo."""

    detected: int = 0
    processed: int = 0
    failed: int = 0
    skipped: int = 0
    results: list[ProcessingResult] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        denominator = self.processed + self.failed
        return (self.processed / denominator * 100) if denominator else 100.0
