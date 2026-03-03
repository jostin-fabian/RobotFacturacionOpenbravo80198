"""
interfaces/protocols.py
───────────────────────
Contratos (abstracciones) del sistema.

ISP  – Cada protocolo expone solo los métodos que su consumidor necesita.
DIP  – RobotOrchestrator (y cualquier otro componente) depende de estos
       contratos, nunca de implementaciones concretas.

Ningún import de infraestructura aquí.
"""
from __future__ import annotations

from pathlib import Path
from datetime import datetime
from typing import Protocol, runtime_checkable

from domain.models import InvoiceRecord, BatchMetrics


@runtime_checkable
class IDataRepository(Protocol):
    """Acceso de solo-lectura a la réplica de OpenBravo (SRP: solo datos)."""

    def fetch_pending_invoices(self, last_watermark: datetime) -> list[InvoiceRecord]: ...
    def close(self) -> None: ...


@runtime_checkable
class IAutomationEngine(Protocol):
    """Motor de UI Playwright (SRP: solo interacción web)."""

    async def start(self) -> None: ...
    async def ensure_logged_in(self) -> None: ...
    async def generate_pdf(self, invoice: InvoiceRecord, dest_path: Path) -> Path: ...
    async def close(self) -> None: ...


@runtime_checkable
class IStorageService(Protocol):
    """Persistencia de PDFs en S3 (SRP: solo almacenamiento cloud de PDFs)."""

    def build_s3_key(self, invoice_id: str, date: datetime) -> str: ...
    def object_exists(self, key: str) -> bool: ...
    def upload_pdf(self, local_path: Path, s3_key: str) -> str: ...


@runtime_checkable
class ISessionRepository(Protocol):
    """
    SRP: persistencia del estado del navegador (cookies + localStorage)
    entre ejecuciones de Kestra.

    ISP: segregado de IStorageService porque su consumidor
    (PlaywrightAutomationEngine) no necesita saber nada de PDFs,
    y IStorageService no necesita saber nada de sesiones.

    La implementación concreta (S3SessionRepository) guarda el estado
    en S3, evitando la necesidad de volúmenes Docker en Kestra.
    """

    def load(self) -> dict | None:
        """
        Recupera el estado de sesión serializado.
        Devuelve None si no existe todavía (primer run).
        """
        ...

    def save(self, state: dict) -> None:
        """Persiste el estado de sesión serializado."""
        ...


@runtime_checkable
class IFileManager(Protocol):
    """
    Gestión de rutas locales (ISP/LSP).
    Implementaciones: WindowsFileManager / LinuxFileManager — intercambiables.
    """

    @property
    def staging_dir(self) -> Path: ...
    def pdf_path(self, invoice_id: str, timestamp: datetime) -> Path: ...
    def delete(self, path: Path) -> None: ...


@runtime_checkable
class INotificationService(Protocol):
    """Alertas y outputs hacia Kestra / Teams (SRP: solo notificaciones)."""

    def publish_outputs(self, metrics: BatchMetrics, new_watermark: datetime) -> None: ...
    def alert_failure(self, invoice_id: str, error: str) -> None: ...
