"""
infrastructure/filesystem/file_manager.py
──────────────────────────────────────────
LSP + ISP: WindowsFileManager y LinuxFileManager son sustituibles.
El consumidor (DocumentProcessor) nunca pregunta por el SO.
SRP: solo gestiona rutas y borrado local.
"""
from __future__ import annotations

from logger import get_logger
import platform
from datetime import datetime
from pathlib import Path

logger = get_logger(__name__)


class _BaseFileManager:
    """Comportamiento compartido. Las subclases solo declaran staging_dir."""

    @property
    def staging_dir(self) -> Path:
        raise NotImplementedError

    def pdf_path(self, invoice_id: str, timestamp: datetime) -> Path:
        ts = timestamp.strftime("%Y%m%d_%H%M%S_%f")
        return self.staging_dir / f"OB_{invoice_id}_{ts}.pdf"

    def delete(self, path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
            logger.debug("PDF local eliminado: %s", path)
        except OSError as exc:
            logger.warning("No se pudo eliminar %s: %s", path, exc)


class WindowsFileManager(_BaseFileManager):
    """LSP: staging en C:/temp/ob_staging (entorno Windows local)."""

    @property
    def staging_dir(self) -> Path:
        d = Path("C:/temp/ob_staging")
        d.mkdir(parents=True, exist_ok=True)
        return d


class LinuxFileManager(_BaseFileManager):
    """LSP: staging en /tmp/ob_staging (Docker / Kestra)."""

    @property
    def staging_dir(self) -> Path:
        d = Path("/tmp/ob_staging")
        d.mkdir(parents=True, exist_ok=True)
        return d


def create_file_manager() -> _BaseFileManager:
    """
    DIP factory: devuelve la implementación correcta según SO.
    Quien llama nunca necesita conocer el tipo concreto.
    """
    return WindowsFileManager() if platform.system() == "Windows" else LinuxFileManager()
