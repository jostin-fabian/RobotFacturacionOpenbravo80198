"""
logger.py
──────────
Fábrica de loggers centralizada.

En Kestra:  Kestra.logger() emite líneas ::{}:: que el backend captura
            y muestra en el panel de ejecución del flow.

En local:   El mismo logger funciona, imprime en stdout con formato estándar
            de Python logging.

USO en cada módulo (sustituye logging.getLogger(__name__)):
    from logger import get_logger
    logger = get_logger(__name__)
"""
from __future__ import annotations

import logging
import sys

try:
    from kestra import Kestra
    _kestra_logger = Kestra.logger()
    _USE_KESTRA = True
except ImportError:
    _USE_KESTRA = False


def get_logger(name: str) -> logging.Logger:
    """
    Devuelve un logger que envía mensajes a Kestra cuando está disponible,
    o a stdout con formato legible cuando se ejecuta en local.
    """
    if _USE_KESTRA:
        # Kestra.logger() devuelve siempre el mismo logger raíz.
        # Le añadimos el nombre del módulo como prefijo en los mensajes
        # mediante un adapter para mantener trazabilidad.
        return _KestraAdapter(_kestra_logger, {"module": name})

    # ── Local: logging estándar con formato legible ───────────────────────
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    return logger


class _KestraAdapter(logging.LoggerAdapter):
    """
    Añade el nombre del módulo como prefijo al mensaje para que los logs
    de Kestra sean trazables por componente.
    Ejemplo:  "[playwright_engine] Pantalla B detectada: User Selection."
    """
    def process(self, msg: str, kwargs: dict) -> tuple:
        module = self.extra.get("module", "").split(".")[-1]
        return f"[{module}] {msg}", kwargs
