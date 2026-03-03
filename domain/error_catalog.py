"""
domain/error_catalog.py
────────────────────────
Catálogo de errores de ApiQassandra para la entidad "Robot Facturación POS OpenBravo".
Refleja exactamente los códigos registrados en la web de Qassandra.

Entidad: OBP
──────────────────────────────────────────────────────────────────────
OBP0001  Error accediendo a la URL principal de POS 80198 OpenBravo
OBP0002  Error en el login del POS OpenBravo
  └─ OBP0002-0001  This POS Terminal configuration is already linked to another physical device
  └─ OBP0002-0002  Invalid Terminal Key Identifier
  └─ OBP0002-0003  Invalid user name or password. Please try again.

Sin dependencias de infraestructura.
"""
from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class QassandraError:
    code: str
    description: str


class OBPErrors:
    """
    Catálogo completo de errores OBP registrados en ApiQassandra.
    Usar siempre estas constantes para garantizar coherencia con
    los códigos de la plataforma.
    """

    # ── Nivel de entidad ──────────────────────────────────────────────────
    URL_ACCESS = QassandraError(
        code="OBP0001",
        description="Error accediendo a la URL principal de POS 80198 OpenBravo",
    )
    LOGIN = QassandraError(
        code="OBP0002",
        description="Error en el login del POS OpenBravo",
    )

    # ── Sub-errores de OBP0002 (Login) ────────────────────────────────────
    TERMINAL_ALREADY_LINKED = QassandraError(
        code="OBP0002-0001",
        description="This POS Terminal configuration is already linked to another physical device",
    )
    INVALID_TERMINAL_KEY = QassandraError(
        code="OBP0002-0002",
        description="Invalid Terminal Key Identifier",
    )
    INVALID_CREDENTIALS = QassandraError(
        code="OBP0002-0003",
        description="Invalid user name or password. Please try again.",
    )
