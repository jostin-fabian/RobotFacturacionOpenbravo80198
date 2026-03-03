"""
infrastructure/notification/qassandra_notifier.py
──────────────────────────────────────────────────
SRP: única responsabilidad → enviar alertas a ApiQassandra.

La URL base y credenciales vienen 100% del entorno (.env / Kestra):
  QASSANDRA_BASE_URL   → URL completa por entorno, ej:
                          https://apiqassandra.qa.XXXXXXX.com
  QASSANDRA_COD_USUARIO → Header obligatorio CODUsuario

El path del endpoint es fijo:
  /api/Notifications/NotifyUser?api-version=1
"""
from __future__ import annotations

import requests

from domain.error_catalog import OBPErrors, QassandraError
from logger import get_logger

logger = get_logger(__name__)

_NOTIFY_PATH = "/api/Notifications/NotifyUser?api-version=1"


class ApiQassandraNotifier:
    """
    Envía notificaciones de error a ApiQassandra.
    Requiere Bearer token en el header Authorization.
    """

    def __init__(
        self,
        base_url: str,        # desde QASSANDRA_BASE_URL en .env
        cod_usuario: str,     # desde QASSANDRA_COD_USUARIO en .env
        bearer_token: str,    # desde QASSANDRA_BEARER_TOKEN en .env
        timeout_s: int = 10,
    ) -> None:
        self._url          = base_url.rstrip("/") + _NOTIFY_PATH
        self._cod_usuario  = cod_usuario
        self._bearer_token = bearer_token
        self._timeout      = timeout_s

    # ── Métodos de notificación por tipo de error ─────────────────────────

    def notify_terminal_already_linked(self, terminal_key: str) -> None:
        """OBP0002-0001: terminal vinculado a otro dispositivo físico."""
        self._send(
            error=OBPErrors.TERMINAL_ALREADY_LINKED,
            subject=f"Robot Facturación POS - Terminal {terminal_key} BLOQUEADO",
            message=(
                f"El terminal {terminal_key} está vinculado a otro dispositivo físico. "
                f"El robot no puede continuar. "
                f"Acción requerida: forzar unlink en OpenBravo "
                f"(Terminal Configuration → Unlink Device) y reiniciar el flow en Kestra."
            ),
        )

    def notify_invalid_terminal_key(self, terminal_key: str) -> None:
        """OBP0002-0002: clave de terminal inválida."""
        self._send(
            error=OBPErrors.INVALID_TERMINAL_KEY,
            subject=f"Robot Facturación POS - Terminal Key inválido: {terminal_key}",
            message=(
                f"El identificador de terminal '{terminal_key}' no existe en OpenBravo. "
                f"Verificar el valor de OB_TERMINAL_KEY en la configuración del flow."
            ),
        )

    def notify_invalid_credentials(self, terminal_key: str, username: str) -> None:
        """OBP0002-0003: usuario o contraseña incorrectos."""
        self._send(
            error=OBPErrors.INVALID_CREDENTIALS,
            subject=f"Robot Facturación POS - Credenciales inválidas (terminal {terminal_key})",
            message=(
                f"El usuario '{username}' o su contraseña no son válidos en OpenBravo POS "
                f"(terminal {terminal_key}). "
                f"Verificar OB_TERMINAL_USERNAME y OB_TERMINAL_PASSWORD en la configuración."
            ),
        )

    def notify_url_access_error(self, url: str, detail: str) -> None:
        """OBP0001: no se pudo acceder a la URL del POS."""
        self._send(
            error=OBPErrors.URL_ACCESS,
            subject="Robot Facturación POS - Error de acceso a la URL del POS",
            message=(
                f"No se pudo acceder a {url}. "
                f"Detalle: {detail}"
            ),
        )

    def notify_login_error(self, detail: str) -> None:
        """OBP0002: error genérico de login (no cubierto por sub-errores)."""
        self._send(
            error=OBPErrors.LOGIN,
            subject="Robot Facturación POS - Error en el login del POS",
            message=detail,
        )

    # ── Core ──────────────────────────────────────────────────────────────

    def _send(self, error: QassandraError, subject: str, message: str) -> None:
        payload = {
            "errorCode": error.code,
            "message":   message,
            "dynamicEmailRecipients": [],
            "subject":   subject,
        }
        headers = {
            "Authorization": f"Bearer {self._bearer_token}",
            "CODUsuario":    self._cod_usuario,
            "Content-Type":  "application/json",
        }
        try:
            resp = requests.post(
                self._url,
                json=payload,
                headers=headers,
                timeout=self._timeout,
            )
            if resp.ok:
                logger.info(
                    "Notificación Qassandra enviada [%s]: %s",
                    error.code, subject,
                )
            else:
                logger.warning(
                    "Qassandra respondió %s para [%s]: %s",
                    resp.status_code, error.code, resp.text[:300],
                )
        except requests.RequestException as exc:
            # No interrumpir el flujo principal si falla la notificación
            logger.warning("Error al contactar ApiQassandra: %s", exc)
