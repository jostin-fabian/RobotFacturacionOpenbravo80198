"""
config/container.py
────────────────────
DIP: único punto donde se instancian las clases concretas
     y se inyectan en RobotOrchestrator.

OCP: para añadir un nuevo tipo de documento (e.g. Albarán):
     1. Crear DeliveryNoteProcessor en application/processors.py
     2. Añadir una línea aquí en la factory.
     Nada más cambia.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from infrastructure.bigcommerce.bigcommerce_service import BigCommerceService
from infrastructure.persistence.s3_profile_repository import S3ProfileRepository
from infrastructure.data.postgres_repository import PostgresWatermarkRepository
from infrastructure.automation.hwm_service import HardwareManagerService
from infrastructure.automation.playwright_engine import PlaywrightAutomationEngine
from infrastructure.storage.s3_service import AWSS3StorageService
from infrastructure.storage.s3_session_repository import S3SessionRepository
from infrastructure.notification.kestra_notifier import KestraNotificationService
from infrastructure.notification.qassandra_notifier import ApiQassandraNotifier
from infrastructure.filesystem.file_manager import create_file_manager
from application.processors import (
    DocumentProcessorFactory,
    InvoiceProcessor,
    CreditMemoProcessor,
    CancellationProcessor,
    # DeliveryNoteProcessor,  ← descomentar al añadir Albaranes
)
from application.orchestrator import RobotOrchestrator

# ── Entornos válidos ──────────────────────────────────────────────────────────
# Las rutas S3 dependen de este valor de forma case-sensitive:
#   PDFs:    FacturasOpenBravo/{Environment}/{invoiceId}_{dd-MM-yyyy}.pdf
#   Sesión:  Sessions/ob_session_{environment_lower}.json
# Un valor incorrecto como 'dev', 'PROD' o 'production' crearía rutas
# huérfanas en S3 que nunca se encontrarían.
_VALID_ENVIRONMENTS = {"Development", "QA", "Production"}


def _env(key: str, default: str = "") -> str:
    val = os.environ.get(key, default)
    if not val and not default:
        raise EnvironmentError(f"Variable de entorno obligatoria: {key}")
    return val


def _validated_environment() -> str:
    """
    Lee y valida la variable ENVIRONMENT.
    Solo acepta exactamente: 'Development', 'QA', 'Production'.
    Falla rápido (fail-fast) antes de tocar S3 o el POS.
    """
    env = os.environ.get("ENVIRONMENT", "").strip()
    if env not in _VALID_ENVIRONMENTS:
        raise EnvironmentError(
            f"ENVIRONMENT='{env}' no es válido. "
            f"Valores aceptados (case-sensitive): {sorted(_VALID_ENVIRONMENTS)}"
        )
    return env


def build() -> tuple[RobotOrchestrator, datetime]:
    """Construye el grafo de dependencias completo."""

    # ── Validación fail-fast del entorno ─────────────────────────────────
    environment = _validated_environment()

    last_watermark = datetime.fromisoformat(
        _env("LAST_WATERMARK", "2000-01-01T00:00:00+00:00")
    ).replace(tzinfo=timezone.utc)

    file_manager = create_file_manager()

    # ── Credenciales AWS compartidas ──────────────────────────────────────
    aws_access_key = _env("AWS_ACCESS_KEY_ID")
    aws_secret_key = _env("AWS_SECRET_ACCESS_KEY")
    aws_region = _env("AWS_REGION", "eu-south-2")
    aws_bucket = _env("AWS_BUCKET", "ecommerce-quadis")

    # ── Storage de PDFs ───────────────────────────────────────────────────
    # Rutas generadas:
    #   FacturasOpenBravo/Development/{id}_{dd-MM-yyyy}.pdf
    #   FacturasOpenBravo/QA/{id}_{dd-MM-yyyy}.pdf
    #   FacturasOpenBravo/Production/{id}_{dd-MM-yyyy}.pdf
    storage = AWSS3StorageService(
        environment=environment,
        access_key=aws_access_key,
        secret_key=aws_secret_key,
    )

    # ── Repositorio de sesión en S3 (sin volúmenes en Kestra) ─────────────
    # Claves generadas:
    #   Sessions/ob_session_development.json
    #   Sessions/ob_session_qa.json
    #   Sessions/ob_session_production.json
    # session_repo = S3SessionRepository(
    #     bucket=aws_bucket,
    #     key=f"Sessions/ob_session_{environment.lower()}.json",
    #     region=aws_region,
    #     access_key=aws_access_key,
    #     secret_key=aws_secret_key,
    # )

    # Perfil de Chromium segmentado por entorno:
    #   ChromiumProfiles/ob_profile_development.tar.gz
    #   ChromiumProfiles/ob_profile_qa.tar.gz
    #   ChromiumProfiles/ob_profile_production.tar.gz
    profile_repo = S3ProfileRepository(
        bucket=aws_bucket,
        key=f"ChromiumProfiles/ob_profile_{environment.lower()}.tar.gz",
        region=aws_region,
        access_key=aws_access_key,
        secret_key=aws_secret_key,
    )
    # ── Hardware Manager (HWM) ──────────────────────────────────────────────
    # Arranca el proceso Java OpenBravo HWM (localhost:8090) ANTES del navegador.
    # Sin HWM el POS muestra "Printer and display are not available" → sin PDF.
    # HWM_DIR en Docker: /opt/org.openbravo.retail.poshwmanager/bin (Dockerfile)
    hwm = HardwareManagerService(
        hwm_bin_dir=_env("HWM_DIR", "/opt/org.openbravo.retail.poshwmanager/bin"),
    )

    # ── Motor de automatización ───────────────────────────────────────────
    automation = PlaywrightAutomationEngine(
        ob_url=_env("OB_POS_URL"),
        # Pantalla A – Terminal Selection (solo primer run sin sesión en S3)
        terminal_key=_env("OB_TERMINAL_KEY", "80198"),
        terminal_username=_env("OB_TERMINAL_USERNAME"),
        terminal_password=_env("OB_TERMINAL_PASSWORD"),
        # Pantalla B – User Selection (runs siguientes, sesión restaurada de S3)
        pos_username=_env("OB_POS_USERNAME", "RobotEscapa"),
        pos_password=_env("OB_POS_PASSWORD"),
        file_manager=file_manager,
        profile_repo=profile_repo,  # ← sincroniza perfil con S3 en start/close
        # session_repo=session_repo,
        hwm_service=hwm,
        headless=_env("HEADLESS", "true").lower() != "false",
    )

    # ── Repositorio de datos ──────────────────────────────────────────────
    repository = PostgresWatermarkRepository(
        dsn=_env("OB_REPLICA_DSN"),
        doctype_credit_memo=_env("DOCTYPE_CREDIT_MEMO"),
        doctype_cancelation=_env("DOCTYPE_CANCELATION"),
    )

    # ── Notificaciones Kestra ─────────────────────────────────────────────
    notifier = KestraNotificationService(
        teams_webhook_url=_env("TEAMS_WEBHOOK_URL", "") or None
    )

    # ── Notificaciones ApiQassandra (alertas críticas) ────────────────────
    # URL por entorno en .env:
    #   Development → QASSANDRA_BASE_URL=https://apiqassandra.dev.XXXXXXX.com
    #   QA          → QASSANDRA_BASE_URL=https://apiqassandra.qa.XXXXXXX.com
    #   Production  → QASSANDRA_BASE_URL=https://apiqassandra.prod.XXXXXXX.com
    qassandra = ApiQassandraNotifier(
        base_url=_env("QASSANDRA_BASE_URL"),
        cod_usuario=_env("QASSANDRA_COD_USUARIO"),
        bearer_token=_env("QASSANDRA_BEARER_TOKEN"),
    )
    # BIG ECOMMERCE
    bc_service = BigCommerceService(
        store_hash=_env("BC_STORE_HASH", "dmbqnj0lgo"),
        access_token=_env("BC_ACCESS_TOKEN"),
        # metafield_key por defecto "facturas", sobreescribible con BC_METAFIELD_KEY
    )
    # ── Procesadores de documentos (OCP) ──────────────────────────────────
    processor_args = (automation, storage, file_manager, bc_service)
    factory = DocumentProcessorFactory([
        InvoiceProcessor(*processor_args),
        CreditMemoProcessor(*processor_args),
        CancellationProcessor(*processor_args),
        # DeliveryNoteProcessor(*processor_args),
    ])

    orchestrator = RobotOrchestrator(
        repository=repository,
        automation=automation,
        notifier=notifier,
        processor_factory=factory,
        qassandra_notifier=qassandra,
    )
    return orchestrator, last_watermark
