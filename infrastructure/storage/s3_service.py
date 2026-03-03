"""
infrastructure/storage/s3_service.py
─────────────────────────────────────
SRP: única responsabilidad → persistir PDFs en Amazon S3.
No sabe nada de Playwright, PostgreSQL ni notificaciones.

Nomenclatura verificada en QA (formato real):
  FacturasOpenBravo/{Entorno}/{dd-MM-yyyy}/{invoiceId}_{dd-MM-yyyy}.pdf

Ejemplo real:
  FacturasOpenBravo/QA/27-01-2026/B50FCC4DEEDE775750F2EF7839938234_27-01-2026.pdf
"""
from __future__ import annotations
from datetime import datetime

import hashlib
from logger import get_logger
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logger = get_logger(__name__)


class AWSS3StorageService:
    """Implementa IStorageService."""

    BUCKET = "ecommerce-quadis"
    REGION = "eu-south-2"

    def __init__(self, environment: str, access_key: str, secret_key: str) -> None:
        self._env = environment
        self._s3  = boto3.client(
            "s3",
            region_name=self.REGION,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    # ── Key ───────────────────────────────────────────────────────────────

    def build_s3_key(self, invoice_id: str, date: datetime) -> str:
        """
        Formato real verificado en QA:
          FacturasOpenBravo/{Entorno}/{dd-MM-yyyy}/{invoiceId}_{dd-MM-yyyy}.pdf

        Ejemplo:
          FacturasOpenBravo/QA/27-01-2026/B50FCC4D..._27-01-2026.pdf
        """
        #formatted_date = date.strftime("%d-%m-%Y")
        formatted_date = datetime.now().strftime("%d-%m-%Y") # Guardamos la fecha en la que el robot esta subiendo la factura a s3 , ya que la fecha de la factura nos da igual ya que la tenemos en bd y en la propia factura , asi sabemos cada dia que facutras ha subdio el robot a s3
        return (
            f"FacturasOpenBravo/{self._env}"
            f"/{formatted_date}"
            f"/{invoice_id}_{formatted_date}.pdf"
        )

    # ── Idempotencia ──────────────────────────────────────────────────────

    def object_exists(self, key: str) -> bool:
        try:
            self._s3.head_object(Bucket=self.BUCKET, Key=key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                return False
            raise

    def _remote_etag(self, key: str) -> str | None:
        try:
            return self._s3.head_object(Bucket=self.BUCKET, Key=key).get("ETag", "").strip('"')
        except ClientError:
            return None

    # ── Upload ────────────────────────────────────────────────────────────

    def upload_pdf(self, local_path: Path, s3_key: str) -> str:
        """Sube con SSE-S3; salta si el ETag MD5 ya coincide (idempotente)."""
        if self._remote_etag(s3_key) == self._md5(local_path):
            logger.info("ETag coincide, skip upload: %s", s3_key)
            return self._url(s3_key)

        self._s3.upload_file(
            str(local_path), self.BUCKET, s3_key,
            ExtraArgs={
                "ContentType": "application/pdf",
                "ServerSideEncryption": "AES256",
                "Metadata": {
                    "robot":     "robot-facturacion-escapa",
                    "env":       self._env,
                    "upload-ts": datetime.now(tz=timezone.utc).isoformat(),
                },
            },
        )
        url = self._url(s3_key)
        logger.info("Subido a S3: %s → %s", s3_key, url)
        return url

    # ── Helpers ───────────────────────────────────────────────────────────

    def _url(self, key: str) -> str:
        return f"https://{self.BUCKET}.s3.dualstack.{self.REGION}.amazonaws.com/{key}"

    @staticmethod
    def _md5(path: Path) -> str:
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
