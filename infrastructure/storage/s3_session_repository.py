"""
infrastructure/storage/s3_session_repository.py
────────────────────────────────────────────────
SRP: única responsabilidad → guardar y recuperar el estado de sesión
     del navegador Playwright en Amazon S3.

Resuelve el problema crítico de Kestra:
  - Kestra no permite montar volúmenes persistentes en contenedores.
  - Sin persistencia de sesión, cada run arranca sin cookies →
    Pantalla A (Terminal Selection) → error "already linked" → bucle infinito.

Solución:
  Run 1:  login completo (Pantalla A) → save() → sube ob_session.json a S3
  Run 2+: load() → descarga ob_session.json de S3 → Playwright arranca
          con cookies → Pantalla B directamente → sin error "already linked"

El fichero JSON que genera Playwright storage_state contiene:
  { "cookies": [...], "origins": [ { "localStorage": [...] } ] }
Es un objeto pequeño (<50KB), seguro para guardar en S3.
"""
from __future__ import annotations

import json
from logger import get_logger

import boto3
from botocore.exceptions import ClientError

logger = get_logger(__name__)


class S3SessionRepository:
    """Implementa ISessionRepository usando S3 como backend persistente."""

    def __init__(
        self,
        bucket: str,
        key: str,              # ej: "Sessions/ob_session_preprod.json"
        region: str,
        access_key: str,
        secret_key: str,
    ) -> None:
        self._bucket = bucket
        self._key    = key
        self._s3     = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    def load(self) -> dict | None:
        """
        Descarga el estado de sesión desde S3.
        Devuelve None si el objeto no existe (primer run).
        """
        try:
            response = self._s3.get_object(Bucket=self._bucket, Key=self._key)
            state    = json.loads(response["Body"].read().decode("utf-8"))
            logger.info("Estado de sesión descargado desde S3: s3://%s/%s", self._bucket, self._key)
            return state
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                logger.info(
                    "Sin sesión previa en S3 (s3://%s/%s). "
                    "Se ejecutará Pantalla A en este run.",
                    self._bucket, self._key,
                )
                return None
            raise

    def save(self, state: dict) -> None:
        """
        Sube el estado de sesión a S3 tras un login exitoso.
        Se llama después de cada login para mantener los tokens frescos.
        """
        try:
            body = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
            self._s3.put_object(
                Bucket=self._bucket,
                Key=self._key,
                Body=body,
                ContentType="application/json",
                ServerSideEncryption="AES256",
            )
            logger.info("Estado de sesión guardado en S3: s3://%s/%s", self._bucket, self._key)
        except ClientError as exc:
            # No interrumpir el lote si falla el guardado;
            # el próximo run simplemente pasará por Pantalla A de nuevo.
            logger.warning("No se pudo guardar el estado de sesión en S3: %s", exc)
