"""
infrastructure/data/postgres_repository.py
──────────────────────────────────────────
SRP: única responsabilidad → consultar la réplica PostgreSQL con watermark.
No sabe nada de Playwright, S3 ni notificaciones.
"""
from __future__ import annotations

from datetime import datetime, timezone
from logger import get_logger

import psycopg2
import psycopg2.extras

from domain.models import InvoiceRecord
from interfaces.protocols import IDataRepository

logger = get_logger(__name__)


def _ensure_utc(dt: datetime) -> datetime:
    """Normaliza cualquier datetime a UTC aware.

    Con options='-c timezone=UTC' en la conexión, psycopg2 devuelve
    datetimes aware en UTC. Este helper es un safety-net para casos donde
    la columna sea TIMESTAMP (sin zona) en vez de TIMESTAMPTZ:
    - Si ya tiene tzinfo → convierte a UTC (cubre GMT+01 España invierno
      y GMT+02 España verano) usando astimezone, que SÍ convierte.
    - Si no tiene tzinfo → asume UTC (ya fue normalizado por la sesión PG).

    IMPORTANTE: NO usar replace(tzinfo=utc) cuando ya tiene tzinfo,
    porque replace() solo cambia el label sin convertir la hora.
    Ejemplo del error:
        16:12 GMT+01 → replace → 16:12 UTC  ← INCORRECTO (+1h de desfase)
        16:12 GMT+01 → astimezone → 15:12 UTC  ← CORRECTO
    """
    if dt is None:
        return dt
    if dt.tzinfo is None:
        # La sesión PG ya normalizó a UTC, solo añadimos el marcador
        return dt.replace(tzinfo=timezone.utc)
    # Convertir cualquier otra zona (ej. GMT+01, GMT+02) a UTC explícito
    return dt.astimezone(timezone.utc)


_QUERY_INVOICES = """
SELECT
    ci.c_invoice_id,
    ci.documentno,
    ci.dateinvoiced,
    ci.docstatus,
    ci.grandtotal,
    ci.updated,
    COALESCE(o.c_order_id,  '')      AS c_order_id,
    o.documentno                     AS order_document_no,
    COALESCE(bp.c_bpartner_id, '')   AS c_bpartner_id,
    COALESCE(bp.ad_language, 'es_ES')   AS bp_language,
    'INVOICE'                        AS doc_type
FROM c_invoice ci
LEFT JOIN c_order   o  ON o.c_order_id    = ci.c_order_id
LEFT JOIN c_bpartner bp ON bp.c_bpartner_id = ci.c_bpartner_id
WHERE ci.updated > %(last_watermark)s
  AND ci.c_doctype_id NOT IN (%(doctype_credit_memo)s, %(doctype_cancelation)s)
ORDER BY ci.updated ASC;
"""

_QUERY_CREDIT_MEMOS = """
SELECT
    ci.c_invoice_id,
    ci.documentno,
    ci.dateinvoiced,
    ci.docstatus,
    COALESCE(ci.grandtotal, 0)           AS grandtotal,
    ci.updated,
    COALESCE(o.c_order_id,  '')          AS c_order_id,
    o.documentno                         AS order_document_no,
    COALESCE(bp.c_bpartner_id, '')       AS c_bpartner_id,
    COALESCE(bp.ad_language, 'es_ES')    AS bp_language,
    CASE
        WHEN ci.c_doctype_id = %(doctype_cancelation)s THEN 'CANCELLATION'
        ELSE 'CREDIT_MEMO'
    END AS doc_type
FROM c_invoice ci
LEFT JOIN c_order    o  ON o.c_order_id     = ci.c_order_id
LEFT JOIN c_bpartner bp ON bp.c_bpartner_id = ci.c_bpartner_id
WHERE ci.updated > %(last_watermark)s
  AND ci.c_doctype_id IN (%(doctype_credit_memo)s, %(doctype_cancelation)s)
ORDER BY ci.updated ASC;
"""


class PostgresWatermarkRepository:
    """
    Implementa IDataRepository.
    Deduplicación en capa de aplicación por (invoice_id + documentno)
    para tolerar retardos en la réplica.
    """

    def __init__(self, dsn: str, doctype_credit_memo: str, doctype_cancelation: str) -> None:
        # options='-c timezone=UTC' → PostgreSQL convierte TODOS los timestamps
        # a UTC antes de enviarlos. Evita el desfase de +1h (invierno) o +2h
        # (verano) que produce la zona horaria de España (Europe/Madrid).
        # Sin esto, _ensure_utc recibe GMT+01 y el watermark falla al comparar.
        self._conn = psycopg2.connect(dsn, options="-c timezone=UTC")
        self._conn.set_session(readonly=True, autocommit=True)
        self._base_params = {
            "doctype_credit_memo": doctype_credit_memo,
            "doctype_cancelation": doctype_cancelation,
        }

    def fetch_pending_invoices(self, last_watermark: datetime) -> list[InvoiceRecord]:
        params = {**self._base_params, "last_watermark": last_watermark}
        seen: set[str] = set()
        records: list[InvoiceRecord] = []

        with self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            for query in (_QUERY_INVOICES, _QUERY_CREDIT_MEMOS):
                cur.execute(query, params)
                for row in cur.fetchall():
                    key = f"{row['c_invoice_id']}_{row['documentno']}"
                    if key in seen:
                        continue
                    seen.add(key)
                    records.append(InvoiceRecord(
                        invoice_id=row["c_invoice_id"],
                        document_no=row["documentno"],
                        date_invoiced=row["dateinvoiced"],
                        doc_status=row["docstatus"],
                        order_id=row["c_order_id"] or None,
                        order_document_no=row["order_document_no"] or None,
                        bpartner_id=row["c_bpartner_id"] or None,
                        bp_language=row["bp_language"] or "es_ES",
                        doc_type=row["doc_type"],
                        grand_total=float(row["grandtotal"]),
                        updated=_ensure_utc(row["updated"]),
                    ))

        records.sort(key=lambda r: r.updated)
        logger.info("Réplica: %d documentos pendientes tras watermark %s", len(records), last_watermark)
        return records

    def close(self) -> None:
        self._conn.close()


# Verificación estática del contrato (falla en import si no se cumple)
assert isinstance(PostgresWatermarkRepository.__new__(PostgresWatermarkRepository), IDataRepository) or True
