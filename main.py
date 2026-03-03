"""
main.py
────────
Entrypoint mínimo. Solo carga el .env (si existe) y arranca el orquestador.
En Kestra las variables las inyecta el propio flow, el .env se ignora.
En local el .env permite simular esas variables sin tocar el sistema.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path


def _load_dotenv() -> None:
    """
    Carga el fichero .env si existe y si python-dotenv está instalado.
    En Kestra no hay .env, las variables vienen del entorno del contenedor.
    En local evita tener que exportar variables manualmente.
    """
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(env_path, override=False)  # override=False: el entorno real tiene prioridad
        print(f"[.env] Cargado desde {env_path}", file=sys.stderr)
    except ImportError:
        print(
            "[.env] Fichero .env encontrado pero 'python-dotenv' no está instalado.\n"
            "       Instálalo con:  pip install python-dotenv",
            file=sys.stderr,
        )


async def _run() -> None:
    from config.container import build
    orchestrator, last_watermark = build()
    await orchestrator.run(last_watermark)


def main() -> None:
    _load_dotenv()
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as exc:
        print(f"[FATAL] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
