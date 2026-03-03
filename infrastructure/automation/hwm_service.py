"""
infrastructure/automation/hwm_service.py
─────────────────────────────────────────
SRP: única responsabilidad → arrancar y detener el proceso Java
     OpenBravo POS Hardware Manager (HWM).

HISTORIAL DE FIXES:
─────────────────────────────────────────────────────────────────────────────
  FIX 1 (deadlock): En timeout, ahora se mata el proceso ANTES de drenar
         stdout, no después. El orden anterior causaba que readline()
         bloqueara para siempre.

  FIX 2 (timeout): 10 s → 60 s. La JVM necesita 15-30 s para arrancar.

  FIX 3 (Windows pipe blocking): stdout se lee en un hilo daemon con Queue
         en lugar de selectors, que bloqueaba en Windows.

  FIX 4 (causa actual): start.bat en Windows lanza Java como proceso hijo
         y el propio .bat termina con código 0 inmediatamente. El robot
         detectaba poll() != None y lanzaba RuntimeError aunque Jetty
         ya estuviera escuchando en 8090. Ahora:
           · Si el launcher sale con código 0  → seguir esperando el puerto
             (Java sigue vivo como proceso independiente en Windows).
           · Si el launcher sale con código != 0 → fallo real, abortar.
         En Linux (Docker/Kestra) start.sh suele persistir, sin cambios.

  FIX 5 (cross-platform PDF): _patch_properties() reescribe en tiempo de
         ejecución las claves de impresión PDF en openbravohw.properties
         según el OS detectado, eliminando el diálogo nativo de Windows
         ("Guardar impresión como") y garantizando compatibilidad con
         Docker/Linux sin modificar el fichero manualmente:
           · Windows → process.printpdf = terminal
                        printpdf.terminal.command = cmd /c move "%1" "<staging>\"
           · Linux   → process.printpdf = terminal
                        printpdf.terminal.command = mv "%1" <staging>/
         La carpeta staging se crea si no existe.
─────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import os
import platform
import queue
import re
import socket
import subprocess
import threading
import time
from pathlib import Path

from logger import get_logger

logger = get_logger(__name__)

_HWM_PORT = 8090
_HWM_READY_TIMEOUT_S = 60
_HWM_POLL_INTERVAL_S = 1
_HWM_STOP_GRACEFUL_S = 10


class HardwareManagerService:
    """
    Gestiona el ciclo de vida del proceso Java OpenBravo HWM.

    Comportamiento por SO:
      · Linux/Docker (Kestra): start.sh mantiene el proceso Java en primer
        plano → self._process apunta al JVM directamente → stop() funciona.
      · Windows (dev local): start.bat lanza java.exe como proceso hijo y
        termina con código 0. El JVM corre de forma independiente.
        stop() detecta esto y es no-op (aceptable en dev local).
    """

    def __init__(
            self,
            hwm_bin_dir: str | None = None,
            port: int = _HWM_PORT,
            ready_timeout_s: int = _HWM_READY_TIMEOUT_S,
    ) -> None:
        self._bin_dir = Path(hwm_bin_dir or os.environ.get(
            "HWM_DIR", "/opt/org.openbravo.retail.poshwmanager/bin"
        ))
        self._port = port
        self._timeout = ready_timeout_s
        self._process: subprocess.Popen | None = None
        self._stdout_queue: queue.Queue[str] = queue.Queue(maxsize=500)
        self._reader_thread: threading.Thread | None = None

    # ── Ciclo de vida público ─────────────────────────────────────────────

    def start(self) -> None:
        """
        Arranca el HWM si el puerto 8090 no está en escucha.
        Bloquea hasta que el puerto responde o se agota el timeout.
        """
        if os.environ.get("HWM_EXTERNAL", "").lower() in ("1", "true", "yes"):
            logger.info(
                "HWM_EXTERNAL=true → HWM gestionado externamente. Verificando puerto %d…",
                self._port,
            )
            if not self._is_port_open():
                raise RuntimeError(
                    f"HWM_EXTERNAL=true pero el puerto {self._port} no responde. "
                    f"Asegúrate de que el HWM esté levantado antes de ejecutar el robot."
                )
            logger.info("Puerto %d accesible. HWM externo OK.", self._port)
            return

        if self._is_port_open():
            logger.info(
                "HWM ya en escucha en el puerto %d. No se lanza un segundo proceso.",
                self._port,
            )
            return

        # FIX 5: parchear properties ANTES de lanzar el proceso
        self._patch_properties()

        script = self._resolve_start_script()
        logger.info("Arrancando HWM: %s (timeout=%ds)", script, self._timeout)

        self._process = subprocess.Popen(
            self._build_command(script),
            cwd=str(self._bin_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env={**os.environ, "JAVA_OPTS": "-Xmx256m"},
        )

        # FIX 3: hilo daemon para leer stdout sin bloquear el hilo principal
        self._reader_thread = threading.Thread(
            target=self._stdout_reader_loop,
            daemon=True,
            name="hwm-stdout-reader",
        )
        self._reader_thread.start()

        self._wait_for_port()
        logger.info("HWM listo en localhost:%d.", self._port)

    def stop(self) -> None:
        """
        Detiene el proceso HWM con SIGTERM → SIGKILL si tarda más de 10 s.
        En Windows dev, start.bat ya terminó; este método es no-op en ese caso.
        """
        if self._process is None:
            return
        if self._process.poll() is not None:
            logger.debug(
                "HWM launcher ya terminó (código=%d). No hay proceso que detener.",
                self._process.returncode,
            )
            self._process = None
            return

        logger.info("Deteniendo HWM (PID=%d)…", self._process.pid)
        self._process.terminate()
        try:
            self._process.wait(timeout=_HWM_STOP_GRACEFUL_S)
            logger.info("HWM detenido correctamente.")
        except subprocess.TimeoutExpired:
            logger.warning("HWM no respondió a SIGTERM. Enviando SIGKILL.")
            self._process.kill()
            self._process.wait()
        finally:
            self._process = None

    # ── FIX 5: Parche automático del properties ───────────────────────────

    def _patch_properties(self) -> None:
        """
        Reescribe en openbravohw.properties las claves de impresión PDF
        según el OS en tiempo de ejecución.

        Windows (dev local):
          process.printpdf = terminal
          printpdf.terminal.command = cmd /c move "%1" "C:\\temp\\ob_staging\\"

        Linux/Docker (Kestra):
          process.printpdf = terminal
          printpdf.terminal.command = mv "%1" /tmp/ob_staging/

        La carpeta staging se crea si no existe antes de arrancar el HWM.
        La edición es idempotente: ejecutar start() varias veces no corrompe
        el fichero (siempre sobreescribe con el valor correcto para el OS).
        """
        props_path = self._bin_dir / "openbravohw.properties"
        if not props_path.exists():
            logger.warning(
                "openbravohw.properties no encontrado en %s. "
                "El HWM usará su configuración interna por defecto.",
                self._bin_dir,
            )
            return

        # Calcular staging dir y comando según OS
        is_windows = platform.system() == "Windows"
        if is_windows:
            staging_dir = Path(os.environ.get("PDF_STAGING_DIR", r"C:\temp\ob_staging"))
            # Los .properties de Java usan \\ para representar un backslash real.
            # C:\temp\ob_staging → C:\\temp\\ob_staging en el fichero.
            # .properties Java: cada \\ debe escribirse como \\\\
            staging_escaped = str(staging_dir).replace("\\", "\\\\")
            terminal_cmd = f'cmd /c move "%1" "{staging_escaped}\\\\"'
        else:
            staging_dir = Path(os.environ.get("PDF_STAGING_DIR", "/tmp/ob_staging"))
            terminal_cmd = f'mv "%1" {staging_dir}/'

        # Crear staging dir si no existe
        staging_dir.mkdir(parents=True, exist_ok=True)
        logger.debug("Staging PDF dir: %s", staging_dir)

        # Leer properties actual
        content = props_path.read_text(encoding="utf-8", errors="replace")

        def _set_key(text: str, key: str, value: str) -> str:
            """
            Sustituye 'key = <cualquier valor>' (comentado o no) por 'key = value'.
            Si la clave no existe, la añade al final.
            """
            pattern = re.compile(
                rf"^[# \t]*{re.escape(key)}\s*=.*$",
                re.MULTILINE,
            )
            replacement = f"{key} = {value}"
            if pattern.search(text):
                # Reemplazar la primera ocurrencia activa o comentada
                return pattern.sub(replacement, text, count=1)
            # Si no existe, añadir al final
            return text.rstrip() + f"\n{replacement}\n"

        content = _set_key(content, "process.printpdf", "terminal")
        content = _set_key(content, "printpdf.terminal.command", terminal_cmd)

        # Comentar printerpdf / apachepdfbox / desktop si existieran activos
        # (evitar conflicto con el modo terminal recién activado)
        for old_mode in ("apachepdfbox", "printerpdf", "desktop"):
            content = re.sub(
                rf"^(\s*process\.printpdf\s*=\s*{old_mode}.*)$",
                r"# \1",
                content,
                flags=re.MULTILINE,
            )

        props_path.write_text(content, encoding="utf-8")
        logger.info(
            "openbravohw.properties parcheado [OS=%s]: "
            "process.printpdf=terminal, staging=%s",
            platform.system(), staging_dir,
        )

    # ── Helpers internos ──────────────────────────────────────────────────

    def _resolve_start_script(self) -> Path:
        script = self._bin_dir / ("start.bat" if platform.system() == "Windows" else "start.sh")
        if not script.exists():
            raise FileNotFoundError(
                f"Script de arranque del HWM no encontrado: {script}\n"
                f"Verifica que HWM_DIR apunte al directorio 'bin'.\n"
                f"Docker: /opt/org.openbravo.retail.poshwmanager/bin\n"
                f"Local:  ruta al bin/ descomprimido del .zip del HWM."
            )
        return script

    def _build_command(self, script: Path) -> list[str]:
        if platform.system() == "Windows":
            return ["cmd.exe", "/c", str(script)]
        script.chmod(script.stat().st_mode | 0o111)
        return ["bash", str(script)]

    def _stdout_reader_loop(self) -> None:
        """Lee stdout del proceso en un hilo daemon → nunca bloquea el hilo principal."""
        if not self._process or not self._process.stdout:
            return
        try:
            for line in self._process.stdout:
                stripped = line.rstrip()
                if stripped:
                    try:
                        self._stdout_queue.put_nowait(stripped)
                    except queue.Full:
                        pass
        except Exception:  # noqa: BLE001
            pass

    def _wait_for_port(self) -> None:
        """
        Espera bloqueante hasta que el puerto 8090 acepta conexiones TCP.

        FIX 4 — Comportamiento cuando el proceso launcher termina:
          · Código 0  → Normal en Windows: start.bat lanza Java y sale.
                        Seguimos esperando el puerto (Java corre en background).
          · Código ≠ 0 → Error real del launcher. Abortamos inmediatamente.

        FIX 1 — En timeout: matar proceso PRIMERO, luego leer output.
        """
        deadline = time.monotonic() + self._timeout
        last_log_ts = time.monotonic()
        launcher_exited_ok = False

        logger.info(
            "Esperando que HWM levante el puerto %d (timeout=%ds)…",
            self._port, self._timeout,
        )

        while time.monotonic() < deadline:
            # ── Puerto listo ───────────────────────────────────────────────
            if self._is_port_open():
                return

            # ── Log periódico de stdout ────────────────────────────────────
            if time.monotonic() - last_log_ts >= 5:
                for line in self._drain_queue(max_lines=5):
                    logger.debug("[HWM] %s", line)
                last_log_ts = time.monotonic()

            # ── Comprobar si el launcher ha terminado ──────────────────────
            if self._process and self._process.poll() is not None:
                exit_code = self._process.returncode

                if exit_code == 0 and not launcher_exited_ok:
                    # FIX 4: start.bat salió limpio en Windows → Java sigue vivo
                    launcher_exited_ok = True
                    logger.info(
                        "Launcher start.bat terminó con código 0 "
                        "(comportamiento normal en Windows: Java sigue corriendo "
                        "como proceso independiente). Continuando espera del puerto %d…",
                        self._port,
                    )
                    self._process = None  # ya no tenemos handle; stop() será no-op

                elif exit_code != 0:
                    # Error real del launcher
                    output = self._drain_queue_all()
                    raise RuntimeError(
                        f"HWM terminó inesperadamente (código={exit_code}) "
                        f"antes de levantar el puerto {self._port}.\n"
                        f"Output:\n{output}"
                    )
                # Si launcher_exited_ok es True, continuamos el bucle

            time.sleep(_HWM_POLL_INTERVAL_S)

        # ── Timeout agotado ────────────────────────────────────────────────
        # FIX 1: matar proceso PRIMERO, luego drenar output (evita deadlock)
        if self._process:
            logger.warning("Timeout: matando proceso HWM…")
            self._process.kill()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            self._process = None

        time.sleep(0.2)  # pausa para que reader_thread drene el pipe
        output = self._drain_queue_all()

        raise TimeoutError(
            f"HWM no levantó el puerto {self._port} en {self._timeout}s.\n"
            f"Últimas líneas de output:\n{output}\n"
            f"Posibles causas:\n"
            f"  · JAVA_HOME no apunta a Java 17/21 "
            f"(actual: {os.environ.get('JAVA_HOME', 'no definido')})\n"
            f"  · El puerto {self._port} está ocupado por otro proceso\n"
            f"  · openbravohw.properties mal configurado en {self._bin_dir}\n"
            f"  · Memoria insuficiente (prueba aumentar -Xmx en JAVA_OPTS)"
        )

    def _is_port_open(self) -> bool:
        """
        Prueba 127.0.0.1 primero para evitar el bug de Windows donde
        'localhost' resuelve a ::1 pero Jetty solo escucha en IPv4.
        """
        for host in ("127.0.0.1", "::1", "localhost"):
            try:
                with socket.create_connection((host, self._port), timeout=0.5):
                    return True
            except OSError:
                continue
        return False

    def _drain_queue(self, max_lines: int = 10) -> list[str]:
        lines: list[str] = []
        for _ in range(max_lines):
            try:
                lines.append(self._stdout_queue.get_nowait())
            except queue.Empty:
                break
        return lines

    def _drain_queue_all(self) -> str:
        lines: list[str] = []
        while True:
            try:
                lines.append(self._stdout_queue.get_nowait())
            except queue.Empty:
                break
        return "\n".join(lines[-30:]) if lines else "(sin output disponible)"
