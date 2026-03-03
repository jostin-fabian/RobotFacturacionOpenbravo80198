"""
infrastructure/automation/playwright_engine.py
───────────────────────────────────────────────
SRP: única responsabilidad → automatizar la UI del OpenBravo POS.
No sabe nada de PostgreSQL, PDFs ni notificaciones.

PERSISTENCIA DE SESIÓN (solución al problema de Kestra sin volúmenes):
─────────────────────────────────────────────────────────────────────────────
  PlaywrightAutomationEngine recibe un ISessionRepository (inyectado por DIP).
  La implementación concreta es S3SessionRepository, que guarda el estado
  en S3 y lo restaura al arrancar. Sin volumes, sin estado local efímero.

  Run 1  (load() → None):
    Chromium arranca SIN cookies → Pantalla A → Link Device → login OK
    → context.storage_state() → session_repo.save(state) → JSON en S3

  Run 2+ (load() → dict con cookies):
    Chromium arranca CON cookies → Pantalla B directamente → login OK
    → session_repo.save(state)   → refresca el JSON en S3

DOS PANTALLAS DE ACCESO:
─────────────────────────────────────────────────────────────────────────────
  PANTALLA A – Terminal Selection (sin sesión previa):
    Detectar:  input[placeholder='Terminal Key Identifier'] visible
    Flujo:     fill Terminal Key + User + Password
               check "Log in with this user after linking"
               click "Link Device"
    Posible aviso: "already linked" → warning, continúa

  PANTALLA B – User Selection (sesión restaurada desde S3):
    Detectar:  ausencia del input Terminal Key Identifier
    Flujo:     click card usuario → fill Contraseña → "Iniciar sesión"

Post-login (ambas): spinner "Reading completely Product." → hidden
"""
from __future__ import annotations

import asyncio
from logger import get_logger
import os
from pathlib import Path

from playwright.async_api import (
    Browser,
    BrowserContext,
    Download,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

from domain.exceptions import InvalidCredentialsException, TerminalLockedException
from domain.models import InvoiceRecord
from infrastructure.automation.hwm_service import HardwareManagerService
from infrastructure.filesystem.file_manager import _BaseFileManager
from interfaces.protocols import ISessionRepository

logger = get_logger(__name__)

_T_NAV      =    60_000   # 1 min  – navegación HTTP
_T_ELEM     =    30_000   # 30 s   – aparición de elemento UI
_T_DOWNLOAD =    90_000   # 90 s   – descarga de PDF
_T_LOADING  =   120_000   # 2 min  – loading corto (sesión caliente)
_T_CATALOG  = 1_800_000   # 30 min – loading largo (primer run: catálogo de productos)
# NOTA: el catálogo de Escapa (preprod) tarda >10 min porque carga
# "Reading completely Product (N).." en múltiples pasadas de ~5 min c/u.


class PlaywrightAutomationEngine:
    """
    Implementa IAutomationEngine.
    Headless-ready: headless=True/False no cambia la lógica de negocio.
    """

    def __init__(
        self,
        ob_url: str,
        # ── Credenciales Pantalla A (Terminal Selection) ──────────────────
        terminal_key: str,
        terminal_username: str,
        terminal_password: str,
        # ── Credenciales Pantalla B (User Selection) ──────────────────────
        pos_username: str,
        pos_password: str,
        file_manager: _BaseFileManager,
        # ── DIP: repositorio de sesión inyectado ──────────────────────────
        session_repo: ISessionRepository,
        # ── DIP: HWM service (opcional, crea uno por defecto) ────────────
        # Arranca el proceso Java OpenBravo HWM (puerto 8090) ANTES de
        # lanzar el navegador. Sin él el POS muestra
        # "Printer and display are not available" y nunca genera el PDF.
        hwm_service: HardwareManagerService | None = None,
        headless: bool = True,
    ) -> None:
        self._ob_url            = ob_url
        self._terminal_key      = terminal_key
        self._terminal_username = terminal_username
        self._terminal_password = terminal_password
        self._pos_username      = pos_username
        self._pos_password      = pos_password
        self._fm                = file_manager
        self._session_repo      = session_repo
        self._hwm               = hwm_service or HardwareManagerService()
        self._headless          = headless

        self._playwright: Playwright | None     = None
        self._browser: Browser | None           = None
        self._context: BrowserContext | None    = None
        self._page: Page | None                 = None
        self._logged_in: bool                   = False

    # ── Ciclo de vida ─────────────────────────────────────────────────────

    async def start(self) -> None:
        logger.info("Arrancando Hardware Manager (HWM)…")
        self._hwm.start()
        logger.info("HWM listo. Lanzando Chromium…")

        self._playwright = await async_playwright().start()
        self._browser    = await self._playwright.chromium.launch(
            headless=self._headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-features=PrivateNetworkAccessPermissionPrompt",
                "--enable-features=PrivateNetworkAccessRespectPreflightResults",
                "--allow-running-insecure-content",
            ],
        )

        session_state = self._session_repo.load()
        context_kwargs: dict = {"accept_downloads": True, "locale": "es-ES"}
        if session_state:
            context_kwargs["storage_state"] = session_state
            logger.info("Sesión previa cargada desde S3. Chromium arrancará en Pantalla B.")
        else:
            logger.info("Sin sesión previa. Chromium arrancará en Pantalla A (primer run).")

        self._context = await self._browser.new_context(**context_kwargs)
        self._page    = await self._context.new_page()

        self._page.on("dialog", lambda d: asyncio.ensure_future(d.accept()))

        logger.info("Chromium lanzado (headless=%s).", self._headless)

    async def close(self) -> None:
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        logger.info("Navegador cerrado.")
        self._hwm.stop()

    # ── Sesión ────────────────────────────────────────────────────────────

    async def ensure_logged_in(self) -> None:
        """Re-login automático ante sesión expirada."""
        if self._logged_in and await self._session_alive():
            return
        await self._do_login()

    async def _session_alive(self) -> bool:
        # Si el spinner de catálogo está visible, el POS está arrancando:
        # NO interpretamos esto como sesión muerta (sería un falso positivo
        # que dispararía un bucle infinito de re-login).
        try:
            await self._page.wait_for_selector(
                "[data-testid='null-loadingContent']",
                state="visible",
                timeout=1_000,
            )
            logger.debug("Spinner de catálogo activo → sesión en proceso de carga, no es re-login.")
            return True
        except PlaywrightTimeoutError:
            pass

        try:
            await self._page.wait_for_selector(
                "[data-testid='obc2TerminalAuthenticationForm-button_submit'], "
                "[data-testid='obc2LoginForm-button_submit']",
                timeout=3_000,
            )
            logger.info("Pantalla de acceso detectada → re-login necesario.")
            self._logged_in = False
            return False
        except PlaywrightTimeoutError:
            return True

    async def _do_login(self) -> None:
        logger.info("Navegando al POS: %s", self._ob_url)
        await self._page.goto(self._ob_url, wait_until="domcontentloaded", timeout=_T_NAV)
        await self._wait_for_initial_loading()

        if await self._is_terminal_selection_screen():
            await self._login_terminal_selection()
            await self._dismiss_error_dialogs()

            if await self._is_user_selection_screen():
                logger.info("Pantalla B detectada tras link de terminal. Procediendo con user login.")
                await self._login_user_selection()
        else:
            await self._dismiss_error_dialogs()
            await self._login_user_selection()

        # ── Señal definitiva: esperar la UI principal del POS ────────────
        # NO usamos el spinner como señal (aparece y desaparece de forma
        # no determinista). En cambio esperamos OBPOS2_Orders, el botón
        # de la barra lateral que solo es visible cuando el catálogo ha
        # cargado completamente. Timeout = 30 min (catálogo preprod lento).
        await self._wait_for_pos_ready()

        self._logged_in = True
        logger.info("Login completado.")
        await self._persist_session()

    async def _dismiss_error_dialogs(self, max_reloads: int = 3) -> None:
        for attempt in range(1, max_reloads + 1):
            try:
                dialog = self._page.locator("[data-testid='obc2ConfirmDialog']")
                await dialog.wait_for(state="visible", timeout=2_000)
            except PlaywrightTimeoutError:
                return

            try:
                title = await self._page.locator(
                    "[data-testid='obc2ConfirmDialog-title']"
                ).inner_text(timeout=2_000)
            except PlaywrightTimeoutError:
                title = "desconocido"

            logger.warning(
                "Popup de error detectado (intento %d/%d): '%s'. Pulsando Reload…",
                attempt, max_reloads, title.strip(),
            )

            await self._page.locator("[data-testid='obc2ConfirmDialog-ok']").click()

            try:
                await dialog.wait_for(state="hidden", timeout=_T_ELEM)
            except PlaywrightTimeoutError:
                logger.warning("El popup no desapareció tras Reload. Continuando.")

            await self._wait_for_loading_complete(timeout=_T_CATALOG)

        logger.warning(
            "Popup de error persistió tras %d intentos de Reload. "
            "El POS puede no estar completamente cargado.",
            max_reloads,
        )

    async def _wait_for_pos_ready(self, timeout: int = _T_CATALOG) -> None:
        """
        Espera hasta que el botón principal del POS (OBPOS2_Orders) sea visible.

        Esta es la ÚNICA señal fiable de que el catálogo ha terminado de cargarse
        completamente. El spinner 'null-loadingContent' es unreliable porque:
          · Puede no aparecer inmediatamente → detección prematura de "POS listo"
          · Reaparece múltiples veces durante la carga del catálogo (pasadas sucesivas
            de "Reading completely Product (N)..")
          · En sesiones calientes puede desaparecer en < 2s pero el POS no estar listo

        Timeout: 30 min (catálogo preprod de Escapa puede tardar 20-30 min la
        primera vez; ejecuciones posteriores son más rápidas al haber caché).
        """
        _SEL_READY = "[data-testid='OBPOS2_Orders']"
        logger.info(
            "Esperando UI principal del POS (OBPOS2_Orders) — timeout=%d min. "
            "El catálogo puede tardar hasta 30 min en preprod…",
            timeout // 60_000,
        )
        start_ts = asyncio.get_event_loop().time()
        log_interval = 60  # loguear progreso cada 60 s

        while True:
            try:
                await self._page.wait_for_selector(
                    _SEL_READY, state="visible", timeout=min(log_interval * 1000, timeout)
                )
                elapsed = asyncio.get_event_loop().time() - start_ts
                logger.info("✅ POS listo (OBPOS2_Orders visible) tras %.0f segundos.", elapsed)
                return
            except PlaywrightTimeoutError:
                elapsed = asyncio.get_event_loop().time() - start_ts
                if elapsed * 1000 >= timeout:
                    raise TimeoutError(
                        f"POS no alcanzó el estado 'listo' (OBPOS2_Orders) "
                        f"en {timeout // 60_000} minutos. El catálogo puede estar "
                        f"bloqueado o las credenciales han expirado."
                    )
                # Loguear progreso y continuar esperando
                loading_text = ""
                try:
                    loading_text = await self._page.locator(
                        "[data-testid='null-loadingContent']"
                    ).inner_text(timeout=1_000)
                except PlaywrightTimeoutError:
                    pass
                logger.info(
                    "⏳ POS cargando… (%.0f min transcurridos)%s",
                    elapsed / 60,
                    f" — {loading_text.strip()[:80]}" if loading_text.strip() else "",
                )
                # Descartar popups de error mientras esperamos
                await self._dismiss_error_dialogs()

    async def _wait_for_initial_loading(self) -> None:
        try:
            await self._page.wait_for_selector(
                "[data-testid='obc2TerminalAuthenticationForm-button_submit'], "
                "[data-testid='obc2LoginForm-button_submit']",
                timeout=_T_LOADING,
            )
            logger.debug("Página lista: formulario de login detectado.")
        except PlaywrightTimeoutError:
            logger.warning(
                "No se detectó formulario de login en %ds. "
                "Continuando de todas formas.",
                _T_LOADING // 1000,
            )

    async def _wait_for_loading_complete(self, timeout: int = _T_LOADING) -> None:
        try:
            await self._page.wait_for_selector(
                "[data-testid='null-loadingContent']",
                timeout=_T_ELEM,
            )
            logger.debug(
                "Carga detectada (null-loadingContent). "
                "Esperando finalización (timeout=%ds)…",
                timeout // 1000,
            )
        except PlaywrightTimeoutError:
            logger.debug("Spinner de carga no detectado (POS ya listo).")
            return

        try:
            await self._page.wait_for_selector(
                "[data-testid='null-loadingContent']",
                state="hidden",
                timeout=timeout,
            )
            logger.debug("Carga completada.")
        except PlaywrightTimeoutError:
            logger.warning(
                "Carga todavía en progreso tras %ds. Continuando de todas formas.",
                timeout // 1000,
            )

    async def _is_terminal_selection_screen(self) -> bool:
        try:
            await self._page.wait_for_selector(
                "[data-testid='obc2TerminalAuthenticationForm-input_terminalKeyIdentifier-input']",
                timeout=4_000,
            )
            return True
        except PlaywrightTimeoutError:
            return False

    async def _is_user_selection_screen(self) -> bool:
        try:
            await self._page.wait_for_selector(
                "[data-testid='obc2LoginForm-button_submit']",
                timeout=4_000,
            )
            return True
        except PlaywrightTimeoutError:
            return False

    async def _login_terminal_selection(self) -> None:
        logger.info("Pantalla A detectada: Terminal Selection.")

        async def _check_login_error() -> None:
            banner = self._page.locator("[data-testid='obc2LoginForm-msgBox-message']")
            try:
                await banner.wait_for(state="visible", timeout=3_000)
                text = (await banner.inner_text()).strip()
                if "already linked" in text.lower():
                    logger.critical(
                        "Terminal %s BLOQUEADO: '%s'. Se requiere unlink manual en OpenBravo.",
                        self._terminal_key, text,
                    )
                    raise TerminalLockedException(self._terminal_key)
                if "invalid user name" in text.lower() or "invalid user" in text.lower():
                    logger.critical(
                        "Credenciales inválidas (usuario '%s') en terminal %s: '%s'.",
                        self._terminal_username, self._terminal_key, text,
                    )
                    raise InvalidCredentialsException(self._terminal_key, self._terminal_username)
                logger.error("Banner de error desconocido en Terminal Selection: '%s'", text)
                raise RuntimeError(f"Error desconocido en Terminal Selection: {text!r}")
            except PlaywrightTimeoutError:
                pass

        await _check_login_error()

        await self._fill(
            "[data-testid='obc2TerminalAuthenticationForm-input_terminalKeyIdentifier-input']",
            self._terminal_key,
        )
        await self._fill(
            "[data-testid='obc2LoginForm-input_user-input']",
            self._terminal_username,
        )
        await self._fill(
            "[data-testid='obc2LoginForm-input_password-input']",
            self._terminal_password,
        )

        checkbox = self._page.locator("#isLinkAndLogin")
        if await checkbox.count() and not await checkbox.is_checked():
            await checkbox.check()
            logger.debug("Checkbox 'Log in with this user after linking' marcado.")

        await self._page.locator(
            "[data-testid='obc2TerminalAuthenticationForm-button_submit']"
        ).click()
        logger.info("Pantalla A: 'Link Device' pulsado.")

        await _check_login_error()

    _SEL_POPUP    = "[data-testid='obc2ConfirmDialog']"
    _SEL_SPINNER  = "[data-testid='null-loadingContent']"

    async def _wait_for_selector_or_popup(
        self,
        selector: str,
        timeout: int = _T_ELEM,
    ) -> str:
        combined = f"{selector}, {self._SEL_POPUP}"
        try:
            el = await self._page.wait_for_selector(combined, timeout=timeout)
            if el is None:
                return "ready"
            test_id = await el.get_attribute("data-testid") or ""
            if "ConfirmDialog" in test_id:
                return "popup"
            return "ready"
        except PlaywrightTimeoutError:
            return "ready"

    async def _wait_for_spinner_gone(self, timeout: int = _T_CATALOG) -> None:
        try:
            await self._page.wait_for_selector(
                self._SEL_SPINNER, state="visible", timeout=2_000
            )
        except PlaywrightTimeoutError:
            logger.debug("Spinner no detectado: POS listo para interacción.")
            return

        logger.info(
            "Spinner de catálogo visible. Esperando finalización "
            "(timeout=%d min). El catálogo puede tardar >10 min en preprod.",
            timeout // 60_000,
        )
        start_ts = asyncio.get_event_loop().time()
        while True:
            try:
                await self._page.wait_for_selector(
                    self._SEL_SPINNER, state="hidden", timeout=min(60_000, timeout)
                )
                elapsed = asyncio.get_event_loop().time() - start_ts
                logger.info("Catálogo cargado tras %.0f segundos.", elapsed)
                return
            except PlaywrightTimeoutError:
                elapsed = asyncio.get_event_loop().time() - start_ts
                elapsed_ms = int(elapsed * 1000)
                if elapsed_ms >= timeout:
                    logger.warning(
                        "Spinner de catálogo sigue activo tras %d min. "
                        "Continuando de todas formas (puede haber interacción parcial).",
                        timeout // 60_000,
                    )
                    return
                loading_text = ""
                try:
                    loading_text = await self._page.locator(
                        "[data-testid='null-loadingContent']"
                    ).inner_text(timeout=1_000)
                except PlaywrightTimeoutError:
                    pass
                logger.debug(
                    "Carga en progreso (%d min transcurridos): %s",
                    int(elapsed // 60), loading_text.strip()[:60],
                )
                await self._dismiss_error_dialogs()

    async def _login_user_selection(self) -> None:
        _MAX_POPUP_RETRIES = 5
        _SEL_KEYMAP = "[data-testid='obc2UserAvatarLoginKeymap-keymap']"
        _SEL_PWD = "[data-testid='obc2LoginForm-input_password-input']"

        logger.info("Pantalla B detectada: User Selection.")

        await self._wait_for_spinner_gone(timeout=_T_CATALOG)

        # ── Comprobar si seguimos en Pantalla B ───────────────────────────
        # Cuando Pantalla A se usa con "Log in with this user after linking",
        # el POS loguea al usuario automáticamente durante la carga del catálogo.
        # Al terminar el spinner, la UI principal ya está activa → no hay cards.
        still_on_b = await self._is_user_selection_screen()
        if not still_on_b:
            logger.info(
                "Pantalla B ya no visible tras carga del catálogo. "
                "Usuario logueado automáticamente vía 'Link and Log In'. Continuando."
            )
            return

        for attempt in range(1, _MAX_POPUP_RETRIES + 1):
            result = await self._wait_for_selector_or_popup(_SEL_KEYMAP, timeout=_T_ELEM)
            if result == "popup":
                logger.warning(
                    "Popup detectado esperando keymap (intento %d/%d). Dismissing…",
                    attempt, _MAX_POPUP_RETRIES,
                )
                await self._dismiss_error_dialogs()
                continue
            break

        user_card = self._page.locator(
            "[data-testid~='obc2UserAvatarLogin-container']"
        ).filter(
            has=self._page.locator(
                "[data-testid='obc2UserLabel']",
                has_text=self._pos_username,
            )
        ).first

        card_clicked = False
        for attempt in range(1, _MAX_POPUP_RETRIES + 1):
            await self._dismiss_error_dialogs()

            try:
                await user_card.wait_for(state="visible", timeout=_T_ELEM)
                await user_card.click()
                logger.debug("Card '%s' clicada (intento %d).", self._pos_username, attempt)
                card_clicked = True
                break
            except PlaywrightTimeoutError:
                popup_visible = await self._page.locator(self._SEL_POPUP).is_visible()
                if popup_visible:
                    logger.warning(
                        "Popup interceptó espera de card (intento %d/%d). Dismissing…",
                        attempt, _MAX_POPUP_RETRIES,
                    )
                    await self._dismiss_error_dialogs()
                    continue
                break

        if not card_clicked:
            logger.warning(
                "Card '%s' no encontrada por selector exacto. "
                "Intentando match parcial en todas las cards.",
                self._pos_username,
            )
            await self._dismiss_error_dialogs()

            all_cards = self._page.locator("[data-testid~='obc2UserAvatarLogin-container']")
            count = await all_cards.count()
            logger.debug("Cards disponibles en Pantalla B: %d", count)

            users_found: list[str] = []
            for i in range(count):
                card = all_cards.nth(i)

                try:
                    text = (await card.locator(
                        "[data-testid='obc2UserLabel']"
                    ).inner_text(timeout=1_000)).strip()
                except PlaywrightTimeoutError:
                    continue

                users_found.append(text)

                if self._pos_username.lower() not in text.lower():
                    continue

                try:
                    await card.click()
                    logger.debug("Card encontrada por match parcial y clicada: '%s'", text)
                    card_clicked = True
                    break
                except PlaywrightTimeoutError:
                    logger.warning(
                        "Click interceptado en card '%s'. "
                        "Esperando spinner y reintentando…", text
                    )
                    await self._wait_for_spinner_gone(timeout=_T_CATALOG)
                    await self._dismiss_error_dialogs()
                    try:
                        await card.click()
                        logger.debug("Card '%s' clicada tras esperar spinner.", text)
                        card_clicked = True
                        break
                    except PlaywrightTimeoutError as exc:
                        raise RuntimeError(
                            f"Card '{text}' encontrada en Pantalla B pero click "
                            f"bloqueado incluso tras esperar el spinner. "
                            f"Puede haber otro overlay activo."
                        ) from exc

            if not card_clicked:
                raise RuntimeError(
                    f"Usuario '{self._pos_username}' no encontrado en Pantalla B. "
                    f"Usuarios disponibles: {users_found}"
                )

        for attempt in range(1, _MAX_POPUP_RETRIES + 1):
            result = await self._wait_for_selector_or_popup(_SEL_PWD, timeout=_T_ELEM)
            if result == "popup":
                logger.warning(
                    "Popup detectado esperando campo de contraseña (intento %d/%d). "
                    "Dismissing y re-clicando card…",
                    attempt, _MAX_POPUP_RETRIES,
                )
                await self._dismiss_error_dialogs()
                try:
                    await user_card.click(timeout=_T_ELEM)
                except PlaywrightTimeoutError:
                    pass
                continue
            break

        await self._dismiss_error_dialogs()

        pwd_input = self._page.locator(_SEL_PWD)
        await pwd_input.wait_for(state="visible", timeout=_T_ELEM)
        await pwd_input.click(click_count=3)
        await pwd_input.fill(self._pos_password)

        await self._page.locator("[data-testid='obc2LoginForm-button_submit']").click()
        logger.info("Pantalla B: 'Log In' pulsado.")

    async def _persist_session(self) -> None:
        try:
            state = await self._context.storage_state()
            self._session_repo.save(state)
        except Exception as exc:  # noqa: BLE001
            logger.warning("No se pudo persistir la sesión: %s", exc)

    async def _fill(self, selector: str, value: str) -> None:
        loc = self._page.locator(selector).first
        await loc.wait_for(state="visible", timeout=_T_ELEM)
        await loc.click(click_count=3)
        await loc.fill(value)

    # ── Generación de PDF ─────────────────────────────────────────────────

    async def generate_pdf(self, invoice: InvoiceRecord, dest_path: Path) -> Path:
        await self.ensure_logged_in()
        os.environ["PDF_OUT_PATH"] = str(dest_path)
        await self._navigate_to_order(invoice)
        await self._open_print_dialog()
        await self._select_invoice_only()
        return await self._capture_download(dest_path)

    # ── Paso 1: Navegación al pedido ──────────────────────────────────────

    async def _navigate_to_order(self, invoice: InvoiceRecord) -> None:
        order_no = invoice.order_document_no or invoice.document_no
        logger.info("Navegando a pedido: %s", order_no)

        await self._page.locator("[data-testid='OBPOS2_Orders']").click()
        logger.debug("Sidebar OBPOS2_Orders clickado.")

        grid = self._page.locator("[data-testid='obpos2TicketListRemoteGrid']")
        await grid.wait_for(state="visible", timeout=_T_ELEM)

        filter_input = self._page.locator(
            "[data-testid='obc2FilterBarInput-documentNo-input']"
        )
        await filter_input.wait_for(state="visible", timeout=_T_ELEM)

        # ──────────────────────────────────────────────────────────────────
        # BUG CORREGIDO: triple_click() NO existe en Playwright Python.
        # ✗  await filter_input.triple_click()   → AttributeError
        # ✓  await filter_input.fill(order_no)   → limpia y escribe directo
        # ──────────────────────────────────────────────────────────────────
        await filter_input.fill(order_no)
        logger.debug("Filtro 'Order No' rellenado con: %s", order_no)

        cell_doc_no = self._page.locator(
            "[data-testid^='obpos2TicketListRemoteGrid-dataGrid-cell-']"
            "[data-testid$='-documentNo']"
        ).filter(has_text=order_no)

        try:
            await cell_doc_no.first.wait_for(state="visible", timeout=_T_ELEM)
        except PlaywrightTimeoutError:
            raise PlaywrightTimeoutError(
                f"No apareció ninguna fila con documentNo='{order_no}' "
                f"tras filtrar en la vista Orders."
            )

        matched_cell = None
        all_cells = await cell_doc_no.all()
        for cell in all_cells:
            text = (await cell.inner_text()).strip()
            if text == order_no:
                matched_cell = cell
                break

        if matched_cell is None:
            logger.warning(
                "No hay match exacto para '%s', usando primer resultado.", order_no
            )
            matched_cell = all_cells[0]

        row_container = matched_cell.locator(
            "xpath=ancestor::div[contains(@class,'obc2DataGrid-rowContainer')]"
        )
        await row_container.click()
        logger.debug("Fila del pedido '%s' clickada.", order_no)

        clear_btn = self._page.locator(
            "[data-testid='obc2FilterBarInput-documentNo-clearText']"
        )
        try:
            await clear_btn.wait_for(state="visible", timeout=3_000)
            await clear_btn.click()
            logger.debug("Filtro 'Order No' limpiado.")
        except PlaywrightTimeoutError:
            pass

        ticket_header = self._page.locator(
            "[data-testid='obpos2ButtonBar1-ticketHeaderDocumentnoButton']"
        )
        try:
            await ticket_header.wait_for(state="visible", timeout=_T_ELEM)
            header_text = await ticket_header.inner_text()
            logger.info("Pedido cargado correctamente: %s", header_text.strip())
        except PlaywrightTimeoutError:
            logger.warning(
                "No se confirmó la carga del ticket vía header "
                "(puede ser normal según el tipo de pedido). Continuando."
            )

    # ── Paso 2: Abrir el diálogo Print/Email Duplicate ────────────────────

    async def _open_print_dialog(self) -> None:
        print_btn = self._page.locator(
            "[data-testid='obpos2ButtonBar2-printBookedTicket']"
        )
        await print_btn.wait_for(state="visible", timeout=_T_ELEM)
        await print_btn.click()
        logger.debug("Botón 'Print / Email Duplicate' clickado.")

        dialog = self._page.locator("[data-testid='obc2InputDataDialog']")
        await dialog.wait_for(state="visible", timeout=_T_ELEM)
        logger.debug("Diálogo 'Print / Email Duplicate' abierto.")

    # ── Paso 3: Seleccionar solo Invoice en el diálogo ────────────────────

    async def _select_invoice_only(self) -> None:
        _CB_INVOICE = (
            "[data-testid='obpos2BookedDeliveryOptionsDialog"
            "-singleInvoiceCheckBoxGroup-ticketInvoice-checkbox']"
        )
        _CB_GIFT = (
            "[data-testid='obpos2BookedDeliveryOptionsDialog"
            "-singleInvoiceCheckBoxGroup-giftReceipt-checkbox']"
        )
        _CB_ORDER = (
            "[data-testid='obpos2BookedDeliveryOptionsDialog"
            "-singleInvoiceCheckBoxGroup-receipt-checkbox']"
        )
        _LBL_INVOICE = (
            "[data-testid='obpos2BookedDeliveryOptionsDialog"
            "-singleInvoiceCheckBoxGroup-ticketInvoice-label']"
        )
        _LBL_GIFT = (
            "[data-testid='obpos2BookedDeliveryOptionsDialog"
            "-singleInvoiceCheckBoxGroup-giftReceipt-label']"
        )
        _LBL_ORDER = (
            "[data-testid='obpos2BookedDeliveryOptionsDialog"
            "-singleInvoiceCheckBoxGroup-receipt-label']"
        )

        async def _is_checked(cb_selector: str) -> bool:
            cb_el = self._page.locator(cb_selector)
            try:
                checked_icon = cb_el.locator("[data-testid='CheckBoxIcon']")
                return await checked_icon.count() > 0
            except Exception:  # noqa: BLE001
                return False

        async def _ensure_checked(cb_sel: str, lbl_sel: str, should_be: bool) -> None:
            current = await _is_checked(cb_sel)
            if current != should_be:
                await self._page.locator(lbl_sel).click()
                logger.debug(
                    "Checkbox %s → %s",
                    cb_sel.split("-")[-2],
                    "marcado" if should_be else "desmarcado",
                )

        await _ensure_checked(_CB_INVOICE, _LBL_INVOICE, should_be=True)
        await _ensure_checked(_CB_GIFT, _LBL_GIFT, should_be=False)
        await _ensure_checked(_CB_ORDER, _LBL_ORDER, should_be=False)

        logger.debug("Checkboxes configurados: Invoice=ON, GiftReceipt=OFF, Order=OFF")

    # ── Paso 4: Capturar el PDF via staging folder ────────────────────────
    #
    # CONTEXTO: openbravohw.properties tiene:
    #   process.printpdf = terminal
    #   printpdf.terminal.command = cmd /c move "%1" "C:\temp\ob_staging\"
    #
    # El HWM genera el PDF internamente (Apache PDFBox) y lo mueve a
    # C:\temp\ob_staging\ sin mostrar ningún diálogo de Windows.
    # El robot simplemente espera a que aparezca un .pdf nuevo en esa carpeta.

    async def _capture_download(self, dest_path: Path) -> Path:
        """
        Click Print → HWM genera PDF en C:\\temp\\ob_staging\\ → robot lo detecta
        y lo mueve a dest_path.

        El diálogo "Guardar impresión como" de Windows NO aparece porque
        process.printpdf = terminal en openbravohw.properties.
        """
        print_btn = self._page.locator("[data-testid='obc2InputDataDialog-print']")
        await print_btn.wait_for(state="visible", timeout=_T_ELEM)

        # Carpeta staging donde el HWM deposita el PDF
        staging_dir = dest_path.parent
        staging_dir.mkdir(parents=True, exist_ok=True)

        # Snapshot de PDFs ANTES de click → para detectar el nuevo
        before: set[Path] = set(staging_dir.glob("*.pdf"))
        ts_before = asyncio.get_event_loop().time()

        logger.info("Clickando 'Print'. HWM moverá el PDF a %s…", staging_dir)
        await print_btn.click()

        # Esperar el nuevo PDF en staging (el HWM lo deposita en segundos)
        pdf_in_staging = await self._wait_new_pdf_in_staging(
            staging_dir, before, ts_before, timeout_s=90
        )

        # Renombrar al nombre canónico esperado por el orquestador
        pdf_in_staging.rename(dest_path)
        logger.info("PDF movido de staging a destino: %s", dest_path)

        # Cerrar el diálogo si sigue abierto
        try:
            close_btn = self._page.locator("[data-testid='obc2InputDataDialog-closeButton']")
            if await close_btn.count():
                await close_btn.click()
        except Exception:  # noqa: BLE001
            pass

        return dest_path

    @staticmethod
    async def _wait_new_pdf_in_staging(
        staging_dir: Path,
        before: set[Path],
        ts_before: float,
        timeout_s: int = 90,
    ) -> Path:
        """
        Espera hasta `timeout_s` segundos a que aparezca un PDF nuevo en
        `staging_dir` que no estuviera en `before`.

        El HWM usa 'cmd /c move "%1" "C:\\temp\\ob_staging\\"' por lo que
        el archivo aparece ya completo (move es atómico en Windows en el
        mismo volumen).
        """
        deadline = asyncio.get_event_loop().time() + timeout_s
        while asyncio.get_event_loop().time() < deadline:
            current: set[Path] = set(staging_dir.glob("*.pdf"))
            new_files = current - before
            if new_files:
                # Si hay varios nuevos (reintento), tomar el más reciente
                newest = max(new_files, key=lambda p: p.stat().st_mtime)
                if newest.stat().st_size > 0:
                    logger.info(
                        "PDF nuevo detectado en staging: %s (%.1f KB)",
                        newest.name,
                        newest.stat().st_size / 1024,
                    )
                    return newest
            await asyncio.sleep(0.5)

        raise TimeoutError(
            f"El HWM no depositó ningún PDF nuevo en {staging_dir} "
            f"tras {timeout_s}s. Comprueba openbravohw.properties: "
            f"process.printpdf = terminal y printpdf.terminal.command."
        )
