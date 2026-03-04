"""
infrastructure/automation/playwright_engine.py
───────────────────────────────────────────────
SRP: unica responsabilidad -> automatizar la UI del OpenBravo POS.

PERSISTENCIA DE PERFIL CON launch_persistent_context + S3:
  start():
    1. profile_repo.download_profile(local_dir) -> descomprime en local
    2. launch_persistent_context(user_data_dir=local_dir)
    3. Login si es necesario
  close():
    1. context.close() -> Chromium vuelca el perfil a local_dir
    2. profile_repo.upload_profile(local_dir) -> comprime y sube a S3

PATH CROSS-PLATFORM SEGMENTADO POR ENTORNO:
  Windows: %LOCALAPPDATA%\QuadisRobot\chromium_profile_{env_lower}
  Linux:   /tmp/quadis_robot/chromium_profile_{env_lower}

FIX 1: Selector dual para el boton Print.
  obpos2ButtonBar2-printBookedTicket -> pedidos normales
  obpos2ButtonBar2-printBill        -> devoluciones / abonos

FIX 2: Timeout _T_ELEM (30s) al esperar Print tras detectar el header.
  Antes 5s hardcodeado, insuficiente para pedidos de devolucion.
"""
from __future__ import annotations

import asyncio
from logger import get_logger
import os
import platform
from pathlib import Path

from playwright.async_api import (
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

from domain.exceptions import InvalidCredentialsException, TerminalLockedException
from domain.models import InvoiceRecord
from infrastructure.automation.hwm_service import HardwareManagerService
from infrastructure.filesystem.file_manager import _BaseFileManager
from infrastructure.persistence.s3_profile_repository import S3ProfileRepository

logger = get_logger(__name__)

_T_NAV = 60_000
_T_ELEM = 30_000
_T_DOWNLOAD = 90_000
_T_LOADING = 120_000
_T_CATALOG = 1_800_000

_SEL_PRINT_BTN = (
    "[data-testid='obpos2ButtonBar2-printBookedTicket'], "
    "[data-testid='obpos2ButtonBar2-printBill']"
)


def _resolve_user_data_dir(environment: str, override: Path | None) -> Path:
    if override:
        return Path(override)
    env_lower = environment.lower()
    profile_name = f"chromium_profile_{env_lower}"
    if platform.system() == "Windows":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
        return base / "QuadisRobot" / profile_name
    return Path("/tmp/quadis_robot") / profile_name


class PlaywrightAutomationEngine:

    def __init__(
            self,
            ob_url: str,
            terminal_key: str,
            terminal_username: str,
            terminal_password: str,
            pos_username: str,
            pos_password: str,
            file_manager: _BaseFileManager,
            environment: str = "Development",
            profile_repo: S3ProfileRepository | None = None,
            user_data_dir: Path | None = None,
            hwm_service: HardwareManagerService | None = None,
            headless: bool = True,
    ) -> None:
        self._ob_url = ob_url
        self._terminal_key = terminal_key
        self._terminal_username = terminal_username
        self._terminal_password = terminal_password
        self._pos_username = pos_username
        self._pos_password = pos_password
        self._fm = file_manager
        self._environment = environment
        self._profile_repo = profile_repo
        self._user_data_dir = _resolve_user_data_dir(environment, user_data_dir)
        self._hwm = hwm_service or HardwareManagerService()
        self._headless = headless

        self._playwright: Playwright | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._logged_in: bool = False

    async def start(self) -> None:
        logger.info("Arrancando Hardware Manager (HWM)...")
        self._hwm.start()
        logger.info("HWM listo. Preparando perfil de Chromium...")

        self._user_data_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            "user_data_dir: '%s' (SO: %s, entorno: %s)",
            self._user_data_dir, platform.system(), self._environment,
        )

        if self._profile_repo:
            profile_restored = self._profile_repo.download_profile(self._user_data_dir)
            if not profile_restored:
                logger.info(
                    "PRIMER RUN [%s]: master data se descargara ahora (~20-30 min). "
                    "Los runs posteriores arrancaran en segundos.",
                    self._environment,
                )
            else:
                logger.info(
                    "Perfil [%s] restaurado desde S3. Arranque rapido esperado.",
                    self._environment,
                )
        else:
            if not any(self._user_data_dir.iterdir()):
                logger.info(
                    "Sin S3ProfileRepository y perfil vacio [%s]. "
                    "PRIMER RUN: master data se descargara ahora (~20-30 min).",
                    self._environment,
                )
            else:
                logger.info("Perfil local [%s] existente. Arranque rapido.", self._environment)

        logger.info("Lanzando Chromium (persistent_context)...")
        self._playwright = await async_playwright().start()

        self._context = await self._playwright.chromium.launch_persistent_context(
            user_data_dir=str(self._user_data_dir),
            headless=self._headless,
            accept_downloads=True,
            locale="es-ES",
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-features=PrivateNetworkAccessPermissionPrompt",
                "--enable-features=PrivateNetworkAccessRespectPreflightResults",
                "--allow-running-insecure-content",
            ],
        )

        pages = self._context.pages
        self._page = pages[0] if pages else await self._context.new_page()
        self._page.on("dialog", lambda d: asyncio.ensure_future(d.accept()))

        logger.info(
            "Chromium lanzado (headless=%s, env=%s, user_data_dir=%s).",
            self._headless, self._environment, self._user_data_dir,
        )

    async def close(self) -> None:
        if self._context:
            await self._context.close()
            logger.info("Navegador cerrado. Perfil volcado a '%s'.", self._user_data_dir)
        if self._playwright:
            await self._playwright.stop()
        if self._profile_repo:
            try:
                self._profile_repo.upload_profile(self._user_data_dir)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "No se pudo subir el perfil [%s] a S3: %s. "
                    "El proximo run tendra que descargar el master data de nuevo.",
                    self._environment, exc,
                )
        self._hwm.stop()

    async def ensure_logged_in(self) -> None:
        if self._logged_in and await self._session_alive():
            return
        await self._do_login()

    async def _session_alive(self) -> bool:
        try:
            await self._page.wait_for_selector(
                "[data-testid='null-loadingContent']", state="visible", timeout=1_000
            )
            logger.debug("Spinner activo -> sesion en carga, no es re-login.")
            return True
        except PlaywrightTimeoutError:
            pass
        try:
            await self._page.wait_for_selector(
                "[data-testid='obc2TerminalAuthenticationForm-button_submit'], "
                "[data-testid='obc2LoginForm-button_submit']",
                timeout=3_000,
            )
            logger.info("Pantalla de acceso detectada -> re-login necesario.")
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
                logger.info("Pantalla B detectada tras link de terminal.")
                await self._login_user_selection()
        else:
            await self._dismiss_error_dialogs()
            await self._login_user_selection()

        await self._wait_for_pos_ready()
        self._logged_in = True
        logger.info("Login completado.")

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
                "Popup de error (intento %d/%d): '%s'. Pulsando Reload...",
                attempt, max_reloads, title.strip(),
            )
            await self._page.locator("[data-testid='obc2ConfirmDialog-ok']").click()
            try:
                await dialog.wait_for(state="hidden", timeout=_T_ELEM)
            except PlaywrightTimeoutError:
                logger.warning("Popup no desaparecio tras Reload. Continuando.")
            await self._wait_for_loading_complete(timeout=_T_CATALOG)
        logger.warning(
            "Popup persistio tras %d intentos. POS puede no estar completamente cargado.",
            max_reloads,
        )

    async def _wait_for_pos_ready(self, timeout: int = _T_CATALOG) -> None:
        _SEL_READY = "[data-testid='OBPOS2_Orders']"
        logger.info(
            "Esperando UI principal del POS (OBPOS2_Orders) - timeout=%d min...",
            timeout // 60_000,
        )
        start_ts = asyncio.get_event_loop().time()
        while True:
            try:
                await self._page.wait_for_selector(
                    _SEL_READY, state="visible", timeout=min(60_000, timeout)
                )
                elapsed = asyncio.get_event_loop().time() - start_ts
                logger.info("POS listo tras %.0f segundos.", elapsed)
                return
            except PlaywrightTimeoutError:
                elapsed = asyncio.get_event_loop().time() - start_ts
                if elapsed * 1000 >= timeout:
                    raise TimeoutError(f"POS no listo tras {timeout // 60_000} min.")
                loading_text = ""
                try:
                    loading_text = await self._page.locator(
                        "[data-testid='null-loadingContent']"
                    ).inner_text(timeout=1_000)
                except PlaywrightTimeoutError:
                    pass
                logger.info(
                    "POS cargando... (%.0f min)%s",
                    elapsed / 60,
                    f" - {loading_text.strip()[:80]}" if loading_text.strip() else "",
                )
                await self._dismiss_error_dialogs()

    async def _wait_for_initial_loading(self) -> None:
        try:
            await self._page.wait_for_selector(
                "[data-testid='obc2TerminalAuthenticationForm-button_submit'], "
                "[data-testid='obc2LoginForm-button_submit']",
                timeout=_T_LOADING,
            )
            logger.debug("Formulario de login detectado.")
        except PlaywrightTimeoutError:
            logger.warning(
                "No se detecto formulario de login en %ds. "
                "Continuando (puede que la sesion ya este activa).",
                _T_LOADING // 1000,
            )

    async def _wait_for_loading_complete(self, timeout: int = _T_LOADING) -> None:
        try:
            await self._page.wait_for_selector(
                "[data-testid='null-loadingContent']", timeout=_T_ELEM
            )
        except PlaywrightTimeoutError:
            logger.debug("Spinner no detectado (POS ya listo).")
            return
        try:
            await self._page.wait_for_selector(
                "[data-testid='null-loadingContent']", state="hidden", timeout=timeout
            )
        except PlaywrightTimeoutError:
            logger.warning("Carga en progreso tras %ds. Continuando.", timeout // 1000)

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
                "[data-testid='obc2LoginForm-button_submit']", timeout=4_000
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
                        "Terminal %s BLOQUEADO: '%s'. Se requiere unlink manual.",
                        self._terminal_key, text,
                    )
                    raise TerminalLockedException(self._terminal_key)
                if "invalid user name" in text.lower() or "invalid user" in text.lower():
                    logger.critical(
                        "Credenciales invalidas (usuario '%s') en terminal %s: '%s'.",
                        self._terminal_username, self._terminal_key, text,
                    )
                    raise InvalidCredentialsException(self._terminal_key, self._terminal_username)
                logger.error("Banner de error desconocido: '%s'", text)
                raise RuntimeError(f"Error desconocido en Terminal Selection: {text!r}")
            except PlaywrightTimeoutError:
                pass

        await _check_login_error()
        await self._fill(
            "[data-testid='obc2TerminalAuthenticationForm-input_terminalKeyIdentifier-input']",
            self._terminal_key,
        )
        await self._fill("[data-testid='obc2LoginForm-input_user-input']", self._terminal_username)
        await self._fill("[data-testid='obc2LoginForm-input_password-input']", self._terminal_password)

        checkbox = self._page.locator("#isLinkAndLogin")
        if await checkbox.count() and not await checkbox.is_checked():
            await checkbox.check()
            logger.debug("Checkbox 'Log in with this user after linking' marcado.")

        await self._page.locator(
            "[data-testid='obc2TerminalAuthenticationForm-button_submit']"
        ).click()
        logger.info("Pantalla A: 'Link Device' pulsado.")
        await _check_login_error()

    _SEL_POPUP = "[data-testid='obc2ConfirmDialog']"
    _SEL_SPINNER = "[data-testid='null-loadingContent']"

    async def _wait_for_selector_or_popup(self, selector: str, timeout: int = _T_ELEM) -> str:
        combined = f"{selector}, {self._SEL_POPUP}"
        try:
            el = await self._page.wait_for_selector(combined, timeout=timeout)
            if el is None:
                return "ready"
            test_id = await el.get_attribute("data-testid") or ""
            return "popup" if "ConfirmDialog" in test_id else "ready"
        except PlaywrightTimeoutError:
            return "ready"

    async def _wait_for_spinner_gone(self, timeout: int = _T_CATALOG) -> None:
        try:
            await self._page.wait_for_selector(
                self._SEL_SPINNER, state="visible", timeout=2_000
            )
        except PlaywrightTimeoutError:
            logger.debug("Spinner no detectado: POS listo.")
            return
        logger.info(
            "Spinner visible. Esperando finalizacion (timeout=%d min).", timeout // 60_000
        )
        start_ts = asyncio.get_event_loop().time()
        while True:
            try:
                await self._page.wait_for_selector(
                    self._SEL_SPINNER, state="hidden", timeout=min(60_000, timeout)
                )
                elapsed = asyncio.get_event_loop().time() - start_ts
                logger.info("Spinner desaparecio tras %.0f segundos.", elapsed)
                return
            except PlaywrightTimeoutError:
                elapsed = asyncio.get_event_loop().time() - start_ts
                if int(elapsed * 1000) >= timeout:
                    logger.warning(
                        "Spinner sigue activo tras %d min. Continuando.", timeout // 60_000
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
                    "Carga en progreso (%d min): %s",
                    int(elapsed // 60), loading_text.strip()[:60],
                )
                await self._dismiss_error_dialogs()

    async def _login_user_selection(self) -> None:
        _MAX = 5
        _SEL_KEYMAP = "[data-testid='obc2UserAvatarLoginKeymap-keymap']"
        _SEL_PWD = "[data-testid='obc2LoginForm-input_password-input']"

        logger.info("Pantalla B detectada: User Selection.")
        await self._wait_for_spinner_gone(timeout=_T_CATALOG)

        if not await self._is_user_selection_screen():
            logger.info(
                "Pantalla B ya no visible tras carga del catalogo. "
                "Usuario logueado automaticamente via 'Link and Log In'. Continuando."
            )
            return

        for attempt in range(1, _MAX + 1):
            result = await self._wait_for_selector_or_popup(_SEL_KEYMAP, timeout=_T_ELEM)
            if result == "popup":
                logger.warning(
                    "Popup detectado esperando keymap (intento %d/%d). Dismissing...",
                    attempt, _MAX,
                )
                await self._dismiss_error_dialogs()
                continue
            break

        user_card = self._page.locator(
            "[data-testid~='obc2UserAvatarLogin-container']"
        ).filter(
            has=self._page.locator(
                "[data-testid='obc2UserLabel']", has_text=self._pos_username
            )
        ).first

        card_clicked = False
        for attempt in range(1, _MAX + 1):
            await self._dismiss_error_dialogs()
            try:
                await user_card.wait_for(state="visible", timeout=_T_ELEM)
                await user_card.click()
                logger.debug("Card '%s' clicada (intento %d).", self._pos_username, attempt)
                card_clicked = True
                break
            except PlaywrightTimeoutError:
                if await self._page.locator(self._SEL_POPUP).is_visible():
                    logger.warning(
                        "Popup intercepto espera de card (intento %d/%d). Dismissing...",
                        attempt, _MAX,
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
                    logger.debug("Card encontrada por match parcial: '%s'.", text)
                    card_clicked = True
                    break
                except PlaywrightTimeoutError:
                    logger.warning("Click interceptado en card '%s'. Reintentando...", text)
                    await self._wait_for_spinner_gone(timeout=_T_CATALOG)
                    await self._dismiss_error_dialogs()
                    await card.click()
                    logger.debug("Card '%s' clicada tras esperar spinner.", text)
                    card_clicked = True
                    break
            if not card_clicked:
                raise RuntimeError(
                    f"Usuario '{self._pos_username}' no encontrado en Pantalla B. "
                    f"Usuarios disponibles: {users_found}"
                )

        for attempt in range(1, _MAX + 1):
            result = await self._wait_for_selector_or_popup(_SEL_PWD, timeout=_T_ELEM)
            if result == "popup":
                logger.warning(
                    "Popup detectado esperando contrasena (intento %d/%d). "
                    "Dismissing y re-clicando card...",
                    attempt, _MAX,
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

    async def _fill(self, selector: str, value: str) -> None:
        loc = self._page.locator(selector).first
        await loc.wait_for(state="visible", timeout=_T_ELEM)
        await loc.click(click_count=3)
        await loc.fill(value)

    async def generate_pdf(self, invoice: InvoiceRecord, dest_path: Path) -> Path:
        await self.ensure_logged_in()
        os.environ["PDF_OUT_PATH"] = str(dest_path)
        await self._navigate_to_order(invoice)
        await self._open_print_dialog()
        await self._select_invoice_only()
        return await self._capture_download(dest_path)

    async def _navigate_to_order(self, invoice: InvoiceRecord) -> None:
        order_no = invoice.order_document_no or invoice.document_no
        logger.info("Navegando a pedido: %s", order_no)

        await self._page.locator("[data-testid='OBPOS2_Orders']").click()
        logger.debug("Sidebar OBPOS2_Orders clickado.")

        await self._page.locator("[data-testid='obpos2TicketListRemoteGrid']").wait_for(
            state="visible", timeout=_T_ELEM
        )

        filter_input = self._page.locator(
            "[data-testid='obc2FilterBarInput-documentNo-input']"
        )
        await filter_input.wait_for(state="visible", timeout=_T_ELEM)
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
                f"No aparecio ninguna fila con documentNo='{order_no}'."
            )

        matched_cell = None
        for cell in await cell_doc_no.all():
            if (await cell.inner_text()).strip() == order_no:
                matched_cell = cell
                break
        if matched_cell is None:
            logger.warning("Sin match exacto para '%s', usando primer resultado.", order_no)
            matched_cell = (await cell_doc_no.all())[0]

        await matched_cell.locator(
            "xpath=ancestor::div[contains(@class,'obc2DataGrid-rowContainer')]"
        ).click()
        logger.debug("Fila del pedido '%s' clickada.", order_no)

        try:
            clear_btn = self._page.locator(
                "[data-testid='obc2FilterBarInput-documentNo-clearText']"
            )
            await clear_btn.wait_for(state="visible", timeout=500)
            await clear_btn.click()
            logger.debug("Filtro 'Order No' limpiado.")
        except PlaywrightTimeoutError:
            pass

        _SEL_HDR = "[data-testid='obpos2ButtonBar1-ticketHeaderDocumentnoButton']"
        _SEL_RACE = f"{_SEL_PRINT_BTN}, {_SEL_HDR}"

        try:
            first_el = await self._page.wait_for_selector(_SEL_RACE, timeout=_T_ELEM)
            test_id = await first_el.get_attribute("data-testid") or ""
            if "ticketHeader" in test_id:
                logger.debug("Header visible. Esperando boton Print...")
                await self._page.locator(_SEL_PRINT_BTN).wait_for(
                    state="visible", timeout=_T_ELEM
                )
            logger.info("Pedido cargado correctamente: %s", order_no)
        except PlaywrightTimeoutError:
            raise PlaywrightTimeoutError(
                f"Boton 'Print / Email Duplicate' no visible para pedido '{order_no}'. "
                f"El pedido puede ser de un tipo no imprimible (simplificado, anulado, etc.)."
            )

    async def _open_print_dialog(self) -> None:
        await self._page.locator(_SEL_PRINT_BTN).click()
        logger.debug("Boton 'Print / Email Duplicate' clickado.")
        await self._page.locator("[data-testid='obc2InputDataDialog']").wait_for(
            state="visible", timeout=_T_ELEM
        )
        logger.debug("Dialogo 'Print / Email Duplicate' abierto.")

    async def _select_invoice_only(self) -> None:
        _CB_INVOICE = "[data-testid='obpos2BookedDeliveryOptionsDialog-singleInvoiceCheckBoxGroup-ticketInvoice-checkbox']"
        _CB_GIFT = "[data-testid='obpos2BookedDeliveryOptionsDialog-singleInvoiceCheckBoxGroup-giftReceipt-checkbox']"
        _CB_ORDER = "[data-testid='obpos2BookedDeliveryOptionsDialog-singleInvoiceCheckBoxGroup-receipt-checkbox']"
        _LBL_INVOICE = "[data-testid='obpos2BookedDeliveryOptionsDialog-singleInvoiceCheckBoxGroup-ticketInvoice-label']"
        _LBL_GIFT = "[data-testid='obpos2BookedDeliveryOptionsDialog-singleInvoiceCheckBoxGroup-giftReceipt-label']"
        _LBL_ORDER = "[data-testid='obpos2BookedDeliveryOptionsDialog-singleInvoiceCheckBoxGroup-receipt-label']"

        async def _is_checked(sel: str) -> bool:
            try:
                return await self._page.locator(sel).locator(
                    "[data-testid='CheckBoxIcon']"
                ).count() > 0
            except Exception:  # noqa: BLE001
                return False

        async def _ensure(cb: str, lbl: str, should: bool) -> None:
            if await _is_checked(cb) != should:
                await self._page.locator(lbl).click()
                logger.debug(
                    "Checkbox %s -> %s",
                    cb.split("-")[-2],
                    "marcado" if should else "desmarcado",
                )

        await _ensure(_CB_INVOICE, _LBL_INVOICE, True)
        await _ensure(_CB_GIFT, _LBL_GIFT, False)
        await _ensure(_CB_ORDER, _LBL_ORDER, False)
        logger.debug("Checkboxes configurados: Invoice=ON, GiftReceipt=OFF, Order=OFF")

    async def _capture_download(self, dest_path: Path) -> Path:
        print_btn = self._page.locator("[data-testid='obc2InputDataDialog-print']")
        await print_btn.wait_for(state="visible", timeout=_T_ELEM)

        staging_dir = dest_path.parent
        staging_dir.mkdir(parents=True, exist_ok=True)
        before: set[Path] = set(staging_dir.glob("*.pdf"))
        ts_before = asyncio.get_event_loop().time()

        logger.info("Clickando 'Print'. HWM movera el PDF a %s...", staging_dir)
        await print_btn.click()

        pdf_in_staging = await self._wait_new_pdf_in_staging(
            staging_dir, before, ts_before, timeout_s=90
        )
        pdf_in_staging.rename(dest_path)
        logger.info("PDF movido de staging a destino: %s", dest_path)

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
        deadline = asyncio.get_event_loop().time() + timeout_s
        while asyncio.get_event_loop().time() < deadline:
            new_files = set(staging_dir.glob("*.pdf")) - before
            if new_files:
                newest = max(new_files, key=lambda p: p.stat().st_mtime)
                if newest.stat().st_size > 0:
                    logger.info(
                        "PDF nuevo en staging: %s (%.1f KB)",
                        newest.name, newest.stat().st_size / 1024,
                    )
                    return newest
            await asyncio.sleep(0.5)
        raise TimeoutError(
            f"HWM no deposito PDF en {staging_dir} tras {timeout_s}s. "
            f"Comprueba openbravohw.properties: process.printpdf=terminal."
        )
