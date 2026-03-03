"""
domain/exceptions.py
─────────────────────
Excepciones de dominio del robot.
Sin dependencias de infraestructura.
"""


class TerminalLockedException(Exception):
    """
    OBP0002-0001: Terminal ya vinculado a otro dispositivo físico.
    Requiere unlink manual en OpenBravo antes de continuar.
    """
    def __init__(self, terminal_key: str) -> None:
        self.terminal_key = terminal_key
        super().__init__(
            f"Terminal {terminal_key} bloqueado: 'already linked to another physical device'. "
            f"Se requiere unlink manual en OpenBravo antes de continuar."
        )


class InvalidCredentialsException(Exception):
    """
    OBP0002-0003: Usuario o contraseña incorrectos en Terminal Selection.
    Requiere corrección de las variables OB_TERMINAL_USERNAME / OB_TERMINAL_PASSWORD
    o del usuario en OpenBravo antes de continuar.
    """
    def __init__(self, terminal_key: str, username: str) -> None:
        self.terminal_key = terminal_key
        self.username     = username
        super().__init__(
            f"Credenciales inválidas para usuario '{username}' en terminal {terminal_key}. "
            f"Revisar OB_TERMINAL_USERNAME / OB_TERMINAL_PASSWORD."
        )
