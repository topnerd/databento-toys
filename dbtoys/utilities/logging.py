"""Utility module for logging."""
import sys
from logging import FileHandler
from logging import Formatter
from logging import Logger
from logging import LogRecord
from logging import StreamHandler
from os import makedirs
from pathlib import Path
from tempfile import gettempdir
from typing import TextIO

from colorama import Fore

DEFAULT_LOG_FILE_PATH = Path(gettempdir()) / "dbtoys"

DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
}


class ColoramaConsoleFormatter(Formatter):
    """A log record formatter for printing to a console."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def format(self, record: LogRecord) -> str:
        """Formats the log record in a way that is more friendly to a console."""
        formatted_line = super().format(record)
        if record.levelno >= 40:
            return f"{Fore.RED}{formatted_line}{Fore.RESET}"
        if record.levelno >= 30:
            return f"{Fore.YELLOW}{formatted_line}{Fore.RESET}"
        return formatted_line


def configure_file_logger(logger: Logger, log_file_name: str):
    """Configures file logging for databento toys.
    Log files are written to the system temporary directory.
    :param logger: The logger to attach the file handler to.
    :param log_file_name: The log file name to use.
    """
    file_formatter = Formatter(
        "%(asctime)s [%(levelname)s] [%(module)s:%(lineno)s] %(message)s",
        "%Y%m%d %H:%M:%S",
    )

    try:
        makedirs(
            DEFAULT_LOG_FILE_PATH,
            mode=0o600,
            exist_ok=True,
        )
    except OSError as exc:
        sys.stderr.write(f"Failed to create log file! {exc}\n")
        return

    file_handler = FileHandler(
        filename=DEFAULT_LOG_FILE_PATH / log_file_name,
        mode="a",
        encoding="utf-8",
        delay=True,
    )
    file_handler.setLevel("NOTSET")
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)


def configure_console_handler(logger: Logger, stream: TextIO):
    """Configures console logging for databento toys.
    :param logger: The logger to attach the console handler to.
    """
    console_formatter = ColoramaConsoleFormatter("[%(levelname)s] %(message)s")

    console_handler = StreamHandler(
        stream=stream,
    )
    console_handler.setLevel("INFO")
    console_handler.setFormatter(console_formatter)

    logger.addHandler(console_handler)
