"""Utility module for manging Databento API keys."""
import logging
import re
import sys
from getpass import getpass
from os import getcwd
from pathlib import Path
from typing import Iterable
from typing import Optional

_LOG = logging.getLogger()

# Some sane defaults for convenience.
DEFAULT_KEY_FILENAME = ".dbkey"
DEFAULT_PATHS = (
    Path(getcwd()),
    Path.home(),
)


def find_key(
    key_file_name: str = DEFAULT_KEY_FILENAME,
    paths: Iterable[Path] = DEFAULT_PATHS,
) -> Optional[Path]:
    """Search for a databento key file.
    :param key_file_name: The name of the key file (default: .dbkey)
    :param paths: A list of directort paths to try. (default: cwd, ~)
    :return: The path to a key file or None if no file is found.
    """
    for path in paths:
        key_file_path = path / DEFAULT_KEY_FILENAME
        if not key_file_path.exists():
            _LOG.debug("No %s found in %s", DEFAULT_KEY_FILENAME, path)
            continue
        _LOG.debug("Found key file in %s", key_file_path)
        return key_file_path
    _LOG.debug("Failed to find a %s in default paths.", key_file_name)
    return None


def read_key_file(key_file_path: Path) -> str:
    """Read a key file and return it's secret as a string.
    :param key_file_path: The path to the key file.
    :return: The content of the key file.
    """
    try:
        with open(key_file_path, "r", encoding="utf-8") as key_file:
            key = key_file.read().strip()
    except OSError as exc:
        _LOG.exception(
            "Failed to read key file at %s", key_file_path, exc_info=exc
        )
        raise exc
    return key


def hide_key(
    key: str, revealed: int = 6, length: int = 29, fill_str: str = "*"
) -> str:
    """Create a log friendly key that only reveals a portion of the secret.
    :param key: The full key.
    :param revealed: The number of characters to reveal.
    :param length: The length of the hidden key.
    :param fill_str: The string to fill empty space with.
    """
    return key[0:revealed].ljust(length, fill_str)


def sanitize_key(key: str) -> str:
    """Removes any special characters from a key.
    :param key: The key.
    :return: A key with any special characters removed.
    """
    return re.sub("[^A-Za-z0-9-]+", "", key)


def get_api_key(prompt_for_key: bool = False) -> str:
    """Get an API key to use for databento from a file (.dbkey) or user input.
    :param prompt_for_key: If a key is not found, prompt the user to enter a key.
    """
    key_file = find_key()
    if key_file:
        file_key = sanitize_key(read_key_file(key_file))
        _LOG.debug("Using %s as a key from key file.", hide_key(file_key))
        return file_key

    if prompt_for_key:
        # Prompt the user for a key.
        sys.stderr.write("Please enter a databento API key.\n")
        user_key = sanitize_key(getpass(">> ", sys.stderr))
        _LOG.debug("User provided %s as a key.", hide_key(user_key))
        return user_key

    return ""
