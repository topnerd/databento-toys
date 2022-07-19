"""Unit test for utilities.key"""
from pathlib import Path
from typing import Tuple
from unittest.mock import DEFAULT
from unittest.mock import patch

import pytest

# pyright: reportPrivateImportUsage=false
from hamcrest import assert_that
from hamcrest import equal_to

from dbtoys.utilities.key import DEFAULT_KEY_FILENAME
from dbtoys.utilities.key import find_key
from dbtoys.utilities.key import get_api_key
from dbtoys.utilities.key import hide_key
from dbtoys.utilities.key import read_key_file
from dbtoys.utilities.key import sanitize_key


@pytest.fixture(name="key", autouse=True)
def fixture_key() -> str:
    """A default key fixture."""
    return "db-unittest"


@pytest.fixture(name="dbkey_file")
def fixture_dbkey_file(tmp_path: Path, key: str) -> Path:
    """A fixture file for a databento secret."""
    key_path: Path = tmp_path / DEFAULT_KEY_FILENAME
    with open(key_path, "w+", encoding="utf-8") as key_file:
        key_file.write(key)
    return key_path


def test_find_key(
    tmp_path_factory,
    dbkey_file: Path,
):
    """Finding a key requires us to check a few directories for a file."""
    test_paths: Tuple[Path, ...] = (
        tmp_path_factory.mktemp("home"),
        tmp_path_factory.mktemp("root"),
        dbkey_file.parent,
    )
    found_key_path = find_key(paths=test_paths)
    assert_that(found_key_path, equal_to(dbkey_file))


def test_get_api_key_with_file(dbkey_file: Path, key: str):
    """Getting a key usually means we look for a key file."""
    with patch("dbtoys.utilities.key.find_key") as mocked_find_key:
        mocked_find_key.return_value = dbkey_file
        found_key = get_api_key(prompt_for_key=False)
        assert_that(found_key, equal_to(key))


def test_get_api_key_with_prompt(key: str):
    """Getting a key usually means we look for a key file."""
    with patch.multiple(
        "dbtoys.utilities.key", find_key=DEFAULT, getpass=DEFAULT
    ) as mocks:
        mocks["find_key"].return_value = None
        mocks["getpass"].return_value = key
        found_key = get_api_key(prompt_for_key=True)
        assert_that(found_key, equal_to(key))


@pytest.mark.parametrize(
    "key, revealed, length, fill_str, expected",
    [
        pytest.param("foobarbaz", 3, 6, "*", "foo***"),
        pytest.param("foobarbaz", 1, 15, "x", "fxxxxxxxxxxxxxx"),
        pytest.param("foobarbaz", 0, 0, "#", ""),
        pytest.param("", 0, 0, "#", ""),
    ],
)
def test_hide_key(
    key: str, revealed: int, length: int, fill_str: str, expected: str
):
    """A hidden key should be partially obscured."""
    hidden = hide_key(
        key=key, revealed=revealed, length=length, fill_str=fill_str
    )
    assert_that(hidden, equal_to(expected))


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("db-foobarbaz"),
        pytest.param("db-foobar\nbaz"),
        pytest.param(""),
    ],
)
def test_read_key_file(dbkey_file: Path, key: str):
    """Reading text from a file is pretty easy."""
    read_key = read_key_file(dbkey_file)
    assert_that(read_key, equal_to(key))


@pytest.mark.parametrize(
    "key, expected",
    [
        pytest.param("foobarbaz", "foobarbaz"),
        pytest.param("f00b4r-b4z!", "f00b4r-b4z"),
        pytest.param("f00b@r_b@z!", "f00brbz"),
        pytest.param("foo\x23\x84", "foo"),
        pytest.param("", ""),
    ],
)
def test_sanitize_key(key: str, expected: str):
    """A sanitized key shouldn't contain any special characters."""
    assert_that(sanitize_key(key), equal_to(expected))
