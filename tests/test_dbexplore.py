"""Unit tests for dbexplore"""
from io import StringIO
from typing import Iterable
from typing import Type
from unittest.mock import MagicMock

import pytest
from databento.historical.error import BentoClientError
from databento.historical.error import BentoHttpError
from databento.historical.error import BentoServerError

# pyright: reportPrivateImportUsage=false
from hamcrest import assert_that
from hamcrest import empty
from hamcrest import string_contains_in_order

from dbtoys.dbexplore.app import DataBentoExplorer


@pytest.fixture(name="_stdout")
def fixture_stdout():
    """A fixture stream to use instead of sys.stdout."""
    return StringIO()


@pytest.fixture(name="dbexplore")
def fixture_dbexplore(_stdout) -> DataBentoExplorer:
    """Fixture for the dbexplore toy."""
    app = DataBentoExplorer(
        api_key="UNITTEST",
        stdout=_stdout,
    )
    setattr(app, "_historical_client", MagicMock())
    return app


@pytest.mark.parametrize(
    "result",
    [
        pytest.param(["FOO"], id="foo"),
        pytest.param(["FOO", "BAR"], id="foo,bar"),
        pytest.param(["FOO", "BAR", "BAZ"], id="foo,bar,baz"),
        pytest.param([], id=""),
    ],
)
def test_list_compressions(
    result: Iterable[str],
    dbexplore: DataBentoExplorer,
    _stdout: StringIO,
    command: str = "list_compressions",
):
    """Test list_compressions displaying a list results.
    The entries will be padded with spaces to "columnize" the data.
    """
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.return_value = result
    dbexplore.onecmd(command)
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(output[0], string_contains_in_order(*result, "\n"))


@pytest.mark.parametrize(
    "bento_exception",
    [
        pytest.param(BentoClientError),
        pytest.param(BentoServerError),
        pytest.param(BentoHttpError),
    ],
)
def test_list_compressions_exceptions(
    bento_exception: Type, dbexplore: DataBentoExplorer, _stdout: StringIO
):
    """Test list_datasets properly consumes databento exceptions."""
    dbexplore.historical_client.metadata.list_datasets.side_effect = (
        bento_exception
    )
    dbexplore.onecmd("list_datasets")
    dbexplore.historical_client.metadata.list_datasets.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(output, empty())


@pytest.mark.parametrize(
    "result",
    [
        pytest.param(["FOO"], id="foo"),
        pytest.param(["FOO", "BAR"], id="foo,bar"),
        pytest.param(["FOO", "BAR", "BAZ"], id="foo,bar,baz"),
        pytest.param([], id=""),
    ],
)
def test_list_datasets(
    result: Iterable[str],
    dbexplore: DataBentoExplorer,
    _stdout: StringIO,
    command: str = "list_datasets",
):
    """Test list_compressions displaying a list results.
    The entries will be padded with spaces to "columnize" the data.
    """
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.return_value = result
    dbexplore.onecmd(command)
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(output[0], string_contains_in_order(*result, "\n"))


@pytest.mark.parametrize(
    "bento_exception",
    [
        pytest.param(BentoClientError),
        pytest.param(BentoServerError),
        pytest.param(BentoHttpError),
    ],
)
def test_list_datasets_exceptions(
    bento_exception: Type,
    dbexplore: DataBentoExplorer,
    _stdout: StringIO,
    command: str = "list_datasets",
):
    """Test list_datasets properly consumes databento exceptions."""
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.side_effect = bento_exception
    dbexplore.onecmd(command)
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(output, empty())


@pytest.mark.parametrize(
    "result",
    [
        pytest.param(["FOO"], id="foo"),
        pytest.param(["FOO", "BAR"], id="foo,bar"),
        pytest.param(["FOO", "BAR", "BAZ"], id="foo,bar,baz"),
        pytest.param([], id=""),
    ],
)
def test_list_encodings(
    result: Iterable[str],
    dbexplore: DataBentoExplorer,
    _stdout: StringIO,
    command: str = "list_encodings",
):
    """Test list_compressions displaying a list results.
    The entries will be padded with spaces to "columnize" the data.
    """
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.return_value = result
    dbexplore.onecmd(command)
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(output[0], string_contains_in_order(*result, "\n"))


@pytest.mark.parametrize(
    "bento_exception",
    [
        pytest.param(BentoClientError),
        pytest.param(BentoServerError),
        pytest.param(BentoHttpError),
    ],
)
def test_list_encodings_exceptions(
    bento_exception: Type,
    dbexplore: DataBentoExplorer,
    _stdout: StringIO,
    command: str = "list_encodings",
):
    """Test list_datasets properly consumes databento exceptions."""
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.side_effect = bento_exception
    dbexplore.onecmd(command)
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(output, empty())


@pytest.mark.parametrize(
    "result",
    [
        # TODO: Need to emulate the json response here.
        pytest.param({}, id=""),
    ],
)
@pytest.mark.xfail()
def test_list_fields(
    result: Iterable[str],
    dbexplore: DataBentoExplorer,
    _stdout: StringIO,
    command: str = "list_fields",
    dataset: str = "GLBX.MDP3",  # We hard code this to pass the choices check.
    schema: str = "trades",  # Same here.
):
    """Test list_compressions displaying a list results.
    The entries will be padded with spaces to "columnize" the data.
    """
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.return_value = result
    dbexplore.onecmd(" ".join([command, dataset, schema]))
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(output[0], string_contains_in_order(*result, "\n"))


@pytest.mark.parametrize(
    "bento_exception",
    [
        pytest.param(BentoClientError),
        pytest.param(BentoServerError),
        pytest.param(BentoHttpError),
    ],
)
def test_list_fields_exceptions(
    bento_exception: Type,
    dbexplore: DataBentoExplorer,
    _stdout: StringIO,
    command: str = "list_fields",
    dataset: str = "GLBX.MDP3",  # We hard code this to pass the choices check.
    schema: str = "trades",  # Same here.
):
    """Test list_datasets properly consumes databento exceptions."""
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.side_effect = bento_exception
    dbexplore.onecmd(" ".join([command, dataset, schema]))
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(output, empty())


@pytest.mark.parametrize(
    "result",
    [
        pytest.param(["FOO"], id="foo"),
        pytest.param(["FOO", "BAR"], id="foo,bar"),
        pytest.param(["FOO", "BAR", "BAZ"], id="foo,bar,baz"),
        pytest.param([], id=""),
    ],
)
def test_list_schemas(
    result: Iterable[str],
    dbexplore: DataBentoExplorer,
    _stdout: StringIO,
    command: str = "list_schemas",
    dataset: str = "GLBX.MDP3",  # We hard code this to pass the choices check.
):
    """Test list_compressions displaying a list results.
    The entries will be padded with spaces to "columnize" the data.
    """
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.return_value = result
    dbexplore.onecmd(" ".join([command, dataset]))
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(output[0], string_contains_in_order(*result, "\n"))


@pytest.mark.parametrize(
    "bento_exception",
    [
        pytest.param(BentoClientError),
        pytest.param(BentoServerError),
        pytest.param(BentoHttpError),
    ],
)
def test_list_schemas_exceptions(
    bento_exception: Type,
    dbexplore: DataBentoExplorer,
    _stdout: StringIO,
    command: str = "list_schemas",
    dataset: str = "GLBX.MDP3",  # We hard code this to pass the choices check.
):
    """Test list_datasets properly consumes databento exceptions."""
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.side_effect = bento_exception
    dbexplore.onecmd(" ".join([command, dataset]))
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(output, empty())
