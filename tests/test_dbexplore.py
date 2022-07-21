"""Unit tests for dbexplore"""
import json
from io import StringIO
from pathlib import Path
from typing import Dict
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
from hamcrest import equal_to
from hamcrest import has_item
from hamcrest import string_contains_in_order

from dbtoys.dbexplore.app import DataBentoExplorer

TEST_DATA_PATH: Path = Path("tests", "test_dbexplore")


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


@pytest.fixture(name="list_fields_data")
def fixture_list_fields_data(dataset: str, schema: str, encoding: str) -> Dict:
    """Fixture for list_fields response data.
    Since responses to list_fields are dictionaries, we store them externally as json.
    """
    try:
        with open(
            TEST_DATA_PATH / f"list_fields_{dataset}_{schema}_{encoding}.json",
            encoding="utf-8",
        ) as json_file:
            return json.load(json_file)
    except OSError as exc:
        pytest.fail(
            f"No test data for 'list_fields {dataset} {schema} {encoding}'. Do you need to add it?\n{exc}"
        )


@pytest.mark.parametrize(
    "dataset,symbols,schema,result",
    [
        pytest.param("GLBX.MDP3", "BAC", "trades", 100.00),
        pytest.param("GLBX.MDP3", "TSLA", "mbo", 123.4567),
        pytest.param("XNAS.ITCH", "*", "ohlcv-1s", 0.0),
    ],
)
def test_get_cost(
    result: float,
    dbexplore: DataBentoExplorer,
    _stdout: StringIO,
    schema: str,
    symbols: str,
    dataset: str,
    command: str = "get_cost",
):
    """Test get_cost displaying the price as a float."""
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.return_value = result
    dbexplore.onecmd(" ".join([command, dataset, symbols, schema]))
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(output[0], equal_to(f"${result}\n"))


@pytest.mark.parametrize(
    "dataset,symbols,schema,result",
    [
        pytest.param("GLBX.MDP3", "BAC", "trades", [0, 0]),
        pytest.param("GLBX.MDP3", "TSLA", "mbo", [5, 5]),
        pytest.param("XNAS.ITCH", "*", "ohlcv-1s", [100, 100]),
    ],
)
def test_get_shape(
    result: Iterable[int],
    dbexplore: DataBentoExplorer,
    _stdout: StringIO,
    schema: str,
    symbols: str,
    dataset: str,
    command: str = "get_shape",
):
    """Test get_size displaying a list of results.
    The result is a tuple with two elements.
    """
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.return_value = result
    dbexplore.onecmd(" ".join([command, dataset, symbols, schema]))
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(
        output[0], string_contains_in_order(*(str(x) for x in result), "\n")
    )


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
    """Test list_compressions properly consumes databento exceptions."""
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
    """Test list_datasets displaying a list results.
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
    """Test list_encodings displaying a list results.
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
    """Test list_encodings properly consumes databento exceptions."""
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.side_effect = bento_exception
    dbexplore.onecmd(command)
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(output, empty())


@pytest.mark.parametrize(
    "dataset, schema, encoding",
    [
        pytest.param("GLBX.MDP3", "definition", "dbz"),
        pytest.param("GLBX.MDP3", "trades", "dbz"),
    ],
)
def test_list_fields(
    dbexplore: DataBentoExplorer,
    _stdout: StringIO,
    encoding: str,
    schema: str,
    dataset: str,
    list_fields_data: Dict,
    command: str = "list_fields",
):
    """Test list_fields displaying a table of results.
    The entries will be printed using tabulate.
    """
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.return_value = list_fields_data
    dbexplore.onecmd(" ".join([command, dataset, schema, encoding]))
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()

    for _, encodings in list_fields_data.items():
        for _, schemas in encodings.items():
            for _, fields in schemas.items():
                for field, _type in fields.items():
                    assert_that(
                        output, has_item(string_contains_in_order(field, _type))
                    )


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
    """Test list_fields properly consumes databento exceptions."""
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
    """Test list_schemas displaying a list results.
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
    """Test list_schemas properly consumes databento exceptions."""
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.side_effect = bento_exception
    dbexplore.onecmd(" ".join([command, dataset]))
    cmd_func.assert_called()

    _stdout.seek(0)
    output = _stdout.readlines()
    assert_that(output, empty())
