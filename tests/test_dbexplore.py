"""Unit tests for dbexplore"""
import json
from io import StringIO
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Type
from typing import Union
from unittest import mock

import humanize
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


@pytest.fixture(name="mock_stdout")
def fixture_stdout():
    """A fixture stream to use instead of sys.stdout."""
    return StringIO()


@pytest.fixture(name="dbexplore")
@pytest.mark.usefixtures("mock_stdout")
def fixture_dbexplore(mock_stdout) -> DataBentoExplorer:
    """Fixture for the dbexplore toy."""
    app = DataBentoExplorer(
        api_key="UNITTEST",
        stdout=mock_stdout,
    )
    setattr(app, "_historical_client", mock.MagicMock())
    return app


@pytest.fixture(name="command_data")
def fixture_command_dict(
    command: str, args: Iterable[str], extension: str = "json"
) -> Union[Dict, List, str, float, bool]:
    """Fixture for reading files as test_data.
    This is useful for commands which may have large returns.
    The test data is expected to be in tests/tests_dbexplore.
    The filename is generated from the command and arguments list, for example:
        list_fields XNAS.ITCH trades
        tests/test_dbexplore/get_fields_XNAS.ITCH_trades.json
    """
    expected_file_name = f"{command}_{'_'.join(args)}.{extension}"
    try:
        with open(
            TEST_DATA_PATH / expected_file_name,
            encoding="utf-8",
        ) as json_file:
            return json.load(json_file)
    except OSError as exc:
        raise EnvironmentError(
            f"Could not load test data: {expected_file_name}",
        ) from exc


def call_command(
    dbexplore: DataBentoExplorer,
    command: str,
    args: Iterable[str],
    return_value: Optional[Any] = None,
    side_effect: Optional[Any] = None,
):
    """Helper method to mock and call a dbexplore command."""
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.return_value = return_value
    cmd_func.side_effect = side_effect
    dbexplore.onecmd(" ".join([command, *args]))


def call_command_and_assert(
    dbexplore: DataBentoExplorer,
    command: str,
    args: Iterable[str],
    return_value: Optional[Any] = None,
    side_effect: Optional[Any] = None,
):
    """Helper method to mock and call a dbexplore command.
    Additionally, asserts that the method was called.
    """
    call_command(
        dbexplore=dbexplore,
        command=command,
        args=args,
        return_value=return_value,
        side_effect=side_effect,
    )
    cmd_func = getattr(dbexplore.historical_client.metadata, command)
    cmd_func.assert_called_once()


@pytest.mark.parametrize(
    "bento_exception",
    [
        pytest.param(BentoClientError),
        pytest.param(BentoServerError),
        pytest.param(BentoHttpError),
    ],
)
@pytest.mark.parametrize(
    "command, args",
    [
        pytest.param("get_cost", ["XNAS.ITCH", "*", "tbbo"]),
        pytest.param("get_shape", ["GLBX.MDP3", "*", "trades"]),
        pytest.param("list_unit_prices", ["XNAS.ITCH"]),
        pytest.param("list_compressions", []),
        pytest.param("list_datasets", []),
        pytest.param("list_encodings", []),
        pytest.param("list_fields", ["GLBX.MDP3", "trades"]),
        pytest.param("list_schemas", ["XNAS.ITCH"]),
        pytest.param("list_unit_prices", ["XNAS.ITCH"]),
    ],
)
def test_command_bentoexception(
    dbexplore: DataBentoExplorer,
    bento_exception: Type,
    command: str,
    args: Iterable[str],
):
    """Tests that every dbexplore command handles BentoExceptions generated from
    the databento calls.
    """
    call_command_and_assert(
        dbexplore,
        command=command,
        args=args,
        side_effect=bento_exception,
    )

    dbexplore.stdout.seek(0)
    output = dbexplore.stdout.readlines()
    assert_that(output, empty())


@pytest.mark.parametrize("command", [pytest.param("get_billable_size")])
@pytest.mark.parametrize(
    "args, result",
    [
        pytest.param(["GLBX.MDP3", "ESH1", "trades", "json"], 1024),
        pytest.param(["XNAS.ITCH", "*", "mbo", "dbz"], 892374),
    ],
)
def test_get_billable_size(
    result: int,
    dbexplore: DataBentoExplorer,
    command: str,
    args: Iterable[str],
):
    """Test get_billable_size displaying a list of results.
    The result is integer number of bytes but we display a human friendly size as well.
    """
    call_command_and_assert(
        dbexplore,
        command=command,
        args=args,
        return_value=result,
    )

    dbexplore.stdout.seek(0)
    output = dbexplore.stdout.readlines()
    assert_that(
        output[0],
        string_contains_in_order(
            str(result), humanize.naturalsize(result), "\n"
        ),
    )


@pytest.mark.parametrize("command", [pytest.param("get_cost")])
@pytest.mark.parametrize(
    "args, result",
    [
        pytest.param(["GLBX.MDP3", "BAC", "trades"], 100.00),
        pytest.param(["GLBX.MDP3", "TSLA", "mbo"], 123.4567),
        pytest.param(["XNAS.ITCH", "*", "ohlcv-1s"], 0.0),
    ],
)
def test_get_cost(
    result: float,
    dbexplore: DataBentoExplorer,
    command: str,
    args: Iterable[str],
):
    """Test get_cost displaying the price as a float.
    Costs are printed to the nearest hundreth.
    """
    call_command_and_assert(
        dbexplore,
        command=command,
        args=args,
        return_value=result,
    )

    dbexplore.stdout.seek(0)
    output = dbexplore.stdout.readlines()
    assert_that(output[0], equal_to(f"{result:.2f}\n"))


@pytest.mark.parametrize("command", [pytest.param("get_shape")])
@pytest.mark.parametrize(
    "args,result",
    [
        pytest.param(["GLBX.MDP3", "BAC", "trades"], [0, 0]),
        pytest.param(["GLBX.MDP3", "TSLA", "mbo"], [5, 5]),
        pytest.param(["XNAS.ITCH", "*", "ohlcv-1s"], [100, 100]),
    ],
)
def test_get_shape(
    result: Iterable[int],
    dbexplore: DataBentoExplorer,
    command: str,
    args: Iterable[str],
):
    """Test get_size displaying a list of results.
    The result is a tuple with two elements.
    """
    call_command_and_assert(
        dbexplore,
        command=command,
        args=args,
        return_value=result,
    )

    dbexplore.stdout.seek(0)
    output = dbexplore.stdout.readlines()
    assert_that(
        output[0], string_contains_in_order(*(str(x) for x in result), "\n")
    )


@pytest.mark.parametrize(
    "command,args",
    [
        pytest.param("list_compressions", []),
        pytest.param("list_datasets", []),
        pytest.param("list_encodings", []),
        pytest.param("list_schemas", ["GLBX.MDP3"]),
    ],
)
@pytest.mark.parametrize(
    "result",
    [
        pytest.param(["FOO"]),
        pytest.param(["FOO", "BAR"]),
        pytest.param(["FOO", "BAR", "BAZ"]),
        pytest.param([]),
    ],
)
def test_list_commands(
    result: Iterable[int],
    dbexplore: DataBentoExplorer,
    command: str,
    args: Iterable[str],
):
    """Tests list commands display results as text with minimal formatting."""
    call_command_and_assert(
        dbexplore,
        command=command,
        args=args,
        return_value=result,
    )

    dbexplore.stdout.seek(0)
    output = dbexplore.stdout.readlines()
    assert_that(output[0], string_contains_in_order(*map(str, result), "\n"))


@pytest.mark.parametrize("command", [pytest.param("list_fields")])
@pytest.mark.parametrize(
    "args",
    [
        pytest.param(["GLBX.MDP3", "definition", "dbz"]),
        pytest.param(["GLBX.MDP3", "trades", "dbz"]),
    ],
)
def test_list_fields(
    dbexplore: DataBentoExplorer,
    command: str,
    args: Iterable[str],
    command_data: Dict,
):
    """Test list_fields displaying a table of results.
    The entries will be printed using tabulate.
    """
    call_command_and_assert(
        dbexplore,
        command=command,
        args=args,
        return_value=command_data,
    )

    dbexplore.stdout.seek(0)
    output = dbexplore.stdout.readlines()

    for _, encodings in command_data.items():
        for _, schemas in encodings.items():
            for _, fields in schemas.items():
                for field, _type in fields.items():
                    assert_that(
                        output,
                        has_item(string_contains_in_order(field, _type, "\n")),
                    )


@pytest.mark.parametrize("command", [pytest.param("list_unit_prices")])
@pytest.mark.parametrize(
    "args",
    [
        pytest.param(["XNAS.ITCH", "live"]),
        pytest.param(["GLBX.MDP3", "historical"]),
    ],
)
def test_list_unit_prices(
    dbexplore: DataBentoExplorer,
    command: str,
    args: Iterable[str],
    command_data: Dict,
):
    """Test list_unit_prices displaying a table of results.
    The entries will be printed using tabulate.
    Costs are printed to the nearest hundreth.
    """
    call_command_and_assert(
        dbexplore,
        command=command,
        args=args,
        return_value=command_data,
    )

    dbexplore.stdout.seek(0)
    output = dbexplore.stdout.readlines()

    for _, unit_prices in command_data.items():
        for schema, unit_price in unit_prices.items():
            assert_that(
                output,
                has_item(
                    string_contains_in_order(schema, f"{unit_price:.2f}", "\n")
                ),
            )
