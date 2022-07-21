"""The dbexplore application"""
import functools
import logging
import logging.config
import sys
from pprint import pformat
from typing import Callable
from typing import Tuple

import cmd2
import databento
import pandas
from colorama import Fore
from databento.common.enums import Compression
from databento.common.enums import Dataset
from databento.common.enums import Encoding
from databento.common.enums import FeedMode
from databento.common.enums import Schema
from databento.historical.error import BentoError
from tabulate import tabulate

import dbtoys.utilities.key
import dbtoys.utilities.logging
import dbtoys.utilities.parser

_LOG = logging.getLogger()
_PROG = "dbexplore"

KNOWN_COMPRESSIONS: Tuple[str, ...] = tuple(x.value for x in Compression)
KNOWN_DATASETS: Tuple[str, ...] = tuple(x.value for x in Dataset)
KNOWN_ENCODINGS: Tuple[str, ...] = tuple(x.value for x in Encoding)
KNOWN_FEED_MODES: Tuple[str, ...] = tuple(x.value for x in FeedMode)
KNOWN_SCHEMAS: Tuple[str, ...] = tuple(x.value for x in Schema)


def main(cantrip: str = "", verbose: bool = False) -> int:
    """Runs the toy dbexplore.
    :param cantrip: Read all commands from stdin and then exit.
    :param list_datasets: print a list of datasets; skips entering the readline interface.
    :param verbose: Enables printing of log records to stderr.
    :return: A POSIX exit code.
    """
    logging.config.dictConfig(dbtoys.utilities.logging.DEFAULT_LOGGING)
    dbtoys.utilities.logging.configure_file_logger(
        logger=_LOG, log_file_name=f"{_PROG}.log"
    )
    _LOG.setLevel("NOTSET")

    if verbose:
        # If the --verbose flag was given we will print log events to stderr.
        dbtoys.utilities.logging.configure_console_handler(
            logger=_LOG, stream=sys.stderr
        )

    _LOG.debug(
        "Executing %s with arguments: cantrip=%s  verbose=%s",
        _PROG,
        cantrip,
        verbose,
    )

    try:
        api_key = dbtoys.utilities.key.get_api_key(prompt_for_key=True)
        explorer = DataBentoExplorer(api_key=api_key)
        if cantrip:
            cantrip_str = " ".join(cantrip)
            _LOG.debug(
                "Running cantrip %s",
                cantrip_str,
            )
            explorer.onecmd(cantrip_str)
        else:
            explorer.cmdloop()
    except Exception as exc:
        _LOG.exception("Terminating due to unhandled %s!", exc.__class__)
        raise exc
    else:
        return 0


# Command Parsers
_parse_get_cost: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
_parse_get_cost.add_argument(
    "dataset",
    choices=KNOWN_DATASETS,
    type=str,
    help="the target dataset",
)
_parse_get_cost.add_argument(
    "symbols", type=str, help="one or more symbols separated by commas"
)
_parse_get_cost.add_argument(
    "schema",
    choices=KNOWN_SCHEMAS,
    type=str,
    help="a data schema",
)
_parse_get_cost.add_argument(
    "encoding",
    choices=KNOWN_ENCODINGS,
    type=str,
    nargs="?",
    help="a data encoding",
    const=None,
)
_parse_get_cost.add_argument(
    "compression",
    choices=KNOWN_COMPRESSIONS,
    type=str,
    nargs="?",
    help="a data compression",
    const=None,
)
_parse_get_cost.add_argument(
    "--start",
    "-s",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the earlierst date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)
_parse_get_cost.add_argument(
    "--end",
    "-e",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the latest date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)

_parse_get_shape: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
_parse_get_shape.add_argument(
    "dataset",
    choices=KNOWN_DATASETS,
    type=str,
    help="the target dataset",
)
_parse_get_shape.add_argument(
    "symbols", type=str, help="one or more symbols separated by commas"
)
_parse_get_shape.add_argument(
    "schema",
    choices=KNOWN_SCHEMAS,
    type=str,
    help="a data schema",
)
_parse_get_shape.add_argument(
    "--start",
    "-s",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the earlierst date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)
_parse_get_shape.add_argument(
    "--end",
    "-e",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the latest date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)

_parse_list_datasets: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
_parse_list_datasets.add_argument(
    "--start",
    "-s",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the earlierst date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)
_parse_list_datasets.add_argument(
    "--end",
    "-e",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the latest date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)

_parse_list_fields: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
_parse_list_fields.add_argument(
    "dataset",
    choices=KNOWN_DATASETS,
    type=str,
    help="the target dataset",
)
_parse_list_fields.add_argument(
    "schema",
    choices=KNOWN_SCHEMAS,
    type=str,
    help="a data schema",
)
_parse_list_fields.add_argument(
    "encoding",
    choices=KNOWN_ENCODINGS,
    type=str,
    nargs="?",
    help="a data encoding",
    const=None,
)

_parse_list_schemas: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
_parse_list_schemas.add_argument(
    "dataset",
    choices=KNOWN_DATASETS,
    type=str,
    help="the target dataset",
)
_parse_list_schemas.add_argument(
    "--start",
    "-s",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the earlierst date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)
_parse_list_schemas.add_argument(
    "--end",
    "-e",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the latest date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)

_parse_list_unit_prices: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
_parse_list_unit_prices.add_argument(
    "dataset",
    choices=KNOWN_DATASETS,
    type=str,
    help="the target dataset",
)
_parse_list_unit_prices.add_argument(
    "mode",
    choices=KNOWN_FEED_MODES,
    type=str,
    nargs="?",
    help="a feed mode",
    default=None,
)
_parse_list_unit_prices.add_argument(
    "schema",
    choices=KNOWN_SCHEMAS,
    type=str,
    nargs="?",
    help="a data schema",
    default=None,
)


def log_command(func: Callable) -> Callable:
    """This is a wrapper to log user commands."""

    @functools.wraps(func)
    def wrapper(obj, statement, *args, **kwargs):
        _LOG.debug(
            "Processing %s",
            pformat(statement.raw),
        )
        return func(obj, statement, *args, **kwargs)

    return wrapper


class DataBentoExplorer(cmd2.Cmd):
    """The read-line interpreter for dbexplore."""

    METADATA_COMMANDS: str = "Metadata Commands"

    def __init__(self, api_key: str, **kwargs):
        super().__init__(**kwargs)
        self.prompt = f"{Fore.MAGENTA}>> {Fore.RESET}"
        self.continuation_prompt = f"{Fore.MAGENTA}>{Fore.RESET}"

        # Hide some builtin cmd2 commands
        self.hidden_commands.append("edit")
        self.hidden_commands.append("eof")
        self.hidden_commands.append("ipy")
        self.hidden_commands.append("py")
        self.hidden_commands.append("run_pyscript")
        self.hidden_commands.append("run_script")
        self.hidden_commands.append("shell")
        self.hidden_commands.append("shortcuts")

        # Databento
        self._historical_client: databento.Historical = databento.Historical(
            key=api_key,
        )

    @property
    def historical_client(self) -> databento.Historical:
        """The databento historical client"""
        return self._historical_client

    @log_command
    @cmd2.with_category(METADATA_COMMANDS)
    @cmd2.with_argparser(_parse_get_cost)  # type: ignore
    def do_get_cost(self, args):
        """Gets the cost of timeseries data."""
        try:
            result = self.historical_client.metadata.get_cost(
                dataset=args.dataset,
                symbols=args.symbols.split(","),
                schema=args.schema,
                start=args.start,
                end=args.end,
            )
        except BentoError as exc:
            self.perror(f"ERROR: {str(exc)}")
            _LOG.exception(exc)
        else:
            self.poutput(f"{result:.2f}")

    @log_command
    @cmd2.with_category(METADATA_COMMANDS)
    @cmd2.with_argparser(_parse_get_shape)  # type: ignore
    def do_get_shape(self, args):
        """Gets the dimensions of timeseries data."""
        try:
            result = self.historical_client.metadata.get_shape(
                dataset=args.dataset,
                symbols=args.symbols.split(","),
                schema=args.schema,
                start=args.start,
                end=args.end,
            )
        except BentoError as exc:
            self.perror(f"ERROR: {str(exc)}")
            _LOG.exception(exc)
        else:
            self.columnize([str(r) for r in result])

    @log_command
    @cmd2.with_category(METADATA_COMMANDS)
    def do_list_compressions(self, _):
        """List all compressions."""
        try:
            result = self.historical_client.metadata.list_compressions()
        except BentoError as exc:
            self.perror(f"ERROR: {str(exc)}")
            _LOG.exception(exc)
        else:
            self.columnize(result)

    @log_command
    @cmd2.with_category(METADATA_COMMANDS)
    @cmd2.with_argparser(_parse_list_datasets)  # type: ignore
    def do_list_datasets(self, args):
        """List all datasets."""
        try:
            result = self.historical_client.metadata.list_datasets(
                start=args.start,
                end=args.end,
            )
        except BentoError as exc:
            self.perror(f"ERROR: {str(exc)}")
            _LOG.exception(exc)
        else:
            self.columnize(result)

    @log_command
    @cmd2.with_category(METADATA_COMMANDS)
    def do_list_encodings(self, _):
        """List all encodings."""
        try:
            result = self.historical_client.metadata.list_encodings()
        except BentoError as exc:
            self.perror(f"ERROR: {str(exc)}")
            _LOG.exception(exc)
        else:
            self.columnize(result)

    @log_command
    @cmd2.with_category(METADATA_COMMANDS)
    @cmd2.with_argparser(_parse_list_fields)  # type: ignore
    def do_list_fields(self, args):
        """List all fields from the given dataset and schema."""
        try:
            result = self.historical_client.metadata.list_fields(
                dataset=args.dataset,
                schema=args.schema,
                encoding=args.encoding,
            )
        except BentoError as exc:
            self.perror(f"ERROR: {str(exc)}")
            _LOG.exception(exc)
        else:
            output = []
            for _, encodings in result.items():
                for encoding, schemas in encodings.items():
                    if args.encoding is None:
                        output.append(f"--- {encoding} fields ---")
                    for _, fields in schemas.items():
                        output.append(
                            tabulate(
                                tabular_data=[
                                    [k, v] for k, v in fields.items()
                                ],
                                headers=["field", "type"],
                            )
                        )
            self.ppaged("\n\n".join(output))

    @log_command
    @cmd2.with_category(METADATA_COMMANDS)
    @cmd2.with_argparser(_parse_list_schemas)  # type: ignore
    def do_list_schemas(self, args):
        """List all available schemas for a data set within the given start and end dates."""
        try:
            result = self.historical_client.metadata.list_schemas(
                dataset=args.dataset,
                start=args.start,
                end=args.end,
            )
        except BentoError as exc:
            self.perror(f"ERROR: {str(exc)}")
            _LOG.exception(exc)
        else:
            self.columnize(result)

    @log_command
    @cmd2.with_category(METADATA_COMMANDS)
    @cmd2.with_argparser(_parse_list_unit_prices)  # type: ignore
    def do_list_unit_prices(self, args):
        """List unit prices per GB for a dataset"""
        try:
            result = self.historical_client.metadata.list_unit_prices(
                dataset=args.dataset,
                mode=args.mode,
                schema=args.schema,
            )
        except BentoError as exc:
            self.perror(f"ERROR: {str(exc)}")
            _LOG.exception(exc)
        else:
            if isinstance(result, float):
                # If we only have one price just print it.
                self.poutput(result)
            else:
                output = []
                for mode, unit_prices in result.items():
                    if args.mode is None:
                        output.append(f"--- {mode} ---")
                    output.append(
                        tabulate(
                            tabular_data=[
                                [k, v] for k, v in unit_prices.items()
                            ],
                            floatfmt=".2f",
                            headers=["field", "unit_price"],
                        )
                    )
                self.ppaged("\n\n".join(output))
