"""The dbexplore application"""
import functools
import logging
import logging.config
import sys
from pprint import pformat
from typing import Callable
from typing import Dict
from typing import Tuple

import cmd2
import databento
import pandas
from colorama import Fore
from databento.common.enums import Dataset
from databento.common.enums import Encoding
from databento.common.enums import Schema
from databento.historical.error import BentoError
from tabulate import tabulate

import dbtoys.utilities.key
import dbtoys.utilities.logging
import dbtoys.utilities.parser

_LOG = logging.getLogger()
_PROG = "dbexplore"

_KNOWN_DATASETS: Tuple[str, ...] = tuple(x.value for x in Dataset)
_KNOWN_SCHEMAS: Tuple[str, ...] = tuple(x.value for x in Schema)
_KNOWN_ENCODINGS: Tuple[str, ...] = tuple(x.value for x in Encoding)


def _parse_args(*args) -> Dict:
    """"""
    parser = dbtoys.utilities.parser.ToyParser(
        prog=_PROG,
        description="tool for exploring databento data sets",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-c",
        "--cantrip",
        nargs="+",
        type=str,
        help="parse further arguments as a command and exit without entering the read-line interface",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enables printing of the log to stderr",
    )
    return dict(vars(parser.parse_args(*args)).items())


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
_parse_list_datasets: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
_parse_list_datasets.add_argument(
    "start",
    type=pandas.Timestamp.fromisoformat,
    nargs="?",
    metavar="YYYY-MM-DD",
    help="the earlierst date to list datasets from in ISO 8601 format",
    default=pandas.Timestamp.today().date().isoformat(),
)
_parse_list_datasets.add_argument(
    "end",
    type=pandas.Timestamp.fromisoformat,
    nargs="?",
    metavar="YYYY-MM-DD",
    help="the latest date to list datasets from in ISO 8601 format",
    default=pandas.Timestamp.today().date().isoformat(),
)

_parse_list_schemas: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
_parse_list_schemas.add_argument(
    "dataset",
    choices=_KNOWN_DATASETS,
    type=str,
    help="the dataset to query",
)
_parse_list_schemas.add_argument(
    "start",
    type=pandas.Timestamp.fromisoformat,
    nargs="?",
    metavar="YYYY-MM-DD",
    help="the earlierst date to list schemas from in ISO 8601 format",
    default=pandas.Timestamp.today().date().isoformat(),
)
_parse_list_schemas.add_argument(
    "end",
    type=pandas.Timestamp.fromisoformat,
    nargs="?",
    metavar="YYYY-MM-DD",
    help="the latest date to list schemas from in ISO 8601 format",
    default=pandas.Timestamp.today().date().isoformat(),
)

_parse_list_fields: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
_parse_list_fields.add_argument(
    "dataset",
    choices=_KNOWN_DATASETS,
    type=str,
    help="the dataset to query",
)
_parse_list_fields.add_argument(
    "schema",
    choices=_KNOWN_SCHEMAS,
    type=str,
    help="the schema to list fields from",
)
_parse_list_fields.add_argument(
    "encoding",
    choices=_KNOWN_ENCODINGS,
    type=str,
    nargs="?",
    help="the encoding to list fields from",
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

    LIST_COMMANDS: str = "List Commands"

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
    @cmd2.with_category(LIST_COMMANDS)
    def do_list_compressions(self, _):
        """list all compressions"""
        try:
            result = self.historical_client.metadata.list_compressions()
        except BentoError as exc:
            self.perror(f"ERROR: {str(exc)}")
            _LOG.exception(exc)
        else:
            self.columnize(result)

    @log_command
    @cmd2.with_category(LIST_COMMANDS)
    @cmd2.with_argparser(_parse_list_datasets)  # type: ignore
    def do_list_datasets(self, args):
        """Calls the ExplorerApp to process list_datasets"""
        try:
            result = self.historical_client.metadata.list_datasets(
                start=pandas.Timestamp(args.start),
                end=pandas.Timestamp(args.end),
            )
        except BentoError as exc:
            self.perror(f"ERROR: {str(exc)}")
            _LOG.exception(exc)
        else:
            self.columnize(result)

    @log_command
    @cmd2.with_category(LIST_COMMANDS)
    def do_list_encodings(self, _):
        """list all encodings"""
        try:
            result = self.historical_client.metadata.list_encodings()
        except BentoError as exc:
            self.perror(f"ERROR: {str(exc)}")
            _LOG.exception(exc)
        else:
            self.columnize(result)

    @log_command
    @cmd2.with_category(LIST_COMMANDS)
    @cmd2.with_argparser(_parse_list_fields)  # type: ignore
    def do_list_fields(self, args):
        """list all fields from the given dataset and schema"""
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
    @cmd2.with_category(LIST_COMMANDS)
    @cmd2.with_argparser(_parse_list_schemas)  # type: ignore
    def do_list_schemas(self, args):
        """list all available schemas for a data set within the given start and end dates"""
        try:
            result = self.historical_client.metadata.list_schemas(
                dataset=Dataset(args.dataset),
                start=args.start,
                end=args.end,
            )
        except BentoError as exc:
            self.perror(f"ERROR: {str(exc)}")
            _LOG.exception(exc)
        else:
            self.columnize(result)
