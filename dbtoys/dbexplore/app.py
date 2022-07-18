"""The dbexplore application"""
import functools
import logging
from pprint import pformat
from typing import Callable
from typing import Tuple

import cmd2
import databento
import pandas
from colorama import Fore
from databento.common.enums import Dataset
from databento.common.enums import Encoding
from databento.common.enums import Schema
from databento.historical.error import BentoError

_LOG = logging.getLogger()


_KNOWN_DATASETS: Tuple[str, ...] = tuple(x.value for x in Dataset)
_KNOWN_SCHEMAS: Tuple[str, ...] = tuple(x.value for x in Schema)
_KNOWN_ENCODINGS: Tuple[str, ...] = tuple(x.value for x in Encoding)

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
            # TODO: Need to make these json outputs more readable.
            self.poutput(pformat(result))

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
