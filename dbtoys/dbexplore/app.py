"""The dbexplore application"""
import functools
import logging
import logging.config
import sys
from pprint import pformat
from typing import Callable

import cmd2
import databento
import humanize
from colorama import Fore
from databento.historical.error import BentoError
from tabulate import tabulate

import dbtoys.utilities.key
import dbtoys.utilities.logging
import dbtoys.utilities.parser
from dbtoys.dbexplore import command_parsers

_LOG = logging.getLogger()
_PROG = "dbexplore"


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
    @cmd2.with_argparser(command_parsers._parse_get_billable_size)  # type: ignore
    def do_get_billable_size(self, args):
        """Gets the size in bytes of timeseries data."""
        try:
            result = self.historical_client.metadata.get_billable_size(
                dataset=args.dataset,
                symbols=args.symbols.split(","),
                schema=args.schema,
                encoding=args.encoding,
                start=args.start,
                end=args.end,
            )
        except BentoError as exc:
            self.perror(f"ERROR: {str(exc)}")
            _LOG.exception(exc)
        else:
            formatted = [str(result), humanize.naturalsize(result)]
            self.columnize(formatted)

    @log_command
    @cmd2.with_category(METADATA_COMMANDS)
    @cmd2.with_argparser(command_parsers._parse_get_cost)  # type: ignore
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
    @cmd2.with_argparser(command_parsers._parse_get_shape)  # type: ignore
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
    @cmd2.with_argparser(command_parsers._parse_list_datasets)  # type: ignore
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
    @cmd2.with_argparser(command_parsers._parse_list_fields)  # type: ignore
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
    @cmd2.with_argparser(command_parsers._parse_list_schemas)  # type: ignore
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
    @cmd2.with_argparser(command_parsers._parse_list_unit_prices)  # type: ignore
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
