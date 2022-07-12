#!/usr/bin/python3
"""A readline databento dataset explorer."""
import logging
import logging.config
import sys
from typing import Dict

import dbtoys.utilities.app
import dbtoys.utilities.key
import dbtoys.utilities.logging
from dbtoys.dbexplore.app import DataBentoExplorer

_LOG = logging.getLogger()
_PROG = "dbexplore"


# class ExplorerApp:
#    """This class uses the databento API to process dispatched commands."""
#
#    def __init__(self, api_key: str):
#        # We can potentially change the key here. Do we care?
#        self.client = databento.Historical(key=api_key)
#
#    @lru_cache(maxsize=1)
#    def list_datasets(self) -> Result:
#        """Handler for the list_datasets explorer command."""
#        try:
#            _LOG.debug("Processing list_datasets command")
#            results = self.client.metadata.list_datasets()
#        except Exception as exc:  # pylint: disable=broad-except
#            _LOG.exception(exc)
#            return Result(False, str(exc))
#        else:
#            _LOG.debug("Command returned: %s", results)
#            return Result(True, "\n".join(results))
#
#    @lru_cache
#    def list_fields(
#        self, dataset: str, schema: str, encoding: Optional[str] = None
#    ) -> Result:
#        """Hander for the list_fields explorer command"""
#        try:
#            _LOG.debug("Processing list_fields command")
#            results = self.client.metadata.list_fields(
#                dataset=dataset,
#                schema=databento.common.enums.Schema(schema),
#                encoding=encoding,
#            )
#        except Exception as exc:  # pylint: disable=broad-except
#            _LOG.exception(exc)
#            return Result(False, str(exc))
#        else:
#            _LOG.debug("Command resturned: %s", results)
#            formatted_result = []
#            for enc, fields in results.items():
#                formatted_result.append(enc)
#                formatted_result.append(pprint(fields))
#            return Result(True, formatted_result)
#
#    @lru_cache
#    def list_schemas(
#        self,
#        dataset: str,
#        start_date: Optional[str] = None,
#        end_date: Optional[str] = None,
#    ) -> Result:
#        """Hander for the list_schemas explorer command."""
#        try:
#            _LOG.debug(
#                "Processing list_schemas %s %s %s",
#                dataset,
#                start_date,
#                end_date,
#            )
#            results = self.client.metadata.list_schemas(
#                dataset, start_date, end_date
#            )
#        except databento.historical.error.BentoError as exc:
#            _LOG.exception(exc)
#            return Result(False, str(exc))
#        else:
#            _LOG.debug("Command returned: %s", results)
#            return Result(True, "\n".join(results))
#
#    def quit(self):
#        """Handler for the quit explorer command."""
#        _LOG.info("Quit command received")
#        sys.exit(0)
#
#
# class ExplorerDispatch(cmd.Cmd):
#    """Readline interpreter for the dbexplore toy."""
#
#    intro = None
#    prompt = f"{Fore.MAGENTA}{Style.BRIGHT}>> {Style.RESET_ALL}"
#    file = None
#
#    def __init__(self, api_key: str):
#        super().__init__()
#        self.app = ExplorerApp(api_key)
#
#    def do_list_datasets(self, _):
#        """Calls the ExplorerApp to process list_datasets"""
#        result = self.app.list_datasets()
#        if result.nominal:
#            print(result.data)
#        else:
#            print(f"ERROR: {result.data}")
#
#    def do_list_fields(self, args):
#        """list_fields
#        List all field names of a databento dataset"""
#        result = self.app.list_fields(*args.split(" "))
#        if result.nominal:
#            print(result.data)
#        else:
#            print(f"ERROR: {result.data}")
#
#    def do_list_schemas(self, args):
#        """list_schemas dataset start end
#        list all available schemas for a data set within the given start and end dates"""
#        result = self.app.list_schemas(*args.split(" "))
#        if result.nominal:
#            print(result.data)
#        else:
#            print(f"ERROR: {result.data}")
#
#    def do_quit(self, _):
#        """quit
#        end the readline interpreter session"""
#        self.app.quit()
#
#    @staticmethod
#    def _split_args(args: str, nargs: int) -> Tuple[str, ...]:
#        return tuple(arg for arg in args.split(" ", maxsplit=nargs))
#
#    @staticmethod
#    def _handle_command_error(exc: Exception, func_handle: Callable):
#        _LOG.error(
#            "Encountered error while running %s",
#            func_handle.__name__,
#            exc_info=exc,
#        )
#        print(f"ERROR: {exc}")
#        print(f"{func_handle.__doc__}")
#
#
def _parse_args(*args) -> Dict:
    """"""
    parser = dbtoys.utilities.app.ToyParser(
        prog=_PROG,
        description="tool for exploring databento data sets",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-c",
        "--cantrip",
        action="store_true",
        help="execute commands from stdin and then exit",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enables printing of the log to stderr",
    )
    return dict(vars(parser.parse_args(*args)).items())


def main(cantrip: bool, verbose: bool):
    """Runs the toy dbexplore.
    :param cantrip: Read all commands from stdin and then exit.
    :param list_datasets: print a list of datasets; skips entering the readline interface.
    :param verbose: Enables printing of log records to stderr.
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
        explorer.cmdloop()
    except Exception as exc:
        _LOG.exception("Terminating due to unhandled %s!", exc.__class__)
        raise exc


main(**_parse_args(sys.argv[1:]))
