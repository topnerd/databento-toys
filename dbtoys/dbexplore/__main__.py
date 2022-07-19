"""A readline databento dataset explorer."""
import logging
import logging.config
import sys
from typing import Dict

import dbtoys.utilities.parser
import dbtoys.utilities.key
import dbtoys.utilities.logging
from dbtoys.dbexplore.app import DataBentoExplorer

_LOG = logging.getLogger()
_PROG = "dbexplore"


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


def main(cantrip: str = "", verbose: bool = False):
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


main(**_parse_args(sys.argv[1:]))
