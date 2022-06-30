#!/usr/bin/python3
"""Returns the close price of a given security."""
import datetime
import logging
import logging.config
import sys
from typing import Dict
from typing import Iterable
from typing import Tuple

import dbtoys.utilities.app
import dbtoys.utilities.key
import dbtoys.utilities.logging

_LOG = logging.getLogger()
_PROG = "dbclose"


def _parse_args(*args) -> Dict:
    """Parses command line arguments for main"""
    parser = dbtoys.utilities.app.ToyParser(
        prog=_PROG,
        description="Returns the close price of a given symbol.",
    )
    parser.add_argument(
        "symbols",
        nargs="+",
        type=str,
        help="the symbol(s) to request",
    )
    parser.add_argument(
        "-d",
        "--date",
        type=datetime.date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="the date to request in ISO 8601 format",
        default=datetime.datetime.today(),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enables printing of the log to stderr",
    )
    return dict(vars(parser.parse_args(*args)).items())


def main(
    symbols: Iterable[str], date: datetime.date, verbose: bool
) -> Tuple[float, ...]:
    """Runs the toy dbclose.
    :param symbols: One or more symbols to query the close price of.
    :param date: The date of the close price; defaults to most revent.
    :param verbose: Enables printing of log records to stderr.
    :return: The close prices for the symbols in order.
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
        "Executing %s with arguments: symbols=%s date=%s verbose=%s",
        _PROG,
        symbols,
        date,
        verbose,
    )

    try:
        api_key = dbtoys.utilities.key.get_api_key(prompt_for_key=True)
        return _do_dbclose()
    except Exception as exc:
        _LOG.exception("Terminating due to unhandled %s!", exc.__class__)
        raise exc


def _do_dbclose():
    #  TODO: The actual thing ya idiot.
    return (0.0,)


main(**_parse_args(sys.argv[1:]))
