#!/usr/bin/python3
"""Returns the close price of a given security."""
import argparse
import logging
import logging.config
import sys
import datetime
from typing import Dict, Iterable, Tuple

try:
    # We wrap third-party imports to avoid naked ImportErrors reaching the user.
    from utilities.logging import DEFAULT_LOG_FILE_PATH
    from utilities.logging import DEFAULT_LOGGING
    from utilities.logging import configure_console_handler
    from utilities.logging import configure_file_logger
    from utilities.splash import DBTOYS_GOODBYE
    from utilities.splash import DBTOYS_SPLASH
    from utilities.key import get_api_key
except ImportError as ie:
    print(f"Missing dependency! {ie.msg}")
    print("Did you forget to install dependencies or activate an environment?")
    sys.exit(1)

_LOG = logging.getLogger()
_DESCRIPTION = f"{DBTOYS_SPLASH}\nReturns the close price of a given symbol."
_EPILOG = f"Log directory: {DEFAULT_LOG_FILE_PATH}\n{DBTOYS_GOODBYE}"
_PROG = "dbclose"


def _parse_args(*args) -> Dict:
    """Parses command line arguments for main"""
    parser = argparse.ArgumentParser(
        prog=_PROG,
        description=_DESCRIPTION,
        epilog=_EPILOG,
        add_help=True,
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
        help="enables printing of the log to stdout",
    )
    return dict(vars(parser.parse_args(*args)).items())


def _do_dbclose(
    api_key: str, symbols: Iterable[str], date: datetime.date
) -> Tuple[float, ...]:
    #  TODO: The actual thing ya idiot.
    return (0.0,)


def main(
    symbols: Iterable[str], date: datetime.date, verbose: bool
) -> Tuple[float, ...]:
    """Runs the toy dbclose.
    :param symbols: One or more symbols to query the close price of.
    :param date: The date of the close price; defaults to most revent.
    :param verbose: Enables printing of log records to stderr.
    :return: The close prices for the symbols in order.
    """
    logging.config.dictConfig(DEFAULT_LOGGING)
    configure_file_logger(logger=_LOG, log_file_name=f"{_PROG}.log")
    _LOG.setLevel("NOTSET")

    if verbose:
        # If the --verbose flag was given we will print log events to stderr.
        configure_console_handler(logger=_LOG, stream=sys.stderr)

    _LOG.debug(
        "Executing %s with arguments: symbols=%s date=%s verbose=%s",
        _PROG,
        symbols,
        date,
        verbose,
    )

    try:
        api_key = get_api_key(prompt_for_key=True)
        return _do_dbclose(api_key=api_key, symbols=symbols, date=date)
    except Exception as exc:
        _LOG.exception("Terminating due to unhandled %s!", exc.__class__)
        raise exc


if __name__ == "__main__":
    main(**_parse_args(sys.argv[1:]))
