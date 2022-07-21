"""Entry point for dbclose."""

import sys

import pandas

from dbtoys.dbclose.app import _PROG
from dbtoys.dbclose.app import main
from dbtoys.utilities.parser import ToyParser


def _parse_args(*args):
    """Parses command line arguments for main"""
    parser = ToyParser(
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
        type=pandas.Timestamp.fromisoformat,
        metavar="YYYY-MM-DD",
        help="the date to request in ISO 8601 format",
        default=pandas.Timestamp.today().date().isoformat(),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enables printing of the log to stderr",
    )
    return dict(vars(parser.parse_args(*args)).items())


sys.exit(main(**_parse_args(sys.argv[1:])))
