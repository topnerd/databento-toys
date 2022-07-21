"""Entry point for dbexplore."""
import sys

from dbtoys.dbexplore.app import _PROG
from dbtoys.dbexplore.app import main
from dbtoys.utilities.parser import ToyParser


def _parse_args(*args):
    """"""
    parser = ToyParser(
        prog=_PROG,
        description="tool for exploring databento data sets",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-c",
        "--cantrip",
        nargs="+",
        type=str,
        help="parse further arguments as a command and without entering the read-line interface",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enables printing of the log to stderr",
    )
    return dict(vars(parser.parse_args(*args)).items())


sys.exit(main(**_parse_args(sys.argv[1:])))
