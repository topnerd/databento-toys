"""Entry point for dbexplore."""
import sys

from dbtoys.dbexplore.app import _parse_args
from dbtoys.dbexplore.app import main

sys.exit(main(**_parse_args(sys.argv[1:])))
