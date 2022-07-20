"""Entry point for dbclose."""

import sys

from dbtoys.dbclose.app import _parse_args
from dbtoys.dbclose.app import main

sys.exit(main(**_parse_args(sys.argv[1:])))
