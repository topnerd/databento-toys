"""Common path CLI for dbtoys applications."""
import argparse

import dbtoys.utilities.splash


class ToyParser(argparse.ArgumentParser):
    """An argument parser with some DBTOYS defaults."""

    def __init__(self, prog: str, description: str):
        """ """
        super().__init__(
            prog=prog,
            description=dbtoys.utilities.splash.DBTOYS_SPLASH
            + "\n"
            + description,
            add_help=True,
            epilog=dbtoys.utilities.splash.DBTOYS_GOODBYE,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
