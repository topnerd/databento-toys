"""Common path CLI for dbtoys applications."""
import argparse

import utilities.splash


class ToyParser(argparse.ArgumentParser):
    """An argument parser with some DBTOYS defaults."""

    def __init__(self, prog: str, description: str):
        """ """
        super().__init__(
            prog=prog,
            description=utilities.splash.DBTOYS_SPLASH + "\n" + description,
            add_help=True,
            epilog=utilities.splash.DBTOYS_GOODBYE,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
