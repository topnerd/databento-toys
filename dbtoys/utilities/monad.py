"""A simple monad implementation."""
import dataclasses
from typing import Any


@dataclasses.dataclass
class Result:
    """A result monad.
    If nominal=True, the data field will contain the result of some operation.
    If nominal=False, the data field will contain an error message."""

    nominal: bool
    data: Any
