"""Argument parsers for dbexplore commands."""
from typing import Tuple

import cmd2
import pandas
from databento.common.enums import Compression
from databento.common.enums import Dataset
from databento.common.enums import Encoding
from databento.common.enums import FeedMode
from databento.common.enums import Schema

KNOWN_COMPRESSIONS: Tuple[str, ...] = tuple(x.value for x in Compression)
KNOWN_DATASETS: Tuple[str, ...] = tuple(x.value for x in Dataset)
KNOWN_ENCODINGS: Tuple[str, ...] = tuple(x.value for x in Encoding)
KNOWN_FEED_MODES: Tuple[str, ...] = tuple(x.value for x in FeedMode)
KNOWN_SCHEMAS: Tuple[str, ...] = tuple(x.value for x in Schema)

get_billable_size: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
get_billable_size.add_argument(
    "dataset",
    choices=KNOWN_DATASETS,
    type=str,
    help="the target dataset",
)
get_billable_size.add_argument(
    "symbols", type=str, help="one or more symbols separated by commas"
)
get_billable_size.add_argument(
    "schema",
    choices=KNOWN_SCHEMAS,
    type=str,
    help="a data schema",
)
get_billable_size.add_argument(
    "encoding",
    choices=KNOWN_ENCODINGS,
    type=str,
    help="a data encoding",
)
get_billable_size.add_argument(
    "--start",
    "-s",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the earlierst date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)
get_billable_size.add_argument(
    "--end",
    "-e",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the latest date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)

get_cost: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
get_cost.add_argument(
    "dataset",
    choices=KNOWN_DATASETS,
    type=str,
    help="the target dataset",
)
get_cost.add_argument(
    "symbols", type=str, help="one or more symbols separated by commas"
)
get_cost.add_argument(
    "schema",
    choices=KNOWN_SCHEMAS,
    type=str,
    help="a data schema",
)
get_cost.add_argument(
    "encoding",
    choices=KNOWN_ENCODINGS,
    type=str,
    nargs="?",
    help="a data encoding",
    const=None,
)
get_cost.add_argument(
    "compression",
    choices=KNOWN_COMPRESSIONS,
    type=str,
    nargs="?",
    help="a data compression",
    const=None,
)
get_cost.add_argument(
    "--start",
    "-s",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the earlierst date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)
get_cost.add_argument(
    "--end",
    "-e",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the latest date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)

get_shape: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
get_shape.add_argument(
    "dataset",
    choices=KNOWN_DATASETS,
    type=str,
    help="the target dataset",
)
get_shape.add_argument(
    "symbols", type=str, help="one or more symbols separated by commas"
)
get_shape.add_argument(
    "schema",
    choices=KNOWN_SCHEMAS,
    type=str,
    help="a data schema",
)
get_shape.add_argument(
    "--start",
    "-s",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the earlierst date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)
get_shape.add_argument(
    "--end",
    "-e",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the latest date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)

list_datasets: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
list_datasets.add_argument(
    "--start",
    "-s",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the earlierst date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)
list_datasets.add_argument(
    "--end",
    "-e",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the latest date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)

list_fields: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
list_fields.add_argument(
    "dataset",
    choices=KNOWN_DATASETS,
    type=str,
    help="the target dataset",
)
list_fields.add_argument(
    "schema",
    choices=KNOWN_SCHEMAS,
    type=str,
    help="a data schema",
)
list_fields.add_argument(
    "encoding",
    choices=KNOWN_ENCODINGS,
    type=str,
    nargs="?",
    help="a data encoding",
    const=None,
)

list_schemas: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
list_schemas.add_argument(
    "dataset",
    choices=KNOWN_DATASETS,
    type=str,
    help="the target dataset",
)
list_schemas.add_argument(
    "--start",
    "-s",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the earlierst date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)
list_schemas.add_argument(
    "--end",
    "-e",
    type=pandas.Timestamp.fromisoformat,
    metavar="YYYY-MM-DDTHHMMSS.MMM",
    help="the latest date in ISO 8601 format",
    default=pandas.Timestamp.today().date(),
)

list_unit_prices: cmd2.Cmd2ArgumentParser = cmd2.Cmd2ArgumentParser()
list_unit_prices.add_argument(
    "dataset",
    choices=KNOWN_DATASETS,
    type=str,
    help="the target dataset",
)
list_unit_prices.add_argument(
    "mode",
    choices=KNOWN_FEED_MODES,
    type=str,
    nargs="?",
    help="a feed mode",
    default=None,
)
list_unit_prices.add_argument(
    "schema",
    choices=KNOWN_SCHEMAS,
    type=str,
    nargs="?",
    help="a data schema",
    default=None,
)
