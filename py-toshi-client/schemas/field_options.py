from dataclasses import dataclass
from typing import Optional

from enums import IndexTypes


@dataclass
class Options:
    stored: bool
    indexed: Optional[bool] = None
    indexing: Optional[dict] = None


@dataclass
class NumericOptions(Options):
    # fast: Optional[bool] = False
    fieldnorms: Optional[bool] = True
    """This attribute only has an effect if indexed is true."""
    coerce: Optional[bool] = False


@dataclass
class TextOptions(Options):
    coerce: Optional[bool] = False


@dataclass
class ByteOptions(Options):
    fast: Optional[bool] = False
    fieldnorms: Optional[bool] = True
    """This boolean has no effect if the field is not marked as indexed true."""


@dataclass
class DateOptions(Options):
    fieldnorms: Optional[bool] = False
    """This boolean has no effect if the field is not marked as indexed true."""
    fast: Optional[bool] = False
    precision: Optional[str] = "seconds"
    """Precision of the date can be seconds, milliseconds, microseconds, nanoseconds"""
