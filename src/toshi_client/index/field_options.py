from dataclasses import dataclass
from typing import Optional

from toshi_client.index.enums import IndexRecordOption
from toshi_client.models.field_options import Options


@dataclass
class NumericOptions(Options):
    # fast: Optional[bool] = False
    fieldnorms: Optional[bool] = True
    """This attribute only has an effect if indexed is true."""
    coerce: Optional[bool] = False


@dataclass
class TextOptionIndexing:
    record: Optional[IndexRecordOption] = IndexRecordOption.POSITION
    fieldnorms: bool = True
    tokenizer: Optional[str] = "default"


@dataclass
class TextOptions(Options):
    indexing: Optional[TextOptionIndexing] = None
    coerce: Optional[bool] = False
    """If true coerce values into string if they are not of type string"""


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


class FacetOptions(Options):
    pass
