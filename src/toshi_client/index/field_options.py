from dataclasses import dataclass
from typing import Optional

from toshi_client.index.enums import IndexRecordOption
from toshi_client.models.field_options import Options


@dataclass
class NumericOptions(Options):
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


class FacetOptions(Options):
    pass
