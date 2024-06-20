from typing import Optional

from enums import IndexTypes
from schemas.field_options import NumericOptions, TextOptions
from schemas.index import IndexField, Index


class IndexBuilder:
    def __init__(self):
        self._raw_index = []

    def build(self) -> Index:
        """TODO"""
        return Index(self._raw_index.copy())

    def add_text_field(
        self,
        name: str,
        stored: bool,
        indexed: Optional[bool] = False,
        indexing: Optional[dict] = None,
        coerce: Optional[bool] = False,
    ):
        """Adds a text field to the index"""
        option = TextOptions(
            stored=stored, indexed=indexed, indexing=indexing, coerce=coerce
        )
        filed = IndexField(name=name, type=IndexTypes.TEXT, options=option)
        self._raw_index.append(filed)

    def add_u64_field(
        self,
        name: str,
        stored: bool,
        indexed: Optional[bool] = None,
        indexing: Optional[dict] = None,
        fast: Optional[bool] = False,
        fieldnorms: Optional[bool] = True,
        coerce: Optional[bool] = False,
    ):
        """"""
        self.add_numeric_field(
            name, stored, IndexTypes.U64, indexed, indexing, fast, fieldnorms, coerce
        )

    def add_i64_field(
        self,
        name: str,
        stored: bool,
        indexed: Optional[bool] = None,
        indexing: Optional[dict] = None,
        fast: Optional[bool] = False,
        fieldnorms: Optional[bool] = True,
        coerce: Optional[bool] = False,
    ):
        """"""
        self.add_numeric_field(
            name, stored, IndexTypes.I64, indexed, indexing, fast, fieldnorms, coerce
        )

    def add_f64_filed(
        self,
        name: str,
        stored: bool,
        indexed: Optional[bool] = None,
        indexing: Optional[dict] = None,
        fast: Optional[bool] = False,
        fieldnorms: Optional[bool] = True,
        coerce: Optional[bool] = False,
    ):
        """"""
        self.add_numeric_field(
            name, stored, IndexTypes.F64, indexed, indexing, fast, fieldnorms, coerce
        )

    def add_bool_filed(
        self,
        name: str,
        stored: bool,
        indexed: Optional[bool] = None,
        indexing: Optional[dict] = None,
        fast: Optional[bool] = False,
        fieldnorms: Optional[bool] = True,
        coerce: Optional[bool] = False,
    ):
        """"""
        self.add_numeric_field(
            name, stored, IndexTypes.BOOL, indexed, indexing, fast, fieldnorms, coerce
        )

    def add_numeric_field(
        self,
        name: str,
        stored: bool,
        index_type: IndexTypes,
        indexed: Optional[bool] = None,
        indexing: Optional[dict] = None,
        fast: Optional[bool] = False,
        fieldnorms: Optional[bool] = True,
        coerce: Optional[bool] = False,
    ):
        option = NumericOptions(
            stored=stored,
            indexed=indexed,
            indexing=indexing,
            # fast=fast,
            fieldnorms=fieldnorms,
            coerce=coerce,
        )
        filed = IndexField(name=name, type=type, options=option)
        self._raw_index.append(filed)
