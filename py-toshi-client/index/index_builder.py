from typing import Optional

from enums import IndexTypes
from index.field_options import TextOptionIndexing, TextOptions, NumericOptions
from index.index import IndexField, Index


class IndexBuilder:
    def __init__(self):
        self._raw_index = []

    def build(self, index_name: str) -> Index:
        """TODO"""
        return Index(fields=self._raw_index.copy(), name=index_name)

    def add_text_field(
        self,
        name: str,
        stored: bool,
        indexed: Optional[bool] = False,
        indexing: Optional[TextOptionIndexing] = None,
        coerce: Optional[bool] = False,
    ):
        """
        Adds a text field to the index.

        Parameters
        ----------
        name : str
            The name of the text field.
        stored : bool
            If True, the text field will be stored in the index.
        indexed : bool, optional
            If True, the text field will be indexed for searching. Default is False.
        indexing : dict, optional
            A dictionary containing indexing options for the text field. Default is None.
        coerce : bool, optional
            If true, coerce values into string if they are not of type string Default is False.
        """

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
        fast: Optional[bool] = False,
        fieldnorms: Optional[bool] = True,
        coerce: Optional[bool] = False,
    ):
        """"""
        self.add_numeric_field(
            name, stored, IndexTypes.U64, indexed, fast, fieldnorms, coerce
        )

    def add_i64_field(
        self,
        name: str,
        stored: bool,
        indexed: Optional[bool] = None,
        fast: Optional[bool] = False,
        fieldnorms: Optional[bool] = True,
        coerce: Optional[bool] = False,
    ):
        """"""
        self.add_numeric_field(
            name, stored, IndexTypes.I64, indexed, fast, fieldnorms, coerce
        )

    def add_f64_filed(
        self,
        name: str,
        stored: bool,
        indexed: Optional[bool] = None,
        fast: Optional[bool] = False,
        fieldnorms: Optional[bool] = True,
        coerce: Optional[bool] = False,
    ):
        """"""
        self.add_numeric_field(
            name, stored, IndexTypes.F64, indexed, fast, fieldnorms, coerce
        )

    def add_bool_filed(
        self,
        name: str,
        stored: bool,
        indexed: Optional[bool] = None,
        fast: Optional[bool] = False,
        fieldnorms: Optional[bool] = True,
        coerce: Optional[bool] = False,
    ):
        """"""
        self.add_numeric_field(
            name, stored, IndexTypes.BOOL, indexed, fast, fieldnorms, coerce
        )

    def add_numeric_field(
        self,
        name: str,
        stored: bool,
        index_type: IndexTypes,
        indexed: Optional[bool] = None,
        fast: Optional[bool] = False,
        fieldnorms: Optional[bool] = True,
        coerce: Optional[bool] = False,
    ):
        option = NumericOptions(
            stored=stored,
            indexed=indexed,
            # fast=fast,
            fieldnorms=fieldnorms,
            coerce=coerce,
        )
        filed = IndexField(name=name, type=index_type, options=option)
        self._raw_index.append(filed)
