from enum import Enum


class IndexFieldTypes(str, Enum):
    TEXT = "text"
    U64 = "u64"
    I64 = "i64"
    F64 = "f64"
    BOOL = "bool"
    FACET = "facet"


class IndexRecordOption(str, Enum):
    """
    Enum representing the indexing options for a text field.
    """

    BASIC = "basic"
    """Records only the `DocId`s"""
    FREQ = "freq"
    """
    Records the document ids as well as the term frequency.
    The term frequency can help giving better scoring of the documents.
    """
    POSITION = "position"
    """
    Records the document id, the term frequency, and the positions of the occurrences in the document.
    Positions are required to run a `PhraseQuery`.
    """
