import pytest

from index import IndexBuilder
from index import IndexField
from index.enums import IndexTypes, IndexRecordOption
from index.field_options import TextOptionIndexing, TextOptions, NumericOptions


@pytest.fixture
def index_builder():
    return IndexBuilder()


def test_add_text_field(index_builder: IndexBuilder):
    index_builder.add_text_field(
        name="test",
        stored=True,
        indexing=TextOptionIndexing(record=IndexRecordOption.POSITION),
        indexed=True,
        coerce=True,
    )

    index = index_builder.build("test_index")

    assert index.name == "test_index"
    assert len(index.fields) == 1
    assert index.fields[0] == IndexField(
        name="test",
        type=IndexTypes.TEXT,
        options=TextOptions(
            stored=True,
            indexed=True,
            coerce=True,
            indexing=TextOptionIndexing(record=IndexRecordOption.POSITION),
        ),
    )


def test_add_i64_field(index_builder: IndexBuilder):
    index_builder.add_i64_field(
        name="test", stored=True, indexed=True, fast=True, fieldnorms=False, coerce=True
    )

    index = index_builder.build("test_index")

    assert index.name == "test_index"
    assert len(index.fields) == 1
    assert index.fields[0] == IndexField(
        name="test",
        type=IndexTypes.I64,
        options=NumericOptions(
            stored=True, indexed=True, fieldnorms=False, coerce=True
        ),
    )


def test_add_u64_field(index_builder: IndexBuilder):
    index_builder.add_u64_field(
        name="test", stored=True, indexed=True, fast=True, fieldnorms=False, coerce=True
    )

    index = index_builder.build("test_index")

    assert index.name == "test_index"
    assert len(index.fields) == 1
    assert index.fields[0] == IndexField(
        name="test",
        type=IndexTypes.U64,
        options=NumericOptions(
            stored=True, indexed=True, fieldnorms=False, coerce=True
        ),
    )


def test_add_f64_field(index_builder: IndexBuilder):
    index_builder.add_f64_filed(
        name="test", stored=True, indexed=True, fast=True, fieldnorms=False, coerce=True
    )

    index = index_builder.build("test_index")

    assert index.name == "test_index"
    assert len(index.fields) == 1
    assert index.fields[0] == IndexField(
        name="test",
        type=IndexTypes.F64,
        options=NumericOptions(
            stored=True, indexed=True, fieldnorms=False, coerce=True
        ),
    )


def test_add_bool_field(index_builder: IndexBuilder):
    index_builder.add_bool_filed(
        name="test", stored=True, indexed=True, fast=True, fieldnorms=False, coerce=True
    )

    index = index_builder.build("test_index")

    assert index.name == "test_index"
    assert len(index.fields) == 1
    assert index.fields[0] == IndexField(
        name="test",
        type=IndexTypes.BOOL,
        options=NumericOptions(
            stored=True, indexed=True, fieldnorms=False, coerce=True
        ),
    )


@pytest.mark.parametrize(
    "field_type", [IndexTypes.I64, IndexTypes.U64, IndexTypes.F64, IndexTypes.BOOL]
)
def test_add_numeric_field(index_builder: IndexBuilder, field_type):
    index_builder.add_numeric_field(
        name="test",
        index_type=field_type,
        stored=True,
        indexed=True,
        fast=True,
        fieldnorms=False,
        coerce=True,
    )

    index = index_builder.build("test_index")

    assert index.name == "test_index"
    assert len(index.fields) == 1
    assert index.fields[0] == IndexField(
        name="test",
        type=field_type,
        options=NumericOptions(
            stored=True, indexed=True, fieldnorms=False, coerce=True
        ),
    )
