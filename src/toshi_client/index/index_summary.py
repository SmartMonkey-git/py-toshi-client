from dataclasses import dataclass

from toshi_client.index.enums import IndexFieldTypes
from toshi_client.index.field_options import TextOptionIndexing
from toshi_client.index.index import Index
from toshi_client.index.index_builder import IndexBuilder


@dataclass
class IndexSettings:
    docstore_blocksize: int
    docstore_compression: str


@dataclass
class IndexSummary:
    index_settings: IndexSettings
    segments: list
    opstamp: int
    index: Index

    @staticmethod
    def from_json(index_name: str, data: dict) -> "IndexSummary":
        builder = IndexBuilder()
        index_schema = data.pop("schema")
        for raw_field in index_schema:
            # can't have type as an input without shadowing the type keyword
            raw_field["index_type"] = raw_field.pop("type")

            if raw_field["index_type"] == IndexFieldTypes.TEXT:
                options = raw_field.pop("options")
                options.pop("fast")
                options["indexing"] = TextOptionIndexing(**options["indexing"])
                raw_field.pop("index_type")

                builder.add_text_field(**raw_field, **options)
            elif raw_field["index_type"] in [
                IndexFieldTypes.I64,
                IndexFieldTypes.U64,
                IndexFieldTypes.F64,
                IndexFieldTypes.BOOL,
            ]:
                options = raw_field.pop("options")
                builder.add_numeric_field(**raw_field, **options)
            elif raw_field["index_type"] == IndexFieldTypes.FACET:
                options = raw_field.pop("options")
                raw_field.pop("index_type")
                builder.add_facet_field(**raw_field, **options)
                pass

        index = builder.build(index_name)
        settings = IndexSettings(**data.pop("index_settings"))
        return IndexSummary(**data, index_settings=settings, index=index)
