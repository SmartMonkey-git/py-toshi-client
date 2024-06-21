from dataclasses import dataclass

from enums import IndexTypes
from index_builder import IndexBuilder
from schemas.field_options import TextOptionIndexing
from schemas.index import Index


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
    def from_json(data: dict) -> "IndexSummary":
        builder = IndexBuilder()
        index_schema = data.pop("schema")
        for raw_field in index_schema:
            if raw_field["type"] == IndexTypes.TEXT:
                raw_field.pop("type")
                options = raw_field.pop("options")
                options.pop(
                    "fast"
                )  # TODO: Is this a bug? TextFields should not have a fast field
                options["indexing"] = TextOptionIndexing(**options["indexing"])
                builder.add_text_field(**raw_field, **options)
            elif raw_field["type"] in [
                IndexTypes.I64,
                IndexTypes.U64,
                IndexTypes.F64,
                IndexTypes.BOOL,
            ]:
                options = raw_field.pop("options")

                # can't have type as an input without shadowing the type keyword
                raw_field["index_type"] = raw_field.pop("type")
                builder.add_numeric_field(**raw_field, **options)

        index = builder.build()
        settings = IndexSettings(**data.pop("index_settings"))
        return IndexSummary(**data, index_settings=settings, index=index)
