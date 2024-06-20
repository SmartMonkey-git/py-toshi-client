from dataclasses import dataclass

from enums import IndexTypes
from index_builder import IndexBuilder
from schemas.index import Index


@dataclass
class IndexSummary:
    index_settings: dict
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
                builder.add_text_field(**raw_field, **options)
            elif raw_field["type"] in [
                IndexTypes.I64,
                IndexTypes.U64,
                IndexTypes.F64,
                IndexTypes.BOOL,
            ]:
                options = raw_field.pop("options")
                builder.add_numeric_field(**raw_field, **options)

        index = builder.build()
        return IndexSummary(**data, index=index)