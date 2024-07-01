import dataclasses
from dataclasses import dataclass

from toshi_client.index.enums import IndexFieldTypes
from toshi_client.models.field_options import Options


@dataclass
class IndexField:
    name: str
    type: IndexFieldTypes
    options: Options


@dataclass
class Index:
    name: str
    fields: list[IndexField]

    def to_json(self) -> list[dict]:
        return [dataclasses.asdict(i) for i in self.fields]
