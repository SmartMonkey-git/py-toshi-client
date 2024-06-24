import dataclasses
from dataclasses import dataclass

from enums import IndexTypes
from models.field_options import Options


@dataclass
class IndexField:
    name: str
    type: IndexTypes
    options: Options


@dataclass
class Index:
    name: str
    fields: list[IndexField]

    def to_json(self) -> list[dict]:
        return [dataclasses.asdict(i) for i in self.fields]
