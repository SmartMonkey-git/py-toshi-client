import dataclasses
from dataclasses import dataclass

from enums import IndexTypes
from schemas.field_options import Options


@dataclass
class Field:
    name: str
    type: IndexTypes
    options: Options


@dataclass
class Index:
    configuration: list[Field]

    def to_json(self) -> list[dict]:
        return [dataclasses.asdict(i) for i in self.configuration]

    @staticmethod
    def from_json(data: dict) -> "Index":
        config = []
        for schema in data:
            options = Options(**schema.pop("options"))
            config.append(Field(options=options, **schema))

        return Index(config)
