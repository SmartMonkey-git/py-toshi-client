from abc import ABC, abstractmethod
from copy import copy


class Document(ABC):

    @staticmethod
    @abstractmethod
    def index_name() -> str:
        raise NotImplementedError

    def to_json(self, commit: bool = False) -> dict:
        raw_data = copy(vars(self))

        json_data = dict({"commit": commit})
        document = {"document": raw_data}
        json_data.update(document)
        return json_data
