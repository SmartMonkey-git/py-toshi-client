from abc import ABC, abstractmethod
from copy import copy


class Document(ABC):

    @staticmethod
    @abstractmethod
    def index_name() -> str:
        raise NotImplementedError

    def __eq__(self, other) -> bool:
        return self.to_json() == other.to_json()

    def to_json(self) -> dict:
        raw_data = copy(vars(self))
        json_data = {"document": raw_data}
        return json_data
