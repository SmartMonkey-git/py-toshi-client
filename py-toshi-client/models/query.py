from abc import ABC, abstractmethod
from typing import Optional


class Query(ABC):

    def __init__(self, field_name: str, limit: Optional[int] = None):
        self._field_name = field_name
        self._limit = limit

    @abstractmethod
    def to_json(self) -> dict:
        pass
