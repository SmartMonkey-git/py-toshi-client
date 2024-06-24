from abc import ABC, abstractmethod


class Query(ABC):

    @abstractmethod
    def to_json(self):
        pass
