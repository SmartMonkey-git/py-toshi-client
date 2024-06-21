from abc import ABC
from copy import copy
from typing import Optional


class Document(ABC):

    def __init__(self, index_name: str, options: Optional[dict] = None):
        self.index_name = index_name

        if options is None:
            options = {"commit": False}
        self.options = options

    def to_json(self) -> dict:
        raw_data = copy(vars(self))
        raw_data.pop("index_name")
        options = {"options": raw_data.pop("options")}
        document = {"document": raw_data}

        json_data = dict(options)
        json_data.update(document)
        return json_data
