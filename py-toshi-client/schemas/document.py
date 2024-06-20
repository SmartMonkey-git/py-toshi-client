from abc import ABC


class Document(ABC):

    def __init__(self, index_name: str, options=None):
        self.index_name = index_name
        self.options = {"commit": False}

    def to_json(self) -> dict:
        raw_data = vars(self)
        raw_data.pop("index_name")
        options = {"options": raw_data.pop("options")}
        document = {"document": raw_data}

        json_data = dict(options)
        json_data.update(document)
        return json_data

    def from_json(self):
        pass
