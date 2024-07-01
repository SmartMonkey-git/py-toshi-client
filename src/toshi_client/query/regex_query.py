from typing import Optional

from toshi_client.models.query import Query


class RegexQuery(Query):

    def __init__(self, regex: str, field_name: str, limit: Optional[int] = None):
        super().__init__(field_name, limit)
        self._regex = regex

    def to_json(self) -> dict:
        query_json = {"query": {"regex": {self._field_name: self._regex}}}
        if self._limit is not None:
            query_json.update({"limit": self._limit})

        return query_json
