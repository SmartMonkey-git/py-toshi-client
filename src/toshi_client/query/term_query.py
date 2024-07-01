from typing import Optional

from toshi_client.models.query import Query


class TermQuery(Query):

    def __init__(self, term: str, field_name: str, limit: Optional[int] = None):
        super().__init__(field_name, limit)
        self._term = term

    def to_json(self) -> dict:
        query_json = {"query": {"term": {self._field_name: self._term}}}
        if self._limit is not None:
            query_json.update({"limit": self._limit})

        return query_json
