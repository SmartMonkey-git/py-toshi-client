from typing import Optional

from models.query import Query


class TermQuery(Query):

    def __init__(self, term: str, field_name: str, limit: Optional[int] = None):
        self._term = term
        self._field_name = field_name
        self._limit = limit

    def specialized_weight(self, enable_scoring: bool) -> "TermWeight":
        # implementation: https://github.com/quickwit-oss/tantivy/blob/e4538481341a89001de8082d651ed256ce946fb8/src/query/term_query/term_query.rs#L69
        raise NotImplementedError

    def to_json(self) -> dict:
        query_json = {"query": {"term": {self._field_name: self._term}}}
        if self._limit is not None:
            query_json.update({"limit": self._limit})

        return query_json
