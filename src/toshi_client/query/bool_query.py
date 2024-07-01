from typing import Optional, Self

from toshi_client.models.query import Query


class BoolQuery(Query):

    def __init__(
        self,
        field_name: str = None,  # Not needed, but present in parent
        limit: Optional[int] = None,
    ):
        super().__init__(field_name, limit)
        self._must = []
        self._must_not = []
        self._should = []

    def must_match(self, query: Query) -> Self:
        """Adds a Query that must match on a document to return it"""
        self._must.append(query)
        return self

    def must_not_match(self, query: Query) -> Self:
        """Adds a Query that must not match on a document to return it"""
        self._must_not.append(query)
        return self

    def should_match(self, query: Query) -> Self:
        """Adds a Query that should on a document to return it"""
        self._should.append(query)
        return self

    def to_json(self) -> dict:
        subqueries = dict()

        subqueries["must"] = [m.to_json()["query"] for m in self._must]
        subqueries["must_not"] = [m.to_json()["query"] for m in self._must_not]
        subqueries["should"] = [s.to_json()["query"] for s in self._should]

        query_json = {"query": {"bool": subqueries}}

        if self._limit is not None:
            query_json.update({"limit": self._limit})

        return query_json
