import dataclasses
from typing import Optional

from models.query import Query


@dataclasses.dataclass
class BoolQueryBundle:
    """Helper class to construct a boolean query"""

    must: Optional[list[Query]] = dataclasses.field(default_factory=list)
    """Queries that must match"""
    must_not: Optional[list[Query]] = dataclasses.field(default_factory=list)
    """Queries that do not match"""
    should: Optional[list[Query]] = dataclasses.field(default_factory=list)
    """Queries that should match"""

    def to_json(self):
        json_data = dict()

        json_data["must"] = [m.to_json()["query"] for m in self.must]
        json_data["must_not"] = [m.to_json()["query"] for m in self.must_not]
        json_data["should"] = [s.to_json()["query"] for s in self.should]

        return json_data


class BoolQuery(Query):

    def __init__(
        self,
        bool_query_bundle: BoolQueryBundle,
        field_name: str = None,  # Not needed, but present in parent
        limit: Optional[int] = None,
    ):
        super().__init__(field_name, limit)
        self._bundle = bool_query_bundle

    def to_json(self):
        query_json = {"query": {"bool": self._bundle.to_json()}}

        if self._limit is not None:
            query_json.update({"limit": self._limit})

        return query_json
