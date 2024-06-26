from typing import Optional

from models.query import Query


class FuzzyQuery(Query):

    def __init__(
        self,
        term: str,
        distance: int,
        transposition: bool,
        field_name: str,
        limit: Optional[int] = None,
    ):
        if distance > 255 or distance < 0:
            raise ValueError("distance needs to be a u8.")

        self._term = term
        self._field_name = field_name
        self._distance = distance
        self._transposition = transposition
        self._limit = limit

    def to_json(self) -> dict:
        query_json = {
            "query": {
                "fuzzy": {
                    self._field_name: {
                        "value": self._term,
                        "distance": self._distance,
                        "transposition": self._transposition,
                    }
                }
            }
        }
        if self._limit is not None:
            query_json.update({"limit": self._limit})

        return query_json
