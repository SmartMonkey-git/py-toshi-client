from typing import Optional

from toshi_client.models.query import Query


class PhraseQuery(Query):

    def __init__(
        self,
        field_name: str,
        terms: [str],
        offsets: Optional[list[int]] = None,
        limit=None,
    ):
        super().__init__(field_name, limit)

        if offsets is not None and len(terms) != len(offsets):
            raise ValueError("Offsets and terms must have the same number of entries.")

        self._terms = terms
        self._offsets = offsets

    def to_json(self) -> dict:
        query_json = {
            "query": {
                "phrase": {
                    self._field_name: {
                        "terms": self._terms,
                        "offsets": self._offsets if self._offsets is not None else None,
                    }
                }
            }
        }
        if self._limit is not None:
            query_json.update({"limit": self._limit})

        return query_json
