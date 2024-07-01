from typing import Optional, Self

from toshi_client.models.query import Query


class RangeQuery(Query):

    def __init__(
        self,
        field_name: str,
        gte: Optional[int] = None,
        lte: Optional[int] = None,
        lt: Optional[int] = None,
        gt: Optional[int] = None,
        limit: Optional[int] = None,
    ):
        super().__init__(field_name, limit)
        self._gte = gte
        self._lte = lte
        self._lt = lt
        self._gt = gt

    def gte(self, val: int) -> Self:
        self._gte = val
        return self

    def gt(self, val: int) -> Self:
        self._gt = val
        return self

    def lte(self, val: int) -> Self:
        self._lte = val
        return self

    def lt(self, val: int) -> Self:
        self._lt = val
        return self

    def to_json(self) -> dict:
        range_data = {}
        if self._gte is not None:
            range_data["gte"] = self._gte
        if self._gt is not None:
            range_data["gt"] = self._gt
        if self._lte is not None:
            range_data["lte"] = self._lte
        if self._lt is not None:
            range_data["lt"] = self._lt

        query_json = {"query": {"range": {self._field_name: range_data}}}
        if self._limit is not None:
            query_json.update({"limit": self._limit})

        return query_json
