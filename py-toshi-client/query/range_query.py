from operator import xor
from typing import Optional

from models.query import Query


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
        if xor(gt is not None, gte is not None) and xor(
            lt is not None, lte is not None
        ):
            self._gte = gte
            self._lte = lte
            self._lt = lt
            self._gt = gt
            self._field_name = field_name
            self._limit = limit
        else:
            raise ValueError(
                "RangeQuery only supports either gte or gt and lte or lt, but not setting gte and gt or lte and lt."
            )

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
