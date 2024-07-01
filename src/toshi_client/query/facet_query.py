from pathlib import Path

from toshi_client.models.query import Query


class FacetQuery(Query):

    def __init__(self, facet_name: str, facets: list[Path], field_name: str = None):
        super().__init__(field_name)
        self._facet_name = facet_name
        self._facets = facets

    def to_json(self) -> dict:
        return {
            self._facet_name: [
                str(f) if str(f).startswith("/") else "/" + str(f) for f in self._facets
            ]
        }
