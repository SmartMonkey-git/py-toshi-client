from typing import Optional

import requests

from errors import IndexException
from schemas.index import Index
from schemas.index_summary import IndexSummary


class ToshiClient:
    def __init__(self, url: str):
        self._url = url

    def create_index(self, name: str, create_index_payload: Index):
        create_index_url = f"{self._url}/{name}/_create"
        resp = requests.put(create_index_url, json=create_index_payload.to_json())

        if resp.status_code != 201:
            raise IndexException(
                f"Creating index failed with status code: {resp.status_code}."
                f"Reason: {resp.json()['message']}"
            )

    def get_index_summary(
        self, name: str, include_size: Optional[bool] = True
    ) -> IndexSummary:
        index_summary_url = f"{self._url}/{name}/_summary?include_sizes={include_size}"
        resp = requests.get(index_summary_url)

        if resp.status_code != 200:
            raise IndexException(
                f"Could not get index summary. Status code: {resp.status_code}."
                f"Reason: {resp.json()['message']}"
            )

        return IndexSummary.from_json(resp.json()["summaries"])


class AsyncToshiClient:
    pass
