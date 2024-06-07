import requests

from errors import IndexCreationError


class ToshiClient:
    def __init__(self, url: str):
        self._url = url

    def create_index(self, name: str, create_index_payload: list[dict]):
        create_index_url = f"{self._url}/{name}/_create"
        resp = requests.put(create_index_url, json=create_index_payload)

        if resp.status_code != 201:
            raise IndexCreationError(
                f"Creating index failed with status code: {resp.status_code}."
                f"Reason: {resp.json()['message']}"
            )


class AsyncToshiClient:
    pass
