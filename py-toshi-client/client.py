from typing import Optional, Type

import requests

from errors import ToshiIndexError, ToshiDocumentError
from schemas.document import Document
from schemas.index import Index
from schemas.index_summary import IndexSummary


class ToshiClient:
    def __init__(self, url: str):
        self._url = url

    def create_index(self, index: Index):
        create_index_url = f"{self._url}/{index.name}/_create"
        resp = requests.put(create_index_url, json=index.to_json())

        if resp.status_code != 201:
            raise ToshiIndexError(
                f"Creating index failed with status code: {resp.status_code}. "
                f"Reason: {resp.json()['message']}"
            )

    def get_index_summary(
        self, name: str, include_size: Optional[bool] = True
    ) -> IndexSummary:
        index_summary_url = f"{self._url}/{name}/_summary?include_sizes={include_size}"
        resp = requests.get(index_summary_url)

        if resp.status_code != 200:
            raise ToshiIndexError(
                f"Could not get index summary. Status code: {resp.status_code}. "
                f"Reason: {resp.json()['message']}"
            )

        return IndexSummary.from_json(index_name=name, data=resp.json()["summaries"])

    def add_document(self, document: Document):
        index_url = f"{self._url}/{document.index_name}/"
        headers = {"Content-Type": "application/json"}
        resp = requests.put(index_url, headers=headers, json=document.to_json())

        if resp.status_code != 201:
            raise ToshiDocumentError(
                f"Could not add document for index {document.index_name}. Status code: {resp.status_code}. "
                f"Reason: {resp.json()['message']}"
            )

    def get_documents(
        self, index_name: str, document: Type[Document]
    ) -> list[Document]:
        index_url = f"{self._url}/{index_name}/"
        resp = requests.get(index_url)

        if resp.status_code != 200:
            raise ToshiDocumentError(
                f"Could not get documents for index {index_name}. Status code: {resp.status_code}. "
                f"Reason: {resp.json()['message']}"
            )
        data = resp.json()
        documents = []
        for doc in data["docs"]:
            documents.append(document(**doc["doc"], index_name=index_name))
        return documents

    def list_indexes(self) -> list[str]:
        list_index_url = f"{self._url}/_list/"
        resp = requests.get(list_index_url)

        if resp.status_code != 200:
            raise ToshiIndexError(
                f"Could not list indexes. Status code: {resp.status_code}. "
                f"Reason: {resp.json()['message']}"
            )

        return resp.json()


class AsyncToshiClient:
    pass
