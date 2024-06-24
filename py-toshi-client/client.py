import json
from typing import Optional, Type, Union

import requests

from errors import ToshiIndexError, ToshiDocumentError, ToshiFlushError
from query.model import Query
from schemas.document import Document
from schemas.index import Index
from schemas.index_summary import IndexSummary


class ToshiClient:
    def __init__(self, url: str):
        if url.endswith("/"):
            url = url[:-1]
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

    def add_document(self, document: Document, commit: Optional[bool] = False):
        index_url = f"{self._url}/{document.index_name()}/"
        headers = {"Content-Type": "application/json"}
        resp = requests.put(index_url, headers=headers, json=document.to_json(commit))

        if resp.status_code != 201:
            raise ToshiDocumentError(
                f"Could not add document for index {document.index_name()}. Status code: {resp.status_code}. "
                f"Reason: {resp.json()['message']}"
            )

    def get_documents(self, document: Type[Document]) -> list[Document]:
        index_url = f"{self._url}/{document.index_name()}/"
        resp = requests.get(index_url)

        if resp.status_code != 200:
            raise ToshiDocumentError(
                f"Could not get documents for index {document.index_name()}. Status code: {resp.status_code}. "
                f"Reason: {resp.json()['message']}"
            )
        data = resp.json()
        documents = []
        for doc in data["docs"]:
            documents.append(document(**doc["doc"]))
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

    def flush(self, index_name: str):
        index_url = f"{self._url}/{index_name}/_flush/"
        resp = requests.post(index_url)

        if resp.status_code != 200:
            raise ToshiFlushError(f"Could not flush. Status code: {resp.status_code}. ")

    def search(
        self, query: Query, document_type: Type[Document], return_score: bool = False
    ) -> list[Union[Document, dict[Document, float]]]:
        search_url = f"{self._url}/{document_type.index_name()}/"
        headers = {"Content-Type": "application/json"}

        resp = requests.post(
            search_url, headers=headers, data=json.dumps(query.to_json())
        )

        documents = []
        for raw_doc in resp.json()["docs"]:
            doc = document_type(**raw_doc["doc"])
            if not return_score:
                documents.append(doc)
            else:
                raw_doc["doc"] = doc
                documents.append(raw_doc)

        return documents


class AsyncToshiClient:
    pass
