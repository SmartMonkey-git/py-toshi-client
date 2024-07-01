import json
from collections import ChainMap
from typing import Optional, Type, Union

import aiohttp
import requests

from toshi_client.errors import (
    ToshiIndexError,
    ToshiDocumentError,
    ToshiFlushError,
    ToshiClientError,
)
from toshi_client.index.index import Index
from toshi_client.index.index_summary import IndexSummary
from toshi_client.models.document import Document
from toshi_client.models.query import Query
from toshi_client.query.facet_query import FacetQuery
from toshi_client.query.term_query import TermQuery


class ToshiClient:
    """
    A client for interacting with the Toshi search server.

    Parameters
    ----------
    url : str
        The base URL of the Toshi search server.
    """

    def __init__(self, url: str):
        if url.endswith("/"):
            url = url[:-1]
        self._url = url

    def create_index(self, index: Index):
        """
        Creates a new index on the Toshi server.

        Parameters
        ----------
        index : Index
            The index to be created.

        Raises
        ------
        ToshiIndexError
            If the index creation fails.
        """
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
        """
        Retrieves a summary of the specified index.

        Parameters
        ----------
        name : str
            The name of the index.
        include_size : Optional[bool], default=True
            Whether to include the size of the index in the summary.

        Returns
        -------
        IndexSummary
            The summary of the index.

        Raises
        ------
        ToshiIndexError
            If retrieving the index summary fails.
        """
        index_summary_url = f"{self._url}/{name}/_summary?include_sizes={include_size}"
        resp = requests.get(index_summary_url)

        if resp.status_code != 200:
            raise ToshiIndexError(
                f"Could not get index summary. Status code: {resp.status_code}. "
                f"Reason: {resp.json()['message']}"
            )

        return IndexSummary.from_json(index_name=name, data=resp.json()["summaries"])

    def add_document(self, document: Document, commit: Optional[bool] = False):
        """
        Adds a document to the specified index.

        Parameters
        ----------
        document : Document
            The document to be added.
        commit : Optional[bool], default=False
            Whether to commit the changes immediately.

        Raises
        ------
        ToshiDocumentError
            If adding the document fails.
        """
        index_url = f"{self._url}/{document.index_name()}/"
        headers = {"Content-Type": "application/json"}

        json_data = dict(document=document.to_json(), options=dict(commit=commit))
        resp = requests.put(index_url, headers=headers, json=json_data)

        if resp.status_code != 201:
            raise ToshiDocumentError(
                f"Could not add document for index {document.index_name()}. Status code: {resp.status_code}. "
                f"Reason: {resp.json()['message']}"
            )

    def bulk_insert_documents(self, documents: list[Document], commit: bool = False):
        """
        Inserts multiple documents into the specified index.

        Parameters
        ----------
        documents : list[Document]
            The documents to be inserted.
        commit : bool, default=False
            Whether to commit the changes immediately.

        Raises
        ------
        ToshiDocumentError
            If bulk inserting the documents fails.
        """
        index_name = documents[0].index_name()
        index_url = f"{self._url}/{index_name}/_bulk"

        body_content = "\n".join([json.dumps(doc.to_json()) for doc in documents])
        resp = requests.post(index_url, data=body_content)

        if resp.status_code != 201:
            raise ToshiDocumentError(
                f"Could not add document for index {index_name}. Status code: {resp.status_code}. "
                f"Reason: {resp.json()['message']}"
            )

        if commit:
            self.flush(index_name)

    def get_documents(self, document: Type[Document]) -> list[Document]:
        """
        Retrieves all documents from the specified index.

        Parameters
        ----------
        document : Type[Document]
            The type of document to retrieve.

        Returns
        -------
        list[Document]
            The list of documents retrieved.

        Raises
        ------
        ToshiDocumentError
            If retrieving the documents fails.
        """
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

    def delete_term(
        self,
        term_queries: list[TermQuery],
        index_name: str,
        commit: Optional[bool] = False,
    ) -> int:
        """
        Deletes documents based on term queries from the specified index.

        Parameters
        ----------
        term_queries : list[TermQuery]
            The term queries specifying the documents to delete.
        index_name : str
            The name of the index.
        commit : Optional[bool], default=False
            Whether to commit the changes immediately.

        Returns
        -------
        int
            The number of documents affected.

        Raises
        ------
        ToshiDocumentError
            If deleting the documents fails.
        """
        index_url = f"{self._url}/{index_name}/"

        terms = dict()
        for tq in term_queries:
            terms.update(tq.to_json()["query"]["term"])

        body = json.dumps(dict(terms=terms, options=dict(commit=commit)))
        resp = requests.delete(index_url, data=body)

        if resp.status_code != 200:
            raise ToshiDocumentError(
                f"Could not delete documents for index {index_name}. Status code: {resp.status_code}. "
                f"Reason: {resp.json()['message']}"
            )

        return resp.json()["docs_affected"]

    def list_indexes(self) -> list[str]:
        """
        Lists all indexes on the Toshi server.

        Returns
        -------
        list[str]
            The list of index names.

        Raises
        ------
        ToshiIndexError
            If listing the indexes fails.
        """
        list_index_url = f"{self._url}/_list/"
        resp = requests.get(list_index_url)

        if resp.status_code != 200:
            raise ToshiIndexError(
                f"Could not list indexes. Status code: {resp.status_code}. "
                f"Reason: {resp.json()['message']}"
            )

        return resp.json()

    def flush(self, index_name: str):
        """
        Flushes the specified index.

        Parameters
        ----------
        index_name : str
            The name of the index to flush.

        Raises
        ------
        ToshiFlushError
            If flushing the index fails.
        """
        # Flush uses actually get method not post, as in the examples
        # https://github.com/toshi-search/Toshi/blob/a13a51820bdb025b1c0556a4e49be2e5b97fbeca/toshi-server/src/router.rs#L56
        index_url = f"{self._url}/{index_name}/_flush/"
        resp = requests.get(index_url)

        if resp.status_code != 200:
            raise ToshiFlushError(f"Could not flush. Status code: {resp.status_code}. ")

    def search(
        self,
        query: Query,
        document_type: Type[Document],
        facet_query: list[FacetQuery] = None,
        return_score: bool = False,
    ) -> list[Union[Document, dict[Document, float]]]:
        """
        Searches for documents in the specified index.

        Parameters
        ----------
        query : Query
            The search query.
        document_type : Type[Document]
            The type of document to search for.
        facet_query : list[FacetQuery], optional
            The facet queries for the search.
        return_score : bool, default=False
            Whether to return the scores along with the documents.

        Returns
        -------
        list[Union[Document, dict[Document, float]]]
            The list of documents or a list of dictionaries with documents and their scores.

        Raises
        ------
        ToshiClientError
            If the search fails.
        """
        search_url = f"{self._url}/{document_type.index_name()}/"
        headers = {"Content-Type": "application/json"}

        json_data = query.to_json()
        if facet_query is not None:
            json_data["facets"] = dict(ChainMap(*[f.to_json() for f in facet_query]))

        resp = requests.post(search_url, headers=headers, data=json.dumps(json_data))

        json_data = resp.json()
        if "message" in json_data:
            raise ToshiClientError(json_data["message"])

        documents = []
        for raw_doc in json_data["docs"]:
            doc = document_type(**raw_doc["doc"])
            if not return_score:
                documents.append(doc)
            else:
                raw_doc["doc"] = doc
                documents.append(raw_doc)

        return documents


class AsyncToshiClient:
    """
    A client for interacting with the Toshi search server.

    Parameters
    ----------
    url : str
        The base URL of the Toshi search server.

    """
    def __init__(self, url: str):
        if url.endswith("/"):
            url = url[:-1]
        self._url = url

    async def create_index(self, index: Index):
        """
        Creates a new index on the Toshi server.

        Parameters
        ----------
        index : Index
            The index to be created.

        Raises
        ------
        ToshiIndexError
            If the index creation fails.
        """
        create_index_url = f"{self._url}/{index.name}/_create"
        async with aiohttp.ClientSession() as session:
            async with session.put(create_index_url, json=index.to_json()) as resp:
                if resp.status != 201:
                    error_message = json.loads(await resp.read())
                    raise ToshiIndexError(
                        f"Creating index failed with status code: {resp.status}. "
                        f"Reason: {error_message['message']}"
                    )

    async def get_index_summary(
        self, name: str, include_size: Optional[bool] = True
    ) -> IndexSummary:
        """
        Retrieves a summary of the specified index.

        Parameters
        ----------
        name : str
            The name of the index.
        include_size : Optional[bool], default=True
            Whether to include the size of the index in the summary.

        Returns
        -------
        IndexSummary
            The summary of the index.

        Raises
        ------
        ToshiIndexError
            If retrieving the index summary fails.
        """
        index_summary_url = f"{self._url}/{name}/_summary?include_sizes={include_size}"
        async with aiohttp.ClientSession() as session:
            async with session.get(index_summary_url) as resp:
                if resp.status != 200:
                    error_message = await resp.json()
                    raise ToshiIndexError(
                        f"Could not get index summary. Status code: {resp.status}. "
                        f"Reason: {error_message['message']}"
                    )
                data = await resp.json()
                return IndexSummary.from_json(index_name=name, data=data["summaries"])

    async def add_document(self, document: Document, commit: Optional[bool] = False):
        """
        Adds a document to the specified index.

        Parameters
        ----------
        document : Document
            The document to be added.
        commit : Optional[bool], default=False
            Whether to commit the changes immediately.

        Raises
        ------
        ToshiDocumentError
            If adding the document fails.
        """
        index_url = f"{self._url}/{document.index_name()}/"
        headers = {"Content-Type": "application/json"}

        json_data = dict(document=document.to_json(), options=dict(commit=commit))
        async with aiohttp.ClientSession() as session:
            async with session.put(index_url, headers=headers, json=json_data) as resp:
                if resp.status != 201:
                    error_message = await resp.json()
                    raise ToshiDocumentError(
                        f"Could not add document for index {document.index_name()}. Status code: {resp.status}. "
                        f"Reason: {error_message['message']}"
                    )

    async def bulk_insert_documents(
        self, documents: list[Document], commit: bool = False
    ):
        """
        Inserts multiple documents into the specified index.

        Parameters
        ----------
        documents : list[Document]
            The documents to be inserted.
        commit : bool, default=False
            Whether to commit the changes immediately.

        Raises
        ------
        ToshiDocumentError
            If bulk inserting the documents fails.
        """
        index_name = documents[0].index_name()
        index_url = f"{self._url}/{index_name}/_bulk"

        body_content = "\n".join([json.dumps(doc.to_json()) for doc in documents])
        async with aiohttp.ClientSession() as session:
            async with session.post(index_url, data=body_content) as resp:
                if resp.status != 201:
                    error_message = await resp.json()
                    raise ToshiDocumentError(
                        f"Could not add document for index {index_name}. Status code: {resp.status}. "
                        f"Reason: {error_message['message']}"
                    )

                if commit:
                    await self.flush(index_name)

    async def get_documents(self, document: Type[Document]) -> list[Document]:
        """
        Retrieves all documents from the specified index.

        Parameters
        ----------
        document : Type[Document]
            The type of document to retrieve.

        Returns
        -------
        list[Document]
            The list of documents retrieved.

        Raises
        ------
        ToshiDocumentError
            If retrieving the documents fails.
        """
        index_url = f"{self._url}/{document.index_name()}/"
        async with aiohttp.ClientSession() as session:
            async with session.get(index_url) as resp:
                if resp.status != 200:
                    error_message = await resp.json()
                    raise ToshiDocumentError(
                        f"Could not get documents for index {document.index_name()}. Status code: {resp.status}. "
                        f"Reason: {error_message['message']}"
                    )
                data = await resp.json()
                documents = []
                for doc in data["docs"]:
                    documents.append(document(**doc["doc"]))
                return documents

    async def delete_term(
        self,
        term_queries: list[TermQuery],
        index_name: str,
        commit: Optional[bool] = False,
    ) -> int:
        """
        Deletes documents based on term queries from the specified index.

        Parameters
        ----------
        term_queries : list[TermQuery]
            The term queries specifying the documents to delete.
        index_name : str
            The name of the index.
        commit : Optional[bool], default=False
            Whether to commit the changes immediately.

        Returns
        -------
        int
            The number of documents affected.

        Raises
        ------
        ToshiDocumentError
            If deleting the documents fails.
        """
        index_url = f"{self._url}/{index_name}/"

        terms = dict()
        for tq in term_queries:
            terms.update(tq.to_json()["query"]["term"])

        body = json.dumps(dict(terms=terms, options=dict(commit=commit)))
        async with aiohttp.ClientSession() as session:
            async with session.delete(index_url, data=body) as resp:
                if resp.status != 200:
                    error_message = await resp.json()
                    raise ToshiDocumentError(
                        f"Could not delete documents for index {index_name}. Status code: {resp.status}. "
                        f"Reason: {error_message['message']}"
                    )

                data = await resp.json()
                return data["docs_affected"]

    async def list_indexes(self) -> list[str]:
        """
        Lists all indexes on the Toshi server.

        Returns
        -------
        list[str]
            The list of index names.

        Raises
        ------
        ToshiIndexError
            If listing the indexes fails.
        """
        list_index_url = f"{self._url}/_list/"
        async with aiohttp.ClientSession() as session:
            async with session.get(list_index_url) as resp:
                if resp.status != 200:
                    error_message = await resp.json()
                    raise ToshiIndexError(
                        f"Could not list indexes. Status code: {resp.status}. "
                        f"Reason: {error_message['message']}"
                    )

                return await resp.json()

    async def flush(self, index_name: str):
        """
        Flushes the specified index.

        Parameters
        ----------
        index_name : str
            The name of the index to flush.

        Raises
        ------
        ToshiFlushError
            If flushing the index fails.
        """
        index_url = f"{self._url}/{index_name}/_flush/"
        async with aiohttp.ClientSession() as session:
            async with session.get(index_url) as resp:
                if resp.status != 200:
                    raise ToshiFlushError(
                        f"Could not flush. Status code: {resp.status}. "
                    )

    async def search(
        self,
        query: Query,
        document_type: Type[Document],
        facet_query: Optional[list[FacetQuery]] = None,
        return_score: bool = False,
    ) -> list[Union[Document, dict]]:
        """
        Searches for documents in the specified index.

        Parameters
        ----------
        query : Query
            The search query.
        document_type : Type[Document]
            The type of document to search for.
        facet_query : list[FacetQuery], optional
            The facet queries for the search.
        return_score : bool, default=False
            Whether to return the scores along with the documents.

        Returns
        -------
        list[Union[Document, dict[Document, float]]]
            The list of documents or a list of dictionaries with documents and their scores.

        Raises
        ------
        ToshiClientError
            If the search fails.
        """
        search_url = f"{self._url}/{document_type.index_name()}/"
        headers = {"Content-Type": "application/json"}

        json_data = query.to_json()
        if facet_query is not None:
            json_data["facets"] = dict(ChainMap(*[f.to_json() for f in facet_query]))

        async with aiohttp.ClientSession() as session:
            async with session.post(
                search_url, headers=headers, data=json.dumps(json_data)
            ) as resp:
                json_data = await resp.json()
                if "message" in json_data:
                    raise ToshiClientError(json_data["message"])

                documents = []
                for raw_doc in json_data["docs"]:
                    doc = document_type(**raw_doc["doc"])
                    if not return_score:
                        documents.append(doc)
                    else:
                        raw_doc["doc"] = doc
                        documents.append(raw_doc)

                return documents
