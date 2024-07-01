from unittest.mock import patch, Mock

import pytest

from toshi_client.client import ToshiClient
from toshi_client.index.index import Index
from toshi_client.index.index_summary import IndexSummary
from toshi_client.models.document import Document
from toshi_client.models.query import Query
from toshi_client.query.term_query import TermQuery


@pytest.fixture
def toshi_client():
    return ToshiClient("http://localhost:8080")


@patch("requests.put")
def test_create_index(mock_put, toshi_client):
    index = Mock(spec=Index)
    index.name = "test_index"
    index.to_json.return_value = {"name": "test_index"}

    mock_response = Mock()
    mock_response.status_code = 201
    mock_put.return_value = mock_response

    toshi_client.create_index(index)
    mock_put.assert_called_once_with(
        "http://localhost:8080/test_index/_create", json={"name": "test_index"}
    )


@patch("requests.get")
def test_get_index_summary(mock_get, toshi_client):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "summaries": {
            "index_settings": {
                "docstore_compression": "lz4",
                "docstore_blocksize": 16384,
            },
            "segments": [],
            "schema": [
                {
                    "name": "lyrics",
                    "type": "text",
                    "options": {
                        "indexing": {
                            "record": "position",
                            "fieldnorms": True,
                            "tokenizer": "default",
                        },
                        "stored": True,
                        "fast": False,
                    },
                },
                {
                    "name": "year",
                    "type": "i64",
                    "options": {"indexed": True, "fieldnorms": True, "stored": True},
                },
                {
                    "name": "idx",
                    "type": "u64",
                    "options": {"indexed": True, "fieldnorms": True, "stored": True},
                },
                {
                    "name": "artist",
                    "type": "text",
                    "options": {
                        "indexing": {
                            "record": "position",
                            "fieldnorms": True,
                            "tokenizer": "default",
                        },
                        "stored": True,
                        "fast": False,
                    },
                },
                {
                    "name": "genre",
                    "type": "text",
                    "options": {
                        "indexing": {
                            "record": "position",
                            "fieldnorms": True,
                            "tokenizer": "default",
                        },
                        "stored": True,
                        "fast": False,
                    },
                },
                {
                    "name": "song",
                    "type": "text",
                    "options": {
                        "indexing": {
                            "record": "position",
                            "fieldnorms": True,
                            "tokenizer": "default",
                        },
                        "stored": True,
                        "fast": False,
                    },
                },
                {"name": "test_facet", "type": "facet", "options": {"stored": True}},
            ],
            "opstamp": 0,
        }
    }
    mock_get.return_value = mock_response

    summary = toshi_client.get_index_summary("test_index", True)
    mock_get.assert_called_once_with(
        "http://localhost:8080/test_index/_summary?include_sizes=True"
    )
    assert isinstance(summary, IndexSummary)


@patch("requests.put")
def test_add_document(mock_put, toshi_client):
    document = Mock(spec=Document)
    document.index_name.return_value = "test_index"
    document.to_json.return_value = {"document": "data"}

    mock_response = Mock()
    mock_response.status_code = 201
    mock_put.return_value = mock_response

    toshi_client.add_document(document, commit=False)
    mock_put.assert_called_once_with(
        "http://localhost:8080/test_index/",
        headers={"Content-Type": "application/json"},
        json={"document": {"document": "data"}, "options": {"commit": False}},
    )


@patch("requests.post")
def test_bulk_insert_documents(mock_post, toshi_client):
    document = Mock(spec=Document)
    document.index_name.return_value = "test_index"
    document.to_json.return_value = {"document": "data"}

    mock_response = Mock()
    mock_response.status_code = 201
    mock_post.return_value = mock_response

    toshi_client.bulk_insert_documents([document], commit=False)
    mock_post.assert_called_once_with(
        "http://localhost:8080/test_index/_bulk", data='{"document": "data"}'
    )


@patch("requests.get")
def test_get_documents(mock_get, toshi_client):
    document = Mock(spec=Document)
    document.index_name.return_value = "test_index"

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"docs": [{"doc": {"data": "value"}}]}
    mock_get.return_value = mock_response

    documents = toshi_client.get_documents(document)
    mock_get.assert_called_once_with("http://localhost:8080/test_index/")
    assert len(documents) == 1


@patch("requests.delete")
def test_delete_term(mock_delete, toshi_client):
    term_query = Mock(spec=TermQuery)
    term_query.to_json.return_value = {"query": {"term": {"field": "value"}}}

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"docs_affected": 1}
    mock_delete.return_value = mock_response

    affected_docs = toshi_client.delete_term([term_query], "test_index", commit=False)
    mock_delete.assert_called_once_with(
        "http://localhost:8080/test_index/",
        data='{"terms": {"field": "value"}, "options": {"commit": false}}',
    )
    assert affected_docs == 1


@patch("requests.get")
def test_list_indexes(mock_get, toshi_client):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = ["index1", "index2"]
    mock_get.return_value = mock_response

    indexes = toshi_client.list_indexes()
    mock_get.assert_called_once_with("http://localhost:8080/_list/")
    assert indexes == ["index1", "index2"]


@patch("requests.get")
def test_flush(mock_get, toshi_client):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    toshi_client.flush("test_index")
    mock_get.assert_called_once_with("http://localhost:8080/test_index/_flush/")


@patch("requests.post")
def test_search(mock_post, toshi_client):
    query = Mock(spec=Query)
    query.to_json.return_value = {"query": "data"}
    document_type = Mock(spec=Document)
    document_type.index_name.return_value = "test_index"

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"docs": [{"doc": {"data": "value"}}]}
    mock_post.return_value = mock_response

    results = toshi_client.search(query, document_type, return_score=False)
    mock_post.assert_called_once_with(
        "http://localhost:8080/test_index/",
        headers={"Content-Type": "application/json"},
        data='{"query": "data"}',
    )
    assert len(results) == 1
