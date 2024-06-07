import pytest
import requests

from client import ToshiClient
from errors import IndexCreationError


@pytest.fixture
def lyrics_index():
    return [
        {
            "name": "lyrics",
            "type": "text",
            "options": {
                "indexing": {"record": "position", "tokenizer": "default"},
                "stored": True,
            },
        },
        {"name": "year", "type": "i64", "options": {"indexed": True, "stored": True}},
        {"name": "idx", "type": "u64", "options": {"indexed": True, "stored": True}},
        {
            "name": "artist",
            "type": "text",
            "options": {
                "indexing": {"record": "position", "tokenizer": "default"},
                "stored": True,
            },
        },
        {
            "name": "genre",
            "type": "text",
            "options": {
                "indexing": {"record": "position", "tokenizer": "default"},
                "stored": True,
            },
        },
        {
            "name": "song",
            "type": "text",
            "options": {
                "indexing": {"record": "position", "tokenizer": "default"},
                "stored": True,
            },
        },
    ]


@pytest.mark.integration()
def test_create_index(lyrics_index, toshi_container):
    index_name = "lyrics"

    get_schema_summary_url = (
        f"{toshi_container}/{index_name}/_summary?include_sizes=true"
    )
    res = requests.get(
        get_schema_summary_url, headers={"Content-Type": "application/json"}
    )
    assert res.json() == {"message": "Unknown Index: 'lyrics' does not exist"}

    client = ToshiClient(toshi_container)
    client.create_index(name=index_name, create_index_payload=lyrics_index)

    get_schema_summary_url = (
        f"{toshi_container}/{index_name}/_summary?include_sizes=true"
    )
    res = requests.get(
        get_schema_summary_url, headers={"Content-Type": "application/json"}
    )
    assert res.json() != {"message": "Unknown Index: 'lyrics' does not exist"}

    with pytest.raises(IndexCreationError):
        client.create_index(name=index_name, create_index_payload=lyrics_index)
