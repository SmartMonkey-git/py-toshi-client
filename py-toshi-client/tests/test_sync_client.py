import pytest
import requests

from client import ToshiClient
from errors import IndexException

from index_builder import IndexBuilder


@pytest.fixture
def lyrics_index():
    builder = IndexBuilder()

    builder.add_text_field(
        name="lyrics",
        stored=True,
        indexing={"record": "position", "tokenizer": "default"},
    )
    builder.add_i64_field(name="year", stored=True, indexed=True)
    builder.add_u64_field(name="idx", stored=True, indexed=True)
    builder.add_text_field(
        name="artist",
        stored=True,
        indexing={"record": "position", "tokenizer": "default"},
    )
    builder.add_text_field(
        name="genre",
        stored=True,
        indexing={"record": "position", "tokenizer": "default"},
    )
    builder.add_text_field(
        name="song",
        stored=True,
        indexing={"record": "position", "tokenizer": "default"},
    )

    return builder.build()


@pytest.mark.integration()
def test_create_index(lyrics_index, toshi_container):
    index_name = "lyrics"
    unknown_index_response = {"message": "Unknown Index: 'lyrics' does not exist"}

    get_schema_summary_url = (
        f"{toshi_container}/{index_name}/_summary?include_sizes=true"
    )
    res = requests.get(
        get_schema_summary_url, headers={"Content-Type": "application/json"}
    )
    assert res.json() == unknown_index_response

    client = ToshiClient(toshi_container)
    client.create_index(name=index_name, create_index_payload=lyrics_index)

    get_schema_summary_url = (
        f"{toshi_container}/{index_name}/_summary?include_sizes=true"
    )
    res = requests.get(
        get_schema_summary_url, headers={"Content-Type": "application/json"}
    )
    assert res.json() != unknown_index_response

    with pytest.raises(IndexException):
        client.create_index(name=index_name, create_index_payload=lyrics_index)


@pytest.mark.integration()
def test_get_index_summary(toshi_container):
    client = ToshiClient(toshi_container)
    res = client.get_index_summary(name="lyrics")
