import time
from typing import Optional

import pytest
import requests

from client import ToshiClient
from errors import ToshiIndexError
from index_builder import IndexBuilder
from schemas.document import Document
from schemas.field_options import TextOptionIndexing
from schemas.index_summary import IndexSummary, IndexSettings
from tests.conftest import CI


@pytest.fixture
def lyrics_index():
    builder = IndexBuilder()

    builder.add_text_field(name="lyrics", stored=True, indexing=TextOptionIndexing())
    builder.add_i64_field(name="year", stored=True, indexed=True)
    builder.add_u64_field(name="idx", stored=True, indexed=True)
    builder.add_text_field(name="artist", stored=True, indexing=TextOptionIndexing())
    builder.add_text_field(name="genre", stored=True, indexing=TextOptionIndexing())
    builder.add_text_field(name="song", stored=True, indexing=TextOptionIndexing())

    return builder.build()


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
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

    with pytest.raises(ToshiIndexError):
        client.create_index(name=index_name, create_index_payload=lyrics_index)


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_get_index_summary(toshi_container, lyrics_index):
    client = ToshiClient(toshi_container)
    res = client.get_index_summary(name="lyrics")
    expected_summary = IndexSummary(
        segments=[],
        opstamp=0,
        index_settings=IndexSettings(
            docstore_blocksize=16384, docstore_compression="lz4"
        ),
        index=lyrics_index,
    )

    assert expected_summary == res


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_list_indexes(toshi_container, lyrics_index):
    client = ToshiClient(toshi_container)
    res = client.list_indexes()

    assert ["lyrics"] == res


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_add_document(toshi_container):
    class Lyrics(Document):

        def __init__(
            self,
            lyrics: str,
            year: int,
            idx: int,
            artist: str,
            genre: str,
            song: str,
            index_name: Optional[str] = "lyrics",
            options: Optional[dict] = None,
        ):
            super().__init__(index_name, options)
            self.lyrics = lyrics
            self.year = year
            self.idx = idx
            self.artist = artist
            self.genre = genre
            self.song = song

    doc = Lyrics(
        lyrics="Here comes the sun, doo-doo-doo-doo",
        year=1969,
        idx=1,
        artist="The Beatles",
        genre="Rock",
        song="Here Comes The Sun",
    )

    client = ToshiClient(toshi_container)
    client.add_document(document=doc)
    time.sleep(0.5)
    retrieved_doc = client.get_documents(index_name="lyrics", document=Lyrics)

    assert len(retrieved_doc) == 1
    assert doc.to_json() == retrieved_doc[0].to_json()
