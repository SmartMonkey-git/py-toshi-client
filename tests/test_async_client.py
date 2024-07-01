import pytest
from aioresponses import aioresponses

from tests.conftest import Lyrics
from toshi_client.client import AsyncToshiClient
from toshi_client.errors import ToshiIndexError
from toshi_client.index.index_summary import IndexSummary
from toshi_client.query.term_query import TermQuery


@pytest.fixture
def toshi_client():
    return AsyncToshiClient(url="http://test.com")


@pytest.mark.asyncio
async def test_create_index(toshi_client, lyrics_index):
    with aioresponses() as m:
        m.put(f"http://test.com/{lyrics_index.name}/_create", status=201)

        await toshi_client.create_index(lyrics_index)


@pytest.mark.asyncio
async def test_create_index_failure(toshi_client, lyrics_index):

    with aioresponses() as m:
        m.put(
            f"http://test.com/{lyrics_index.name}/_create",
            status=400,
            payload={"message": "Bad Request"},
        )

        with pytest.raises(ToshiIndexError):
            await toshi_client.create_index(lyrics_index)


@pytest.mark.asyncio
async def test_get_index_summary(toshi_client):
    with aioresponses() as m:
        m.get(
            "http://test.com/test_index/_summary?include_sizes=True",
            status=200,
            payload={
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
                            "options": {
                                "indexed": True,
                                "fieldnorms": True,
                                "stored": True,
                            },
                        },
                        {
                            "name": "idx",
                            "type": "u64",
                            "options": {
                                "indexed": True,
                                "fieldnorms": True,
                                "stored": True,
                            },
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
                        {
                            "name": "test_facet",
                            "type": "facet",
                            "options": {"stored": True},
                        },
                    ],
                    "opstamp": 0,
                }
            },
        )

        summary = await toshi_client.get_index_summary("test_index")
        assert isinstance(summary, IndexSummary)


@pytest.mark.asyncio
async def test_add_document(toshi_client, black_keys_lyrics_document):
    with aioresponses() as m:
        m.put(f"http://test.com/{black_keys_lyrics_document.index_name()}/", status=201)

        await toshi_client.add_document(black_keys_lyrics_document)


@pytest.mark.asyncio
async def test_bulk_insert_documents(toshi_client, lyric_documents):
    index_name = lyric_documents[0].index_name()
    with aioresponses() as m:
        m.post(f"http://test.com/{index_name}/_bulk", status=201)

        await toshi_client.bulk_insert_documents(lyric_documents)


@pytest.mark.asyncio
async def test_get_documents(toshi_client):
    index_name = Lyrics.index_name()

    with aioresponses() as m:
        m.get(
            f"http://test.com/{index_name}/",
            status=200,
            payload={
                "hits": 3,
                "docs": [
                    {
                        "score": 1.0,
                        "doc": {
                            "song": "Gold on the Ceiling",
                            "idx": 2,
                            "artist": "The Black Keys",
                            "test_facet": "/a/b",
                            "lyrics": "Gold on the ceiling, I ain't blind, just a matter of time",
                            "genre": "Rock",
                            "year": 2011,
                        },
                    },
                    {
                        "score": 1.0,
                        "doc": {
                            "idx": 4,
                            "genre": "Grunge",
                            "test_facet": "/a/b",
                            "year": 1991,
                            "song": "Smells Like Teen Spirit",
                            "artist": "Nirvana",
                            "lyrics": "With the lights out, it's less dangerous, here we are now, entertain us",
                        },
                    },
                    {
                        "score": 1.0,
                        "doc": {
                            "song": "Creep",
                            "idx": 3,
                            "genre": "Alternative Rock",
                            "artist": "Radiohead",
                            "lyrics": "I'm a creep, I'm a weirdo, what the hell am I doing here?",
                            "test_facet": "/a/b",
                            "year": 1992,
                        },
                    },
                ],
                "facets": [],
            },
        )

        docs = await toshi_client.get_documents(Lyrics)
        assert len(docs) == 3
        assert isinstance(docs[0], Lyrics)


@pytest.mark.asyncio
async def test_delete_term(toshi_client):
    term_queries = [TermQuery(term="test_term", field_name="test_field")]
    index_name = "test_index"

    with aioresponses() as m:
        m.delete(
            f"http://test.com/{index_name}/", status=200, payload={"docs_affected": 1}
        )

        affected = await toshi_client.delete_term(term_queries, index_name)
        assert affected == 1


@pytest.mark.asyncio
async def test_list_indexes(toshi_client):
    with aioresponses() as m:
        m.get("http://test.com/_list/", status=200, payload=["index1", "index2"])

        indexes = await toshi_client.list_indexes()
        assert indexes == ["index1", "index2"]


@pytest.mark.asyncio
async def test_flush(toshi_client):
    with aioresponses() as m:
        m.get("http://test.com/test_index/_flush/", status=200)

        await toshi_client.flush("test_index")


@pytest.mark.asyncio
async def test_search(toshi_client):
    query = TermQuery(term="test", field_name="test_field")

    with aioresponses() as m:
        m.post(
            f"http://test.com/{Lyrics.index_name()}/",
            status=200,
            payload={
                "docs": [
                    {
                        "score": 1.0103524,
                        "doc": {
                            "song": "Gold on the Ceiling",
                            "genre": "Rock",
                            "year": 2011,
                            "test_facet": "/a/b",
                            "idx": 2,
                            "lyrics": "Gold on the ceiling, I ain't blind, just a matter of time",
                            "artist": "The Black Keys",
                        },
                    }
                ]
            },
        )

        results = await toshi_client.search(query, Lyrics)
        assert len(results) == 1
        assert isinstance(results[0], Lyrics)
