import asyncio
import json
from pathlib import Path

import aiohttp
import pytest

from tests.conftest import CI, Lyrics
from toshi_client.client import AsyncToshiClient
from toshi_client.errors import ToshiIndexError
from toshi_client.index.index_summary import IndexSummary, IndexSettings
from toshi_client.query.bool_query import BoolQuery
from toshi_client.query.facet_query import FacetQuery
from toshi_client.query.fuzzy_query import FuzzyQuery
from toshi_client.query.phrase_query import PhraseQuery
from toshi_client.query.range_query import RangeQuery
from toshi_client.query.regex_query import RegexQuery
from toshi_client.query.term_query import TermQuery


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
@pytest.mark.asyncio
async def test_create_index(lyrics_index, toshi_container):
    unknown_index_response = {"message": "Unknown Index: 'lyrics' does not exist"}

    get_schema_summary_url = (
        f"{toshi_container}/{lyrics_index.name}/_summary?include_sizes=true"
    )
    async with aiohttp.ClientSession() as session:
        async with session.get(get_schema_summary_url) as res:
            assert json.loads(await res.read()) == unknown_index_response

    client = AsyncToshiClient(toshi_container)
    await client.create_index(index=lyrics_index)

    async with aiohttp.ClientSession() as session:
        async with session.get(
            get_schema_summary_url, headers={"Content-Type": "application/json"}
        ) as res:
            assert await res.json() != unknown_index_response

    with pytest.raises(ToshiIndexError):
        await client.create_index(index=lyrics_index)


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
@pytest.mark.asyncio
async def test_get_index_summary(toshi_container, lyrics_index):
    client = AsyncToshiClient(toshi_container)
    res = await client.get_index_summary(name=lyrics_index.name)
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
@pytest.mark.asyncio
async def test_add_document(toshi_container, lyric_documents):
    client = AsyncToshiClient(toshi_container)
    for doc in lyric_documents:
        await client.add_document(document=doc)
    await asyncio.sleep(0.5)
    retrieved_doc = await client.get_documents(document=Lyrics)

    assert len(retrieved_doc) == 3
    assert sorted(
        [doc.to_json() for doc in lyric_documents], key=lambda d: d["year"]
    ) == sorted([r_doc.to_json() for r_doc in retrieved_doc], key=lambda d: d["year"])

    assert len(retrieved_doc) == 3
    assert sorted(
        [doc.to_json() for doc in lyric_documents], key=lambda d: d["year"]
    ) == sorted([r_doc.to_json() for r_doc in retrieved_doc], key=lambda d: d["year"])


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
@pytest.mark.asyncio
async def test_list_indexes(toshi_container):
    client = AsyncToshiClient(toshi_container)
    res = await client.list_indexes()

    assert res == ["lyrics"]


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
@pytest.mark.asyncio
async def test_search_term_query(
    toshi_container, black_keys_lyrics_document, lyric_documents
):
    client = AsyncToshiClient(toshi_container)

    query = TermQuery(term="ceiling", field_name="lyrics")
    documents = await client.search(query, Lyrics)

    assert len(documents) == 1
    assert documents[0] == black_keys_lyrics_document

    query = TermQuery(term="the", field_name="lyrics")
    documents = await client.search(query, Lyrics)

    assert len(documents) == 3
    assert [d for d in documents] == [d for d in lyric_documents]


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
@pytest.mark.asyncio
async def test_search_fuzzy_query(toshi_container, black_keys_lyrics_document):
    client = AsyncToshiClient(toshi_container)
    query = FuzzyQuery(
        term="ceilin", field_name="lyrics", distance=2, transposition=False
    )

    documents = await client.search(query, Lyrics)

    assert len(documents) == 1
    assert documents[0] == black_keys_lyrics_document


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
@pytest.mark.asyncio
async def test_search_range_query(toshi_container):
    client = AsyncToshiClient(toshi_container)
    gt = 1990
    lt = 2000
    query = RangeQuery(gt=gt, lt=lt, gte=gt, lte=lt, field_name="year")

    documents = await client.search(query, Lyrics)
    assert len(documents) >= 1
    all(gt < doc.year < lt for doc in documents)


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
@pytest.mark.asyncio
async def test_search_bool_query(toshi_container, black_keys_lyrics_document):
    client = AsyncToshiClient(toshi_container)

    term_query = TermQuery(term="the", field_name="lyrics")
    gt = 1990
    lt = 2000
    range_query = RangeQuery(gt=gt, lt=lt, field_name="year")
    query = BoolQuery().must_match(term_query).must_not_match(range_query)

    documents = await client.search(query, Lyrics)
    assert len(documents) >= 1
    assert documents[0] == black_keys_lyrics_document


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
@pytest.mark.asyncio
async def test_regex_query(toshi_container, lyric_documents):
    regex = ".*"
    client = AsyncToshiClient(toshi_container)

    query = RegexQuery(regex=regex, field_name="lyrics")
    documents = await client.search(query, Lyrics)

    assert [d for d in sorted(documents, key=lambda doc: doc.artist)] == [
        d for d in sorted(lyric_documents, key=lambda doc: doc.artist)
    ]


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
@pytest.mark.asyncio
async def test_phrase_query(toshi_container, radiohead_lyrics_document):
    client = AsyncToshiClient(toshi_container)

    query = PhraseQuery(terms=["what", "the", "hell"], field_name="lyrics")
    documents = await client.search(query, Lyrics)

    assert documents[0] == radiohead_lyrics_document


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
@pytest.mark.asyncio
async def test_bulk_insert_documents(toshi_container):
    lyric_documents = [
        Lyrics(
            lyrics="There's a lady who's sure all that glitters is gold, and she's buying a stairway to heaven",
            year=1971,
            idx=10,
            artist="Led Zeppelin",
            genre="Rock",
            song="Stairway to Heaven",
            test_facet="/a/b",
        ),
        Lyrics(
            lyrics="Is this the real life? Is this just fantasy? Caught in a landslide, no escape from reality",
            year=1975,
            idx=11,
            artist="Queen",
            genre="Rock",
            song="Bohemian Rhapsody",
            test_facet="/a/b",
        ),
        Lyrics(
            lyrics="We don't need no education, we don't need no thought control",
            year=1979,
            idx=12,
            artist="Pink Floyd",
            genre="Progressive Rock",
            song="Another Brick in the Wall",
            test_facet="/t/x",
        ),
    ]

    client = AsyncToshiClient(toshi_container)
    await client.bulk_insert_documents(documents=lyric_documents, commit=True)
    await asyncio.sleep(0.5)
    retrieved_doc = await client.get_documents(document=Lyrics)

    assert len(retrieved_doc) == 6
    assert all(doc in retrieved_doc for doc in lyric_documents)


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
@pytest.mark.asyncio
async def test_search_facet_query(
    toshi_container, black_keys_lyrics_document, lyric_documents
):
    client = AsyncToshiClient(toshi_container)

    query = TermQuery(term="the", field_name="lyrics")
    facet_queries = [FacetQuery("test_facet", [Path("/t")])]
    documents = await client.search(query, Lyrics, facet_query=facet_queries)

    assert len(documents) == 4


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
@pytest.mark.asyncio
async def test_delete_index(toshi_container):
    client = AsyncToshiClient(toshi_container)
    term_queries = [
        TermQuery(term="the", field_name="lyrics"),
        TermQuery(term="Nirvana", field_name="artist"),
    ]

    get_term_query = TermQuery(term="the", field_name="lyrics")
    documents = await client.search(get_term_query, Lyrics)
    assert len(documents) == 4

    await client.delete_term(
        term_queries=term_queries, index_name="lyrics", commit=True
    )
    await asyncio.sleep(0.5)

    documents = await client.search(get_term_query, Lyrics)
    assert len(documents) == 0
