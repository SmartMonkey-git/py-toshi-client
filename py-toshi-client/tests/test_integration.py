import time
from pathlib import Path

import pytest
import requests

from client import ToshiClient
from errors import ToshiIndexError
from index import IndexSummary, IndexSettings
from index.field_options import TextOptionIndexing
from index.index_builder import IndexBuilder
from models.document import Document
from query import TermQuery
from query.bool_query import BoolQuery
from query.facet_query import FacetQuery
from query.fuzzy_query import FuzzyQuery
from query.phrase_query import PhraseQuery
from query.range_query import RangeQuery
from query.regex_query import RegexQuery
from tests.conftest import CI


class Lyrics(Document):
    @staticmethod
    def index_name() -> str:
        return "lyrics"

    def __init__(
        self,
        lyrics: str,
        year: int,
        idx: int,
        artist: str,
        genre: str,
        song: str,
        test_facet: str,
    ):
        self.lyrics = lyrics
        self.year = year
        self.idx = idx
        self.artist = artist
        self.genre = genre
        self.song = song
        self.test_facet = test_facet


@pytest.fixture
def black_keys_lyrics_document():
    return Lyrics(
        lyrics="Gold on the ceiling, I ain't blind, just a matter of time",
        year=2011,
        idx=2,
        artist="The Black Keys",
        genre="Rock",
        song="Gold on the Ceiling",
        test_facet="/a/b",
    )


@pytest.fixture
def nirvana_lyrics_document():
    return Lyrics(
        lyrics="With the lights out, it's less dangerous, here we are now, entertain us",
        year=1991,
        idx=4,
        artist="Nirvana",
        genre="Grunge",
        song="Smells Like Teen Spirit",
        test_facet="/a/b",
    )


@pytest.fixture
def radiohead_lyrics_document():
    return Lyrics(
        lyrics="I'm a creep, I'm a weirdo, what the hell am I doing here?",
        year=1992,
        idx=3,
        artist="Radiohead",
        genre="Alternative Rock",
        song="Creep",
        test_facet="/a/b",
    )


@pytest.fixture
def lyric_documents(
    black_keys_lyrics_document, nirvana_lyrics_document, radiohead_lyrics_document
):
    return [
        black_keys_lyrics_document,
        nirvana_lyrics_document,
        radiohead_lyrics_document,
    ]


@pytest.fixture
def lyrics_index():
    builder = IndexBuilder()

    builder.add_text_field(name="lyrics", stored=True, indexing=TextOptionIndexing())
    builder.add_i64_field(name="year", stored=True, indexed=True)
    builder.add_u64_field(name="idx", stored=True, indexed=True)
    builder.add_text_field(name="artist", stored=True, indexing=TextOptionIndexing())
    builder.add_text_field(name="genre", stored=True, indexing=TextOptionIndexing())
    builder.add_text_field(name="song", stored=True, indexing=TextOptionIndexing())
    builder.add_facet_field(name="test_facet", stored=True)

    return builder.build("lyrics")


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_create_index(lyrics_index, toshi_container):
    unknown_index_response = {"message": "Unknown Index: 'lyrics' does not exist"}

    get_schema_summary_url = (
        f"{toshi_container}/{lyrics_index.name}/_summary?include_sizes=true"
    )
    res = requests.get(
        get_schema_summary_url, headers={"Content-Type": "application/json"}
    )
    assert res.json() == unknown_index_response

    client = ToshiClient(toshi_container)
    client.create_index(index=lyrics_index)

    get_schema_summary_url = (
        f"{toshi_container}/{lyrics_index.name}/_summary?include_sizes=true"
    )
    res = requests.get(
        get_schema_summary_url, headers={"Content-Type": "application/json"}
    )
    assert res.json() != unknown_index_response

    with pytest.raises(ToshiIndexError):
        client.create_index(index=lyrics_index)


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_get_index_summary(toshi_container, lyrics_index):
    client = ToshiClient(toshi_container)
    res = client.get_index_summary(name=lyrics_index.name)
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
def test_add_document(toshi_container, lyric_documents):
    client = ToshiClient(toshi_container)
    for doc in lyric_documents:
        client.add_document(document=doc)
    time.sleep(0.5)
    retrieved_doc = client.get_documents(document=Lyrics)

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
def test_list_indexes(toshi_container):
    client = ToshiClient(toshi_container)
    res = client.list_indexes()

    assert res == ["lyrics"]


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_search_term_query(
    toshi_container, black_keys_lyrics_document, lyric_documents
):
    client = ToshiClient(toshi_container)

    query = TermQuery(term="ceiling", field_name="lyrics")
    documents = client.search(query, Lyrics)

    assert len(documents) == 1
    assert documents[0] == black_keys_lyrics_document

    query = TermQuery(term="the", field_name="lyrics")
    documents = client.search(query, Lyrics)

    assert len(documents) == 3
    assert [d for d in documents] == [d for d in lyric_documents]


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_search_fuzzy_query(toshi_container, black_keys_lyrics_document):
    client = ToshiClient(toshi_container)
    query = FuzzyQuery(
        term="ceilin", field_name="lyrics", distance=2, transposition=False
    )

    documents = client.search(query, Lyrics)

    assert len(documents) == 1
    assert documents[0] == black_keys_lyrics_document


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_search_range_query(toshi_container):
    client = ToshiClient(toshi_container)
    gt = 1990
    lt = 2000
    query = RangeQuery(gt=gt, lt=lt, gte=gt, lte=lt, field_name="year")

    documents = client.search(query, Lyrics)
    assert len(documents) >= 1
    all(gt < doc.year < lt for doc in documents)


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_search_bool_query(toshi_container, black_keys_lyrics_document):
    client = ToshiClient(toshi_container)

    term_query = TermQuery(term="the", field_name="lyrics")
    gt = 1990
    lt = 2000
    range_query = RangeQuery(gt=gt, lt=lt, field_name="year")
    query = BoolQuery().must_match(term_query).must_not_match(range_query)

    documents = client.search(query, Lyrics)
    assert len(documents) >= 1
    assert documents[0] == black_keys_lyrics_document


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_regex_query(toshi_container, lyric_documents):
    regex = ".*"
    client = ToshiClient(toshi_container)

    query = RegexQuery(regex=regex, field_name="lyrics")
    documents = client.search(query, Lyrics)

    assert [d for d in sorted(documents, key=lambda doc: doc.artist)] == [
        d for d in sorted(lyric_documents, key=lambda doc: doc.artist)
    ]


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_phrase_query(toshi_container, radiohead_lyrics_document):
    client = ToshiClient(toshi_container)

    query = PhraseQuery(terms=["what", "the", "hell"], field_name="lyrics")
    documents = client.search(query, Lyrics)

    assert documents[0] == radiohead_lyrics_document


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_bulk_insert_documents(toshi_container):
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

    client = ToshiClient(toshi_container)
    client.bulk_insert_documents(documents=lyric_documents, commit=True)
    time.sleep(0.5)
    retrieved_doc = client.get_documents(document=Lyrics)

    assert len(retrieved_doc) == 6
    assert all(doc in retrieved_doc for doc in lyric_documents)


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_search_facet_query(
    toshi_container, black_keys_lyrics_document, lyric_documents
):
    client = ToshiClient(toshi_container)

    query = TermQuery(term="the", field_name="lyrics")
    facet_queries = [FacetQuery("test_facet", [Path("/t")])]
    documents = client.search(query, Lyrics, facet_query=facet_queries)

    assert len(documents) == 4


@pytest.mark.integration()
@pytest.mark.skipif(CI, reason="Integration Test")
def test_delete_index(toshi_container):
    client = ToshiClient(toshi_container)
    term_queries = [
        TermQuery(term="the", field_name="lyrics"),
        TermQuery(term="Nirvana", field_name="artist"),
    ]

    get_term_query = TermQuery(term="the", field_name="lyrics")
    documents = client.search(get_term_query, Lyrics)
    assert len(documents) == 4

    client.delete_term(term_queries=term_queries, index_name="lyrics", commit=True)
    time.sleep(0.5)

    documents = client.search(get_term_query, Lyrics)
    assert len(documents) == 0