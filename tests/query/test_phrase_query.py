import pytest

from toshi_client.query.phrase_query import PhraseQuery


def test_init():
    with pytest.raises(ValueError):
        PhraseQuery(terms=["a", "t"], field_name="lyrics", offsets=[5, 3, 6])

    query = PhraseQuery(terms=["a", "t"], field_name="lyrics")

    assert isinstance(query, PhraseQuery)


def test_to_json():
    query = PhraseQuery(
        terms=["what", "the", "hell"], field_name="lyrics", offsets=[5, 3, 100], limit=5
    )

    assert query.to_json() == {
        "query": {
            "phrase": {
                "lyrics": {"terms": ["what", "the", "hell"], "offsets": [5, 3, 100]}
            }
        },
        "limit": 5,
    }
