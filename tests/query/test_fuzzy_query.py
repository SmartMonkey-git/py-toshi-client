import pytest

from toshi_client.query.fuzzy_query import FuzzyQuery


def test_init():
    with pytest.raises(ValueError):
        FuzzyQuery(
            term="ceilin", field_name="lyrics", distance=500, transposition=False
        )

    query = FuzzyQuery(
        term="ceilin", field_name="lyrics", distance=2, transposition=False
    )

    assert isinstance(query, FuzzyQuery)


def test_to_json():
    query = FuzzyQuery(
        term="ceilin", field_name="lyrics", distance=2, transposition=False
    )
    assert query.to_json() == {
        "query": {
            "fuzzy": {
                "lyrics": {"value": "ceilin", "distance": 2, "transposition": False}
            }
        }
    }
