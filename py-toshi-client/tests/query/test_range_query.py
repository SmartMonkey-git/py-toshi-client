import pytest

from query.range_query import RangeQuery


def test_init():
    with pytest.raises(ValueError):
        RangeQuery(gt=1, gte=1, field_name="test")
    with pytest.raises(ValueError):
        RangeQuery(lt=1, lte=1, field_name="test")
    with pytest.raises(ValueError):
        RangeQuery(gt=1, gte=1, lt=1, lte=1, field_name="test")

    query = RangeQuery(gt=1, lte=1, field_name="test")

    assert isinstance(query, RangeQuery)


def test_to_json():
    query = RangeQuery(gt=1, lte=100, field_name="test")
    assert query.to_json() == {"query": {"range": {"test": {"gt": 1, "lte": 100}}}}
