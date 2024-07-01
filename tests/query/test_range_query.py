from toshi_client.query.range_query import RangeQuery


def test_assembly():
    lt = 1
    lte = 2
    gte = 3
    gt = 4

    query = RangeQuery(field_name="test")
    query.lt(lt).gte(gte).gt(gt).lte(lte)

    assert query._lt == lt
    assert query._gte == gte
    assert query._gt == gt
    assert query._lte == lte


def test_to_json():
    query = RangeQuery(gt=1, lte=100, field_name="test")
    assert query.to_json() == {"query": {"range": {"test": {"gt": 1, "lte": 100}}}}
