from query import TermQuery
from query.bool_query import BoolQuery
from query.range_query import RangeQuery


def test_to_json():
    term_query = TermQuery(term="the", field_name="lyrics")
    gt = 1990
    lt = 2000
    range_query = RangeQuery(gt=gt, lt=lt, field_name="year")
    query = (
        BoolQuery()
        .must_match(term_query)
        .must_not_match(range_query)
        .should_match(term_query)
    )

    assert query.to_json() == {
        "query": {
            "bool": {
                "must": [{"term": {"lyrics": "the"}}],
                "must_not": [{"range": {"year": {"gt": 1990, "lt": 2000}}}],
                "should": [{"term": {"lyrics": "the"}}],
            }
        }
    }
