# Tests in this file
#  1. Has database and table no select or where
#  2. Has database and table single select no where
#  3. Has database and table list select no where
#  4. Has database and table no select single where
#  5. Has database and table no select list where
#  6. Has database and table single select single where
#  7. has database and table list select list where
#  8. Missing database
#  9. Missing table
# 10. Missing both

from spp.engine.query import Query
import pytest

def test_db_and_table():
    expected_query = "SELECT * FROM test.test;"
    assert expected_query == Query("test", "test")


def test_db_table_single_select():
    expected_query = "SELECT column FROM test.test;"
    assert expected_query == Query("test", "test", "column")


def test_db_table_list_select():
    expected_query = "SELECT column_a, column_b FROM test.test;"
    assert expected_query == Query("test", "test", ["column_a", "column_b"])


def test_db_table_single_where():
    expected_query = "SELECT * FROM test.test WHERE column == 500;"
    assert expected_query == Query("test", "test", where={"column": {"condition": "==", "value": "500"}})


def test_db_table_list_where():
    expected_query = "SELECT * FROM test.test WHERE column_a > 500 AND column_b = this AND column_c < 100;"
    assert expected_query == Query("test", "test", where={"column_a": {"condition": ">", "value": "500"},
                                                          "column_b": {"condition": "=", "value": "this"},
                                                          "column_c": {"condition": "<", "value": "100"}})


def test_db_table_single_select_where():
    expected_query = "SELECT column FROM test.test WHERE column = test"
    assert expected_query == Query("test", "test", "column", {"column": {"condition": "=", "value": "test"}})


def test_db_table_list_select_where():
    expected_query = "SELECT column_a, column_b, column_c FROM test.test WHERE column_a = 500 AND column_b > 250 AND" \
                     " column_c < 500;"
    assert expected_query == Query("test", "test", ["column_a", "column_b", "column_c"],
                                   {"column_a": {"condition": "=", "value": "500"},
                                    "column_b": {"condition": ">", "value": "250"},
                                    "column_c": {"condition": "<", "value": "500"}})


def test_missing_db():
    with pytest.raises(QueryError):
        Query(None, "test")


def test_missing_table():
    with pytest.raises(QueryError):
        Query("test", None)


def test_missing_both():
    with pytest.raises(QueryError):
        Query(None, None)