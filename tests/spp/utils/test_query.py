# Tests in this file
#  1. Has database and table no select or where
#  2. Has database and table single select no where
#  3. Has database and table list select no where
#  4. Has database and table no select single where
#  5. Has database and table no select list where
#  6. Has database and table single select single where
#  7. has database and table list select list where

from spp.utils.query import Query


def test_db_and_table():
    expected_query = "SELECT * FROM test.test;"

    assert expected_query == str(Query(database="test", table="test",
                                       environment="sandbox", survey="BMI_SG",
                                       run_id="fake_run_id"))


def test_db_table_single_select():
    expected_query = "SELECT column FROM test.test;"
    assert expected_query == str(Query(database="test", table="test",
                                       environment="sandbox", survey="BMI_SG",
                                       select="column", run_id="fake_run_id"))


def test_db_table_list_select():
    expected_query = "SELECT column_a, column_b FROM test.test;"
    assert expected_query == str(Query(database="test", table="test",
                                       environment="sandbox", survey="BMI_SG",
                                       select=["column_a", "column_b"],
                                       run_id="fake_run_id"))


def test_db_table_single_where():
    expected_query = "SELECT * FROM test.test WHERE column == 500;"
    print(str(Query("test", "test", where={"column":
                                           {"condition": "==", "value": "500"}},
                    environment="sandbox", survey="BMI_SG", run_id="fake_run_id")))
    print(expected_query)
    assert expected_query == str(Query("test", "test",
                                       where=[{"column": "column",
                                               "condition": "==", "value": "500"}],
                                       environment="sandbox", survey="BMI_SG",
                                       run_id="fake_run_id"))


def test_db_table_list_where():
    expected_query = "SELECT * FROM test.test " \
                     "WHERE column_a > 500 " \
                     "AND column_b = this AND column_c < 100;"
    assert expected_query == str(Query("test", "test",
                                       where=[{"column": "column_a",
                                               "condition": ">", "value": "500"},
                                              {"column": "column_b",
                                               "condition": "=", "value": "this"},
                                              {"column": "column_c",
                                               "condition": "<", "value": "100"}],
                                       environment="sandbox", survey="BMI_SG",
                                       run_id="fake_run_id"))


def test_db_table_single_select_where():
    expected_query = "SELECT column FROM test.test WHERE column = test;"
    assert expected_query == str(Query("test", "test", select="column",
                                       where=[{"column": "column",
                                               "condition": "=", "value": "test"}],
                                       environment="sandbox", survey="BMI_SG",
                                       run_id="fake_run_id"))


def test_db_table_list_select_where():
    expected_query = "SELECT column_a, column_b, column_c FROM test.test " \
                     "WHERE column_a = 500 AND column_b > 250 AND" \
                     " column_c < 500;"

    assert expected_query == str(Query("test", "test",
                                       select=["column_a", "column_b", "column_c"],
                                       where=[{"column": "column_a",
                                               "condition": "=", "value": "500"},
                                              {"column": "column_b",
                                               "condition": ">", "value": "250"},
                                              {"column": "column_c",
                                               "condition": "<", "value": "500"}],
                                       environment="sandbox", survey="BMI_SG",
                                       run_id="fake_run_id"))
