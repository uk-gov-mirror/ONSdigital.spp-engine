from spp.engine import read
from spp.utils.query import Query
import pandas as pd
from pandas.testing import assert_frame_equal
from unittest.mock import patch


def test_spark_read_db(create_session):
    df = create_session.read.csv('./tests/resources/data/dummy.csv')
    df.createOrReplaceGlobalTempView('view')
    assert read.spark_read(
        create_session, Query(database='global_temp', table='view',
                              environment="sandbox", survey="BMI_SG",
                              select='*', run_id="fake_run_id"),
        "sandbox", "fake_run_id", "BMI_SG"
    ).collect() == df.collect()


def test_spark_read_csv(create_session):
    df = create_session.read.csv('./tests/resources/data/dummy.csv')
    assert read.spark_read(
        create_session, './tests/resources/data/dummy.csv', "sandbox",
        "fake_run_id", "BMI_SG"
    ).collect() == df.collect()


def test_spark_read_json(create_session):
    df = create_session.read.json('./tests/resources/data/dummy.json')
    assert read.spark_read(
        create_session, './tests/resources/data/dummy.json', "sandbox",
        "fake_run_id", "BMI_SG"
    ).collect() == df.collect()


@patch('spp.engine.read.PandasAthenaReader.read_db')
def test_pandas_read_db(mock_instance_method):
    mock_instance_method.return_value = pd.read_csv('./tests/resources/data/dummy.csv')
    df = pd.read_csv('./tests/resources/data/dummy.csv')
    from spp.engine.read import PandasAthenaReader
    assert_frame_equal(
        read.pandas_read(Query(database='FAKEDB',
                               table='FAKETABLE',
                               environment="sandbox",
                               survey="BMI_SG",
                               select=['col1', 'col2'],
                               where='col1 = 1',
                               run_id="fake_run_id"),
                         "sandbox",
                         "fake_run_id",
                         "survey",
                         PandasAthenaReader()), df)


def test_pandas_read_db_not_implemented():
    from spp.engine.read import PandasReader
    from spp.utils.query import Query
    raised = False
    try:
        read.pandas_read(
            Query(database='FAKEDB',
                  table='FAKETABLE',
                  environment="sandbox",
                  survey="BMI_SG",
                  select=['col1', 'col2'],
                  where='col1 = 1',
                  run_id="fake_run_id"),
            "sandbox",
            "fake_run_id",
            "BMI_SG",
            PandasReader())
    except NotImplementedError:
        raised = True
    finally:
        assert raised


def test_pandas_read_csv():
    df = pd.read_csv('./tests/resources/data/dummy.csv')
    assert_frame_equal(read.pandas_read('./tests/resources/data/dummy.csv',
                                        "sandbox", "fake_run_id",
                                        "BMI_SG"), df)


def test_pandas_read_json():
    df = pd.read_json('./tests/resources/data/dummy.json', lines=True)
    assert_frame_equal(
        read.pandas_read('./tests/resources/data/dummy.json',
                         "sandbox", "fake_run_id", "BMI_SG",
                         lines=True), df)
