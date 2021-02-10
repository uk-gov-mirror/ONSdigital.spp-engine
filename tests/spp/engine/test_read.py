from es_aws_functions import general_functions

from spp.engine.pipeline import *

logger = general_functions.get_logger(
        "test_survey", "test_read.py", "test_env", "test_run_id"
    )


def test_spark_read_db(create_session):
    df = create_session.read.csv('./tests/resources/data/dummy.csv')
    df.createOrReplaceGlobalTempView('view')
    assert spark_read(
        create_session, Query(database='global_temp', table='view',
                              select='*', run_id="fake_run_id"),
        logger
    ).collect() == df.collect()


def test_spark_read_csv(create_session):
    df = create_session.read.csv('./tests/resources/data/dummy.csv')
    assert spark_read(
        create_session, './tests/resources/data/dummy.csv', logger
    ).collect() == df.collect()


def test_spark_read_json(create_session):
    df = create_session.read.json('./tests/resources/data/dummy.json')
    assert spark_read(
        create_session, './tests/resources/data/dummy.json', logger
    ).collect() == df.collect()
