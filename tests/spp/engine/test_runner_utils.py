# parse json data
# validate json data
# mock data access
# check pipeline runs


import json
from pyspark.sql import SparkSession

from spp.engine.pipeline import Platform, construct_pipeline

with open("./tests/resources/config/test_bd_pipeline.json") as f:
    test_bd_json = json.load(f)

with open("./tests/resources/config/test_sd_pipeline.json") as f:
    test_sd_json = json.load(f)


def test_parse_config_bd():
    pipeline = construct_pipeline(test_bd_json['pipeline'], "BMI_SG")

    assert pipeline.name == 'test_pipeline'
    assert isinstance(pipeline.spark, SparkSession)
    assert pipeline.platform == Platform.AWS.value

    assert pipeline.methods[0].module_name == 'tests.test_methods.bd.big_data'
    assert pipeline.methods[0].method_name == 'method_a'
    assert pipeline.methods[0].params == {
        "param_1": "col_1",
        "param_2": "col_2",
        "param_3": "col_3"
    }

    assert pipeline.methods[1].module_name == 'tests.test_methods.bd.big_data'
    assert pipeline.methods[1].method_name == 'method_b'
    assert pipeline.methods[1].params == {
        "param_1": "reporting_date",
        "param_2": "entity_name"
    }

    # TODO: Add DataAccess parsing


def test_parse_config_sd():
    pipeline = construct_pipeline(test_sd_json['pipeline'], "BMI_SG")

    assert pipeline.name == 'test_sd_pipeline'
    assert not pipeline.spark
    assert pipeline.platform == Platform.AWS.value

    assert pipeline.methods[0].module_name == 'tests.test_methods.sd.small_data'
    assert pipeline.methods[0].method_name == 'method_c'
    assert pipeline.methods[0].params == {
        "param_1": "col_1",
        "param_2": "col_2",
        "param_3": "col_3"
    }

    assert pipeline.methods[1].module_name == 'tests.test_methods.sd.small_data'
    assert pipeline.methods[1].method_name == 'method_d'
    assert pipeline.methods[1].params == {
        "param_1": "reporting_date",
        "param_2": "entity_name"
    }

    # TODO: Add DataAccess parsing
