# parse json data
# validate json data
# mock data access
# check pipeline runs

import json
from scripts.runner import parse_config
from spp.engine.pipeline import Platform
from pyspark.sql import SparkSession

test_sd_json = None
test_bd_json = None

test_sd_pipeline = None
test_bd_pipeline = None

with open("./tests/resources/config/test_bd_pipeline.json") as f:
    test_bd_json = json.load(f)

with open("./tests/resources/config/test_sd_pipeline.json") as f:
    test_sd_json = json.load(f)


def test_parse_config_bd():
    test_bd_pipeline = parse_config(test_bd_json)

    assert test_bd_pipeline.name == "test_pipeline"
    assert isinstance(test_bd_pipeline.spark, SparkSession)
    assert test_bd_pipeline.platform.value == Platform.AWS.value

    # Tests method details and data access details


    test_bd_pipeline.run()
