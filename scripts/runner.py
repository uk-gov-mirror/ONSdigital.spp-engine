from __future__ import unicode_literals

import json
import sys

from spp.utils.execution import construct_pipeline, run
from spp.utils.logging import Logger


import boto3
from awsglue.utils import getResolvedOptions


LOG = Logger(__name__).get()

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

args = getResolvedOptions(sys.argv, ['config'])
config_parameters_string = (args['config']).replace("'", '"').replace('"True"', "true").replace('"False"', "false")
config = json.loads(config_parameters_string)['pipeline']

if config['spark']:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('DTrade').getOrCreate()

pipeline = construct_pipeline(config)
run(pipeline, config)
