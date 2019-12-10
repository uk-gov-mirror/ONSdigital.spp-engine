from __future__ import unicode_literals

import json
import sys

from spp.utils.logging import Logger
from spp.engine.pipeline import construct_pipeline

import boto3
from awsglue.utils import getResolvedOptions

LOG = Logger(__name__).get()

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

args = getResolvedOptions(sys.argv, ['config'])
config_parameters_string = (args['config']).replace("'", '"').replace("True", "true").replace("False", "false")
config = json.loads(config_parameters_string)['pipeline']

pipeline = construct_pipeline(config)
LOG.info("Running pipeline {}, run {}".format(pipeline.name, config['run_id']))
pipeline.run(platform=config['platform'])