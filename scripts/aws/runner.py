from __future__ import unicode_literals

import json
import sys

from spp.engine.pipeline import construct_pipeline

from awsglue.utils import getResolvedOptions
from es_aws_functions import general_functions

current_module = "spp-res_glu_emr"
args = getResolvedOptions(sys.argv, ['config', 'crawler-name'])
# may need to change name to 'crawler_name'(depending on param name on aws job config)
# NOTE : awsglue util module 'getResolvedOptions' is cutting off characters '}}' at the
# end of config json. So workaround is add '}}' as below
config_parameters_string = (args['config']).replace("'", '"').\
                               replace("True", "true").replace("False", "false")+'}}'
config = json.loads(config_parameters_string)['pipeline']
survey = config_parameters_string['survey']
run_id = config['run_id']
environment = config['environment']
crawler = args['crawler_name']

try:
    logger = general_functions.get_logger(survey, current_module,
                                          environment, run_id)
except Exception as e:
    raise Exception("{}:Exception raised: {}".format(current_module, e))

try:
    logger.info("Config variables loaded.")
    pipeline = construct_pipeline(config, survey)
    logger.info("Running pipeline {}, run {}".format(pipeline.name, config['run_id']))
    pipeline.run(platform=config['platform'], crawler_name=crawler,
                 survey=survey, environment=environment, run_id=run_id)
except Exception as e:
    logger.error("Error constructing and/or running pipeline: ", e)
    raise Exception("{}:Exception raised: {}".format(current_module, e))
