import json
from spp.utils.logging import Logger
from spp.engine.pipeline import Pipeline, Platform, PipelineMethod

LOG = Logger(__name__).get()


pipeline = None


def parse_config(config):
    """
    Will read through a JSON config to create a pipeline object
    :param config:
    :return:
    """
    conf = config.get("pipeline")
    pipeline = Pipeline(conf.get("name"), conf.get('platform'), conf.get('spark'))

    methods = conf.get("methods")
    for k,v in methods:
        pipeline.add_pipeline_methods()

    return pipeline

