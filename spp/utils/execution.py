from spp.engine.pipeline import Pipeline
from spp.utils.logging import Logger


LOG = Logger(__name__).get()


def construct_pipeline(config):

    LOG.debug("Constructing pipeline with name {}, platform {}, is_spark {}".format(
        config['name'], config['platform'], config['spark']
    ))
    pipeline = Pipeline(
        name=config['name'], run_id=config['run_id'],
        platform=config['platform'], is_spark=config['spark']
    )

    for method in config['methods']:
        LOG.debug("Adding method with name {}, module {}, queries {}, params {}".format(
            method['name'], method['module'], method['data_access'], method['params']
        ))
        pipeline.add_pipeline_methods(
            name=method['name'], module=method['module'], data_source=method['data_access'],
            data_target_prefix=method['data_target_prefix'], params=method['params'][0]
        )

    return pipeline


def run(pipeline, config):
    LOG.info("Running pipeline {}, run {}".format(pipeline.name, config['run_id']))
    pipeline.run(platform=config['platform'])
