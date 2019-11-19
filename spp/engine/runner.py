from spp.engine.pipeline import Pipeline
from spp.utils.logging import Logger


LOG = Logger(__name__).get()


class Runner:

    def __init__(self, config):
        self.config = config['pipeline']
        self.run_id = self.config['run_id']
        self.pipeline = self._build_from_config()

    def run(self):
        LOG.info("Running pipeline {}, run {}".format(self.pipeline.name, self.run_id))
        self.pipeline.run(platform=self.config['platform'])

    def _build_from_config(self):
        LOG.debug("Constructing pipeline with name {}, platform {}, is_spark {}".format(
            self.config['name'], self.config['platform'], self.config['spark']
        ))
        pipeline = Pipeline(name=self.config['name'], platform=self.config['platform'], is_spark=self.config['spark'])

        for method in self.config['methods']:
            LOG.debug("Adding method with name {}, module {}, queries {}, params {}".format(
                method['name'], method['module'], method['data_access'], method['params']
            ))
            pipeline.add_pipeline_methods(
                name=method['name'], module=method['module'], queries=method['data_access'], params=method['params'][0]
            )

        return pipeline
