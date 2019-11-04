class Runner:

    pipeline = None

    def __init__(self, config):
        self.pipeline = self._parse_config(config)


    def _parse_config(self, config):
        """
        Will read through a JSON config to create a pipeline object
        :param config:
        :return:
        """
        return config

    def run_pipeline(self):
        self.pipeline.run()

