import logging
import sys


class Logger(object):
    """
    When called creates a new logger to stdout
    """
    def __init__(self, name):
        logger = logging.getLogger("uk.gov.ons.%s" % name)
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        stdout_handler.setLevel(logging.DEBUG)
        logger.addHandler(stdout_handler)
        self._logger = logger

    def get(self):
        """
        :return: logger
        """
        return self._logger
