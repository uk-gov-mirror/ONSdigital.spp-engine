import logging
import sys
import os


class Logger(object):
    """
    When called creates a new logger to stdout
    """
    def __init__(self, name):
        logger = logging.getLogger("uk.gov.ons.%s" % name)
        logger.setLevel(os.getenv('LOGGING_LEVEL', logging.INFO))
        formatter = logging.Formatter('%(asctime)s - %(name)s - '
                                      '%(levelname)s - %(message)s')

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        stdout_handler.setLevel(os.getenv('LOGGING_LEVEL', logging.INFO))
        logger.addHandler(stdout_handler)
        self._logger = logger

    def get(self):
        """
        :return: logger
        """
        return self._logger
