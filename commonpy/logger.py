"""
reaper logger functions.
"""
import logging
import os
import sys

from commonpy.singleton import Singleton

DEFAULT_LOG_LEVEL = 'INFO'


class Logger(object, metaclass=Singleton):
    def __init__(self, appname=None, level=None):
        """
        Get a logger object for application-wide use.
        """
        avail_levels = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO,
                        'WARNING': logging.WARNING, 'ERROR': logging.ERROR,
                        'CRITICAL': logging.CRITICAL}

        appname = appname or os.environ.get('APPNAME', __name__)
        level_str = str(level or os.environ.get('LOG_LEVEL', DEFAULT_LOG_LEVEL)).upper()

        self._logger = logging.getLogger(appname)
        self._logger.addHandler(logging.StreamHandler(sys.stdout))
        if level_str in avail_levels:
            self._logger.setLevel(avail_levels[level_str])
            self._loglevel_str = level_str
        else:
            print("Can't set log level to {}".format(level_str))
        super().__init__()

    @property
    def logger(self):
        return self._logger

    @property
    def loglevel_str(self):
        return self._loglevel_str
