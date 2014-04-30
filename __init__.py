import sys, os
sys.path.append(os.path.abspath(os.path.dirname(__file__))+'/api_github')
sys.path.append(os.path.abspath(os.path.dirname(__file__))+'/api_gitlab')

import logging
logger = logging.getLogger(__package__)

import twisted.python.log

class LogHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        twisted.python.log.msg(msg, logLevel=record.levelno)

logger.addHandler(LogHandler())
logger.setLevel(logging.DEBUG)
