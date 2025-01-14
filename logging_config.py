# logging_config.py
import logging
import logging.config
import sys


def setup_logging(default_level=logging.WARNING):
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
        },
        'handlers': {
            'default': {
                'level': default_level,
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': sys.stdout,
            },
        },
        'loggers': {
            '': {  # root logger
                'handlers': ['default'],
                'level': default_level,
                'propagate': True
            }
        }
    }

    logging.config.dictConfig(logging_config)


def get_logger(name):
    return logging.getLogger(name)