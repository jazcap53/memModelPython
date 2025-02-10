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
                'level': logging.INFO,
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': sys.stdout,
            },
            'end_tag_handler': {  # New handler for end tag operations
                'level': logging.DEBUG,
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': sys.stdout,
            }
        },
        'loggers': {
            '': {  # root logger
                'handlers': ['default'],
                'level': default_level,
                'propagate': True
            },
            'journal.end_tag': {  # Specific logger for end tag operations
                'handlers': ['end_tag_handler'],
                'level': logging.DEBUG,
                'propagate': False
            }
        }
    }

    logging.config.dictConfig(logging_config)
    # logging.getLogger().setLevel(logging.INFO)



def get_logger(name):
    return logging.getLogger(name)