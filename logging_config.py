# logging_config.py
import logging
import logging.config
import sys


class NoSelectorDataFilter(logging.Filter):
    """Filter out repetitive 'No data available for set bit' messages."""
    def filter(self, record):
        return "No data available for set bit" not in record.msg


def setup_logging(default_level=logging.WARNING):
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
            'minimal': {
                'format': '%(message)s'
            }
        },
        'filters': {
            'no_selector_spam': {
                '()': NoSelectorDataFilter
            }
        },
        'handlers': {
            'default': {
                'level': logging.ERROR,  # Changed from INFO to ERROR
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': sys.stdout,
                'filters': ['no_selector_spam']
            },
            'end_tag_handler': {
                'level': logging.DEBUG,  # Changed from INFO to DEBUG
                'formatter': 'standard', # Using standard format for timestamps
                'class': 'logging.StreamHandler',
                'stream': sys.stdout,
            }
        },
        'loggers': {
            '': {  # root logger
                'handlers': ['default'],
                'level': logging.ERROR,  # Changed from INFO to ERROR
                'propagate': True
            },
            'journal': {
                'handlers': ['default'],
                'level': logging.ERROR, # Changed from INFO to ERROR
                'propagate': False
            },
            'journal.end_tag': {
                'handlers': ['end_tag_handler'],
                'level': logging.DEBUG,  # Keep at DEBUG
                'propagate': False
            }
        }
    }

    logging.config.dictConfig(logging_config)


def get_logger(name):
    return logging.getLogger(name)