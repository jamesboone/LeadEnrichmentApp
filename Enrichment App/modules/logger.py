from __future__ import absolute_import
import logging
import logging.config
import os
import errno

HANDLERS = {
    "latest": {
        "class": "logging.FileHandler",
        "filename": "latest.log",
        "mode": "w",
        "formatter": "verbose"
    },
    "history": {
        "class": "logging.handlers.RotatingFileHandler",
        "maxBytes": 1000000,
        "filename": "history.log",
        "backupCount": 1,
        "formatter": "verbose"
    }
}

CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "root": {
        "handlers": ["main"],
        "level": "INFO"
    },

    "formatters": {
        "simple": {
            "format": "%(processName)-7s %(levelname)-8s %(name)s(%(lineno)d) %(message)s"

        },
        "verbose": {
            "format": "%(asctime)s: %(levelname)-8s %(name)s(%(lineno)d) %(message)s"
                      " [%(processName)s:%(threadName)s]"
        },
    },

    "handlers": {
        "main": {
            "class": "logging.StreamHandler",
            "formatter": "simple"
        },
    }

}


def initialize(log_dir):
    """ Install our initial logging config """
    def makedirs(path, mode=None):
        try:
            os.makedirs(*([path] if mode is None else [path, mode]))
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise e

    CONFIG['root']['handlers'].extend(['history', 'latest'])
    CONFIG['handlers']['history'] = HANDLERS['history']
    CONFIG['handlers']['latest'] = HANDLERS['latest']

    for handler, config in CONFIG['handlers'].iteritems():
        if 'filename' in config:
            config['filename'] = os.path.join(log_dir, config['filename'])

    # Install our config
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("oauth2client").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)

    makedirs(log_dir)
    logging.config.dictConfig(CONFIG)
    return logging
