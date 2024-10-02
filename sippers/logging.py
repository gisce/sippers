# -*- coding: utf-8 -*-
"""
    sippers.logging
    ~~~~~~~~~~~~~~~

    Implements the logging support for SIPPERS

    You can use logging everywhere using::

        from sippers import logger
        logger.info('Info message')
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from raven import Client as SentryClient
from raven.handlers.logging import SentryHandler
from sippers import VERSION
from six import string_types

LOG_FORMAT = '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'


def setup_logging(level=None, logfile=None):
    """
    Setups sippers logging system.

    It will setup sentry logging if SENTRY_DSN environment is defined

    :param level: logging.LEVEL to set to logger (defaults INFO)
    :param logfile: File to write the log
    :return: logger
    """
    stream = logging.StreamHandler()
    stream.setFormatter(logging.Formatter(LOG_FORMAT))

    logger = logging.getLogger('sippers')
    for _h in logger.handlers[:]:
        _h.close()
        logger.removeHandler(_h)

    if logfile:
        hdlr = logging.FileHandler(logfile)
        formatter = logging.Formatter(LOG_FORMAT)
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)

    sentry = SentryClient()
    sentry.tags_context({'version': VERSION})
    sentry_handler = SentryHandler(sentry, level=logging.ERROR)
    logger.addHandler(sentry_handler)

    if isinstance(level, string_types):
        level = getattr(logging, level.upper(), None)

    if level is None:
        level = logging.INFO

    logger.setLevel(level)

    return logger
