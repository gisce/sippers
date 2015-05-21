from __future__ import absolute_import

from osconf import config_from_environment

try:
    VERSION = __import__('pkg_resources') \
        .get_distribution(__name__).version
except Exception, e:
    VERSION = 'unknown'


from sippers.logging import setup_logging

logging_config = config_from_environment('SIPPERS_LOGGING')

logger = setup_logging(**logging_config)
