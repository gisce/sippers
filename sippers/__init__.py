from __future__ import absolute_import

try:
    VERSION = __import__('pkg_resources') \
        .get_distribution(__name__).version
except Exception, e:
    VERSION = 'unknown'


from sippers.logging import setup_logging


logger = setup_logging()
