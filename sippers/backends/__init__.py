from urlparse import urlparse as std_urlparse
from sippers.backends.base import BaseBackend

_AVAILABLE_BACKENDS = {}


def register(name, cls):
    """Register a backend

    :type name: Backend name
    :type cls: Backend class
    """
    _AVAILABLE_BACKENDS[name] = cls


def urlparse(url):
    url = std_urlparse(url)
    config = {
        'backend': url.scheme,
        'username': url.username,
        'password': url.password,
        'hostname': url.hostname,
        'port': url.port,
        'db': url.path.lstrip('/')
    }
    return config


def get_backend(url):
    config = urlparse(url)
    backend = config['backend']
    if backend not in _AVAILABLE_BACKENDS:
        raise Exception(
            'Backend {} is not available/registered'.format(backend)
        )
    return _AVAILABLE_BACKENDS[backend]

# Import Backends
from sippers.backends.mongodb import MongoDBBackend