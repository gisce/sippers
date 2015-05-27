from unittest import TestCase

from sippers.backends import urlparse, get_backend
from sippers.backends.mongodb import MongoDBBackend


class TestBackends(TestCase):

    def test_get_config(self):
        config = urlparse("mongodb://username:password@host:1234/sips")
        self.assertDictEqual(config, {
            'backend': 'mongodb',
            'username': 'username',
            'password': 'password',
            'hostname': 'host',
            'port': 1234,
            'db': 'sips'
        })

    def test_get_backend(self):
        backend = get_backend("mongodb://username:password@host:1234/sips")
        self.assertEqual(backend, MongoDBBackend)
