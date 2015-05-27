from sippers.backends import BaseBackend, register, urlparse
import pymongo


class MongoDBBackend(BaseBackend):
    """MongoDB Backend
    """
    def __init__(self, url):
        self.uri = url
        self.config = urlparse(url)
        self.connection = pymongo.MongoClient(url)
        self.db = self.connection[self.config['db']]

    def insert_ps(self, values, collection='giscedata_sips_ps'):
        self.db[collection].insert(values)

    def insert_measures(self, values, collection='giscedata_sips_consums'):
        self.db[collection].insert(values)

    def disconnect(self):
        self.connection.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()



register("mongodb", MongoDBBackend)
