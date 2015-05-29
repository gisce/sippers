from sippers.backends import BaseBackend, register, urlparse
import pymongo


class MongoDBBackend(BaseBackend):
    """MongoDB Backend
    """
    def __init__(self, uri=None):
        if uri is None:
            uri = "mongodb://localhost:27017/sips"
        super(MongoDBBackend, self).__init__(uri)
        self.uri = uri
        self.config = urlparse(self.uri)
        self.connection = pymongo.MongoClient(self.uri)
        self.db = self.connection[self.config['db']]
        self.ps_collection = 'giscedata_sips_ps'
        self.measures_collection = 'giscedata_sips_consums'
        self.db[self.ps_collection].ensure_index(
            "name", unique=True, background=True
        )
        self.db[self.measures_collection].ensure_index(
            [("name", pymongo.TEXT), ("data_final", pymongo.DESCENDING)],
            unique=True, background=True,
        )

    def insert(self, document):
        ps = document.get('ps')
        if ps:
            self.insert_ps(ps)
        measures = document.get('measures')
        if measures:
            self.insert_measures(measures)

    def insert_ps(self, ps):
        collection = self.ps_collection
        oid = self.db[collection].update(
            {'name': ps['name']}, ps, upsert=True
        )
        return oid

    def insert_measures(self, values):
        collection = self.measures_collection
        oids = []
        if not isinstance(values, list):
            values = [values]
        for measure in iter(values):
            oids.append(self.db[collection].update(
                {"name": measure['name'], "data_final": measure['data_final']},
                measure,
                upsert=True
            ))
        return oids

    def disconnect(self):
        self.connection.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()



register("mongodb", MongoDBBackend)
