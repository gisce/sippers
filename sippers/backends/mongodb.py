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
           "name", background=True,
        )

    def insert(self, document):
        ps = document.get('ps')
        if ps:
            ps.backend = self
            self.insert_ps(ps.backend_data)
        measures = document.get('measures')
        post_measures = []
        measure_cnmc = document.get('measure_cnmc')
        if measures:
            for measure in measures:
                measure.backend = self
                post_measures.append(measure.backend_data)
            self.insert_measures(post_measures)
        elif measure_cnmc:
            measure_cnmc.backend = self
            self.insert_cnmc_measure(measure_cnmc.backend_data)

    def get(self, collection, filters, fields=None):
        return [x for x in self.db[collection].find(filters, fields=fields)]

    def insert_ps(self, ps):
        collection = self.ps_collection
        oid = self.db[collection].update(
            {'name': ps['name']}, ps, upsert=True
        )
        return oid

    def insert_measures(self, values):
        collection = self.measures_collection
        oids = []
        self.db[collection].remove(
            {"name": values[0]["name"]}
        )
        oids.extend(self.db[collection].insert(values))
        return oids

    def insert_cnmc_measure(self, value):
        '''cnmc measures come a measure per line,
        cannot replace the whole block as in insert_measures'''
        collection = self.measures_collection
        self.db[collection].remove(
            {"name" : value["name"],
             "data_final": value["data_final"]
             }
        )
        oid = self.db[collection].insert(value)
        return oid

    def disconnect(self):
        self.connection.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()



register("mongodb", MongoDBBackend)
