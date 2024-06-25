from __future__ import (absolute_import)
from sippers.backends import BaseBackend, register, urlparse
from sippers.parsers.cnmc_v2 import CnmcV2, CnmcV2Cons
from sippers.parsers.cnmc_gas import CnmcGas, CnmcGasCons
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
        self.index_tables()

    def index_tables(self):
        models = [CnmcV2, CnmcV2Cons, CnmcGas, CnmcGasCons]
        sips_table =[
            [self.ps_collection, 'name', True], [self.measures_collection, 'name', False]]
        for model in models:
            sips_table.append([model.collection, model.collection_index, model.index_unic])

        for table in sips_table:
            self.db[table[0]].ensure_index(
                table[1], unique=table[2], background=True
            )

    def insert(self, document):
        batch_insert = False
        docuemnt_list = document
        if isinstance(document, list):
            document = document[0]
            batch_insert = True
        ps = document.get('ps')
        if ps:
            if batch_insert:
                ps_data = []
                for x in docuemnt_list:
                    x.get('ps').backend = self
                    ps_data.append(x.get('ps').backend_data)
            else:
                ps.backend = self
                ps_data = ps.backend_data
            self.insert_ps(ps_data, collection=document.get('collection'))
        measures = document.get('measures')
        post_measures = []
        measure_cnmc = document.get('measure_cnmc')
        if measures:
            if batch_insert:
                raise Exception("Batch insert not implemented for measures")
            for measure in measures:
                measure.backend = self
                post_measures.append(measure.backend_data)
            self.insert_measures(post_measures)
        elif measure_cnmc:
            if batch_insert:
                measure_list = []
                for doc in docuemnt_list:
                    doc.get('measure_cnmc').backend = self
                    measure_list.append(doc.get('measure_cnmc').backend_data)
            else:
                measure_cnmc.backend = self
                measure_list = measure_cnmc.backend_data
            self.insert_cnmc_measure(measure_list, collection=document.get('collection'))

    def get(self, collection, filters, fields=None):
        return [x for x in self.db[collection].find(filters, fields=fields)]

    def insert_ps(self, ps, collection=None):
        if not collection:
            collection = self.ps_collection
        if collection == self.ps_collection:
            key = 'name'
        else:
            key = 'cups'

        if isinstance(ps, list):
            oid = False
            for doc in ps:
                oid = self.db[collection].update(
                    {key: doc[key]}, doc, upsert=True
                )
        else:
            oid = self.db[collection].update(
                {key: ps[key]}, ps, upsert=True
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

    def insert_cnmc_measure(self, value, collection=None):
        '''cnmc measures come a measure per line,
        cannot replace the whole block as in insert_measures'''

        oid = self.db[collection].insert(value)

        return oid

    def disconnect(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()



register("mongodb", MongoDBBackend)
