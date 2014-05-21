# -*- coding: utf-8 -*-
from datetime import datetime
import tablib
import pymongo

def parse_datetime(value, dataformat):
    # Funcio per l'add_formatter converteixi de string a datetime
    try:
        res = datetime.strptime(value, dataformat)
    except:
        res = None
    return res

def parse_float(value):
    # Funcio per l'add_formatter converteixi valors en coma a float amb punt
    try:
        punts = value.replace(',', '.')
        deci = punts.split('.')[-1]
        nume = punts.split('.')[:-1]
        if nume:
            res = float('{}.{}'.format(''.join(nume), deci))
        else:
            res = value
    except:
        res = None
    return res

def slices(s, *args):
    # Funció per tallar un string donades les mides dels camps.
    # La llista amb les mides dels camps entren per els args.
    position = 0
    for length in args:
        yield s[position:position + length]
        position += length

MAGNITUDS = {
    'Wh': 1000,
    'kWh': 1
}

class Parser(object):
    types = []
    header_conf = []
    positions = []
    magnitudes = []
    data = None
    mongodb = None
    fields = None

    def __init__(self, mongodb=None):
        #Creo el dataset buit
        self.data = tablib.Dataset()
        self.mongodb =mongodb


    def load(self):
        self.load_config()
        self.prepare_data_set()
        self.validate_mongo_counters()
        self.prepare_mongo()

    def load_config(self):
        raise NotImplementedError( "Should have implemented this")

    def insert_mongo(self, document, collection):
        # Afegeixo les entrades
        try:
            pvalues = [document[k] for k in self.pkeys]
            query = dict(zip(self.pkeys, pvalues))

            res = collection.update(query, document)
            if res['updatedExisting'] is False:
                collection.insert(document)
        except pymongo.errors.OpertionFailure:
            self.flog.write("Error: A l'insert del mongodb")

        return True

    def prepare_data_set(self):
        self.data = tablib.Dataset()
        self.data.headers = self.headers_conf

        for field, v in zip(self.fields, self.types):
            if v == 'float':
                self.data.add_formatter(field[0],
                                        lambda a: a and parse_float(a) or 0)
            if v == 'integer':
                self.data.add_formatter(field[0],
                                        lambda a:
                                        a and int(parse_float(a)) or 0)
            if v == 'datetime':
                self.data.add_formatter(field[0],
                                        lambda a:
                                        a and parse_datetime(a,
                                                             self.data_format))
            if v == 'long':
                self.data.add_formatter(field[0],
                                        lambda a: a and long(a) or 0)

        # Passar a kW les potencies que estan en W
        for field, v in zip(self.fields, self.magnitudes):
            if v == 'Wh':
                self.data.add_formatter(field[0],
                                        lambda a:
                                        a and float(a)/MAGNITUDS['Wh'] or 0)
            elif v == 'kWh':
                self.data.add_formatter(field[0],
                                        lambda a:
                                        a and float(a)/MAGNITUDS['kWh'] or 0)
    def validate_mongo_counters(self):
        # Comprovo que la collecció estigui creada, si no la creo
        if not self.mongodb['counters'].count():
            self.mongodb['counters'].save({"_id": self.classe, "counter": 1})

    def prepare_mongo(self):
        raise NotImplementedError( "Should have implemented this")

    def parse_line(self, line):
        raise NotImplementedError( "Should have implemented this")





