# -*- coding: utf-8 -*-
from __future__ import absolute_import
from datetime import datetime
import os
import re
import tablib


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


_PARSERS = {

}


def register(cls):
    """Register a parser

    :type cls: Parser class
    """
    module = cls.__module__
    path = '.'.join([module, cls.__name__])
    _PARSERS[path] = cls


def get_parser(sips_file):
    for path, cls in _PARSERS.items():
        if cls.detect(sips_file):
            return cls
    return None


class Parser(object):

    encoding = "iso-8859-15"

    @classmethod
    def detect(cls, sips_file):
        if cls.pattern:
            return re.match(cls.pattern, os.path.basename(sips_file))
        return False

    def __init__(self):
        self.types = []
        self.headers_conf = []
        self.positions = []
        self.magnitudes = []
        self.vals_long = []
        self.fields = None
        self.pkeys = None
        self.date_format = None
        self.pattern = None

    def load_config(self):
        raise NotImplementedError("Should have implemented this")

    def prepare_data_set(self, fields, types, headers_conf, magnitudes):
        data = tablib.Dataset()
        data.headers = headers_conf

        for field, v in zip(fields, types):
            if v == 'float':
                data.add_formatter(field[0],
                                   lambda a: a and parse_float(a) or 0)
            if v == 'integer':
                data.add_formatter(field[0],
                                   lambda a: a and int(parse_float(a)) or 0)
            if v == 'datetime':
                data.add_formatter(
                    field[0], lambda a: a and parse_datetime(a,
                                                             self.date_format))
            if v == 'long':
                data.add_formatter(field[0], lambda a: a and long(a) or 0)

        # Passar a kW les potencies que estan en W
        for field, v in zip(fields, magnitudes):
            if v == 'Wh':
                data.add_formatter(
                    field[0], lambda a: a and float(a)/MAGNITUDS['Wh'] or 0)
            elif v == 'kWh':
                data.add_formatter(
                    field[0], lambda a: a and float(a)/MAGNITUDS['kWh'] or 0)
        return data

    def parse_line(self, line):
        raise NotImplementedError("Should have implemented this")



