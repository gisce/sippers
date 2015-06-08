# -*- coding: utf-8 -*-
from __future__ import absolute_import
from datetime import datetime
import os
import re

from sippers import logger
from sippers.exceptions import ParserNotFoundException


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
    logger.error("Parser not found for file %s", sips_file)
    raise ParserNotFoundException()


class Parser(object):
    """Base parser interface.
    """

    encoding = "iso-8859-15"

    @classmethod
    def detect(cls, sips_file):
        if cls.pattern:
            return re.match(cls.pattern, os.path.basename(sips_file))
        return False

    def parse_line(self, line):
        """Parse a line of a SIPS file.

        :param line: line of the file
        """
        raise NotImplementedError("Should have implemented this")



